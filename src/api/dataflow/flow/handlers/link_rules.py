# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import json
import threading
import time

from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import LinkRuleError, NodeValidError
from dataflow.flow.models import (
    FlowNodeInstance,
    FlowNodeInstanceLink,
    FlowNodeInstanceLinkBlacklist,
    FlowNodeInstanceLinkGroupV2,
)
from dataflow.flow.node_types import NodeTypes

"""
改进后的连线规则
kafka 是虚拟节点 : 首尾含有 虚拟节点 的规则不会返回给前端, 例如 kafka->hdfs_storage
具有传递类型的节点 : stream->kafka->hdfs_storage->batch, 可以通过此节点进行跳跃传递
最大传递次数 :
    stream -> kafka -> stream ：1跳
    stream -> kafka -> hdfs_storage -> batch ：2跳
    目前两跳可以覆盖所有的连线规则
"""


class NodeInstanceLink(object):
    # kafka,memory,tdw 是虚拟节点
    VIRTUAL_NODES = ["kafka", "hdfs", "memory", "tdw"]
    # 具有传递类型的节点, 可以通过这些节点跳向别的节点
    # hdfs 是 channel，hdfs_storage 是画布上的实体存储
    TRANSMIT_NODES = ["kafka", "hdfs", "memory", "tdw"]
    # 最大传递次数, 不超过 3 跳
    MAX_TRANSMIT = 3
    """ 类中全局保存的变量, 辅助计算跳数路径
    global_detail_path_rule_list: ['stream -> kafka -> stream', 'stream -> kafka -> hdfs_storage -> batch', ...]
    global_link_rule_dict:  { // 相同的key下游有哪些节点, 方便跳数计算
        'stream_source': ['stream', 'batch', ...],
        'kafka': ['stream', 'split' , ...]
    }
    global_link_limit_dict: { // 上下游连接限制
        'upstream->downstream': {
            'downstream_link_limit': '1,1',
            'upstream_link_limit': '1,1'
        }
    }
    """
    global_detail_path_rule_list = []
    global_link_rule_dict = {}
    global_link_limit_dict = {}
    """
    缓存连线规则
    当用户连续请求时，若时间间隔不超过 VISIT_TIME_INTERVAL, 无需从数据库中读取计算
    直接从缓存中取规则
    """
    last_visit_time = 0
    # 单位：秒
    VISIT_TIME_INTERVAL = 1 * 60
    response_link_rule_cache = {}
    _value_lock = threading.Lock()

    @staticmethod
    def filter_detail_path(detail_path_list):
        """
        过滤不符合要求的连线
        1. 忽略虚拟节点, 返回给前端的节点不含虚拟节点
        2. 优化掉不合逻辑的连线，3 类
        process_model->kafka->hdfs->hdfs_storage：优化为 process_model->kafka->hdfs_storage
        process_model->kafka->hdfs->clickhouse_storage ：删除掉
        model_ts_custom->kafka->hdfs->model_ts_custom ：删除掉，model_ts_custom 类为混合计算节点
        """
        result_filter_path = []
        for detail_path in detail_path_list:
            paths = detail_path.split("->")
            length = len(paths)
            # 头尾有虚拟节点，删除
            if paths[0] in NodeInstanceLink.VIRTUAL_NODES or paths[length - 1] in NodeInstanceLink.VIRTUAL_NODES:
                detail_path = None
            if length > 3:
                if paths[1] == NodeTypes.KAFKA and paths[2] == NodeTypes.HDFS:
                    if paths[3] in NodeTypes.STORAGE_CATEGORY:
                        if paths[3] == NodeTypes.HDFS_STORAGE:
                            # process_model->kafka->hdfs->hdfs_storage
                            # process_model->kafka->hdfs_storage
                            detail_path = "{}->{}->{}".format(paths[0], paths[1], paths[3])
                        else:
                            # process_model->kafka->hdfs->clickhouse_storage
                            detail_path = None
                    elif paths[3] in NodeTypes.MIX_PROCESSING_CATEGORY:
                        # model_ts_custom->kafka->hdfs->model_ts_custom
                        detail_path = None

            if detail_path:
                result_filter_path.append(detail_path)

        return result_filter_path

    # 只返回头尾, 'stream -> kafka -> stream' 返回 'stream -> stream', 返回结果给黑名单过滤
    @staticmethod
    def get_head_tail(total_path):
        if total_path:
            if "->" in total_path:
                paths = total_path.split("->")
                length = len(paths)
                return "{}->{}".format(paths[0], paths[length - 1])
        return ""

    # 根据原子跳数递归计算具体路径, 并保存到 global_detail_path_rule_list
    @classmethod
    def build_jump_path(cls, from_node, to_node, cur_path, depth):
        if depth == cls.MAX_TRANSMIT:
            return
        to_node_list = cls.global_link_rule_dict[to_node]
        for to_node_jump in to_node_list:
            temp_path = "{}->{}".format(cur_path, to_node_jump)
            if temp_path not in cls.global_detail_path_rule_list:
                cls.global_detail_path_rule_list.append(temp_path)
            if to_node_jump in cls.TRANSMIT_NODES:
                cls.build_jump_path(from_node, to_node_jump, temp_path, depth + 1)

    # 获取 detail_paths 的最大，最小连接限制
    # 比如: stream -> kafka -> stream 最大，最小连接由 stream -> kafka 和 kafka -> stream 计算得出
    @classmethod
    def get_link_limit(cls, total_path):
        paths = total_path.split("->")
        # 长度至少为 2
        length = len(paths)
        key0 = "{}->{}".format(paths[0], paths[1])
        # 转化为 int 数组返回, 前端需要 int
        downstream_link_limit0 = [int(i) for i in cls.global_link_limit_dict[key0]["downstream_link_limit"].split(",")]
        upstream_link_limit0 = [int(i) for i in cls.global_link_limit_dict[key0]["upstream_link_limit"].split(",")]
        if length == 2:
            return downstream_link_limit0, upstream_link_limit0
        # 长度超过 2 或者更长, 取连接的最大值, 是否有优化的点?
        """
        stream->kafka->hdfs->batch = stream->batch
        上下游限制       downstream_link_limit   upstream_link_limit
        stream->kafka   1,10                    1,1
        kafka->hdfs     1,1                     1,1
        hdfs->batch     1,10                    1,10
        计算得出：
        stream->batch   1,10                    1,10
        """
        downstream_limit0 = -1
        downstream_limit1 = -1
        upstream_limit0 = -1
        upstream_limit1 = -1
        i = 0
        while i < length - 1:
            # 由于 filter_detail_path 对 kafka->hdfs 做了替换，可能会找不到 kafka->hdfs_storage 这条路径
            if paths[i] == NodeTypes.KAFKA and paths[i + 1] == NodeTypes.HDFS_STORAGE:
                paths[i + 1] = NodeTypes.HDFS
            key_temp = "{}->{}".format(paths[i], paths[i + 1])
            downstream_link_limit = cls.global_link_limit_dict[key_temp]["downstream_link_limit"]
            upstream_link_limit = cls.global_link_limit_dict[key_temp]["upstream_link_limit"]
            d_limit0 = int(downstream_link_limit.split(",")[0])
            d_limit1 = int(downstream_link_limit.split(",")[1])
            u_limit0 = int(upstream_link_limit.split(",")[0])
            u_limit1 = int(upstream_link_limit.split(",")[1])
            if d_limit0 > downstream_limit0:
                downstream_limit0 = d_limit0
            if d_limit1 > downstream_limit1:
                downstream_limit1 = d_limit1
            if u_limit0 > upstream_limit0:
                upstream_limit0 = u_limit0
            if u_limit1 > upstream_limit1:
                upstream_limit1 = u_limit1
            i = i + 1
        downstream_limit = [downstream_limit0, downstream_limit1]
        upstream_limit = [upstream_limit0, upstream_limit1]
        return downstream_limit, upstream_limit

    # 兼容原连线规则的返回格式
    @classmethod
    def _old_rule_adapter(cls, _all_link_rule_dict):
        """
        原json：
        {
            'model_app->ignite': {
                'downstream_link_limit': None,
                'upstream_link_limit': None,
                'detail_paths': []
            }
        }
        转化为：
        {
            'model_app': {
                'ignite':[
                    'default': {
                        'upstream_link_limit': [1, 1],
                        'downstream_link_limit': [1, 1],
                        'detail_path': []
                    }
                ]
            }
        }
        """
        adaptive_rule = {}
        for key in list(_all_link_rule_dict.keys()):
            up = key.split("->")[0]
            down = key.split("->")[1]
            detail_paths = _all_link_rule_dict[key]["detail_paths"]
            """
            路径问题，按每条路径计算 downstream_link_limit, upstream_link_limit 后返回
            经过过滤后的连线，只有两种情况：
            1. 路径只有1条：default，默认连线
            2. 路径2条：混合计算节点，有 hdfs 的是离线路径 batch，否则是实时路径 stream
                    或者特殊的 ['stream->kafka->stream', 'stream->memory->stream']
            """
            if up not in adaptive_rule:
                adaptive_rule[up] = {}
            adaptive_rule[up][down] = {}
            if len(detail_paths) == 1:
                # 只有一条路径
                downstream_link_limit, upstream_link_limit = cls.get_link_limit(detail_paths[0])
                adaptive_rule[up][down]["default"] = {
                    "downstream_link_limit": downstream_link_limit,
                    "upstream_link_limit": upstream_link_limit,
                    "detail_path": detail_paths[0],
                }
            else:
                memory_path = "stream->memory->stream"
                if memory_path in detail_paths:
                    downstream_link_limit, upstream_link_limit = cls.get_link_limit(memory_path)
                    adaptive_rule[up][down]["default"] = {
                        "downstream_link_limit": downstream_link_limit,
                        "upstream_link_limit": upstream_link_limit,
                        "detail_path": memory_path,
                    }
                else:
                    # 混合计算节点
                    for path in detail_paths:
                        downstream_link_limit, upstream_link_limit = cls.get_link_limit(path)
                        filter_path = "->%s->" % NodeTypes.HDFS
                        if filter_path in path:
                            adaptive_rule[up][down]["batch_path"] = {
                                "downstream_link_limit": downstream_link_limit,
                                "upstream_link_limit": upstream_link_limit,
                                "detail_path": path,
                            }
                        else:
                            adaptive_rule[up][down]["stream_path"] = {
                                "downstream_link_limit": downstream_link_limit,
                                "upstream_link_limit": upstream_link_limit,
                                "detail_path": path,
                            }

        return adaptive_rule

    # TODO 新连线规则, 获取单个连线规则
    @classmethod
    def _single_rules(cls):
        """
        新连线规则, dataflow_node_instance_link 只保存原子的链接方式, 其余链接通过跳数计算得出
        比如： stream->kafka,kafka->stream,计算得出 stream->stream
        """
        # 计算连线用到的全局变量置空
        cls.global_detail_path_rule_list = []
        cls.global_link_rule_dict.clear()
        cls.global_link_limit_dict.clear()
        instance_link_obj_list = FlowNodeInstanceLink.objects.filter(available=1)
        """ 转化为两个 dict
        {
            'stream_source': ['stream', 'batch', ...],
            'kafka': ['stream', 'split' , ...]
        }
        {
            'upstream->downstream': {
                'downstream_link_limit': '1,1',
                'upstream_link_limit': '1,1'
            }
        }
        """
        for item in instance_link_obj_list:
            upstream = item.upstream
            downstream = item.downstream
            downstream_link_limit = item.downstream_link_limit
            upstream_link_limit = item.upstream_link_limit
            if upstream not in cls.global_link_rule_dict:
                cls.global_link_rule_dict[upstream] = []
            cls.global_link_rule_dict[upstream].append(downstream)
            # key 是唯一的, 数据库中联合唯一键
            key = "{}->{}".format(upstream, downstream)
            cls.global_link_limit_dict[key] = {
                "downstream_link_limit": downstream_link_limit,
                "upstream_link_limit": upstream_link_limit,
            }
        # 1. 根据跳数规则和原子连接, 计算出所有的可达的连线规则, 保存在 global_detail_path_rule_list 中
        for from_node in cls.global_link_rule_dict:
            if from_node in cls.VIRTUAL_NODES:
                continue
            to_node_list = cls.global_link_rule_dict[from_node]
            for to_node in to_node_list:
                one_link = "{}->{}".format(from_node, to_node)
                if one_link not in cls.global_detail_path_rule_list:
                    cls.global_detail_path_rule_list.append(one_link)
                if to_node in cls.TRANSMIT_NODES:
                    temp_path = "{}->{}".format(from_node, to_node)
                    cls.build_jump_path(from_node, to_node, temp_path, 1)
        # 过滤和优化一些脏数据
        cls.global_detail_path_rule_list = NodeInstanceLink.filter_detail_path(cls.global_detail_path_rule_list)
        # 2. 准备返回给前端的参数, 用黑名单过滤
        """
        {
            'upstream->downstream': {
                'downstream_link_limit': None,
                'upstream_link_limit': None,
                'detail_paths': []
            }
        }
        """
        # 黑名单
        link_black_list = []
        instance_link_black_obj_list = FlowNodeInstanceLinkBlacklist.objects.all()
        for item in instance_link_black_obj_list:
            upstream = item.upstream
            downstream = item.downstream
            black_item = "{}->{}".format(upstream, downstream)
            link_black_list.append(black_item)
        # 总连线规则
        all_link_rule_dict = {}
        for item in cls.global_detail_path_rule_list:
            if item:
                key = cls.get_head_tail(item)
                if key:
                    # 黑名单过滤
                    if key in link_black_list:
                        continue
                    if key not in all_link_rule_dict:
                        all_link_rule_dict[key] = {
                            # 后续在 _old_rule_adapter 更新 downstream_link_limit,upstream_link_limit
                            # detail_paths 可能有多条路径, 要确定其中一条路径来计算
                            "downstream_link_limit": None,
                            "upstream_link_limit": None,
                            "detail_paths": [],
                        }
                    all_link_rule_dict[key]["detail_paths"].append(item)

        return all_link_rule_dict

    # TODO 新连线规则, 获取组合规则
    @staticmethod
    def _group_rules():
        """
        组合连接：
        a --
            \
              --> c
            /
        b --
        group_rule_dict:
        {
            'group_id': {
                'downstream_instance':'c',
                'upstream_max_link_limit':{ 'a': 1, 'b': 1 }
            }
        }
        group_rule_list: [  // 去掉 group_id，因为前端并不关心这个参数
            {
                'downstream_instance':'c',
                'upstream_max_link_limit':{ 'a': 1, 'b': 1 }
            }
        ]
        """
        group_rule_list = []
        link_group_object = FlowNodeInstanceLinkGroupV2.objects.all()
        for item in link_group_object:
            node_instance_name = item.node_instance_name
            upstream_max_link_limit = item.upstream_max_link_limit
            group_rule_list.append(
                {
                    "downstream_instance": node_instance_name,
                    "upstream_max_link_limit": json.loads(upstream_max_link_limit),
                }
            )
        return group_rule_list

    # TODO 新连线规则
    @classmethod
    def get_link_rules_config(cls):
        # 当前时间超过上一次访问时间 VISIT_TIME_INTERVAL, 清空缓存
        cur_time = int(time.time())
        if cur_time - cls.last_visit_time > cls.VISIT_TIME_INTERVAL:
            # 更新时间
            with cls._value_lock:
                if cur_time - cls.last_visit_time > cls.VISIT_TIME_INTERVAL:
                    cls.response_link_rule_cache.clear()
                    node_rules = cls._old_rule_adapter(cls._single_rules())
                    group_rules = cls._group_rules()
                    cls.response_link_rule_cache = {
                        "node_rules": node_rules,
                        "group_rules": group_rules,
                    }
                    cls.last_visit_time = cur_time
                    return cls.response_link_rule_cache

        if cls.response_link_rule_cache:
            # 缓存不为空，直接返回
            return cls.response_link_rule_cache

    @classmethod
    def get_detail_path_by_node_type(cls, parent_node, to_node):
        """
        根据上下游节点类型以及[实时/离线]路径获取连线路径
        """
        from_node_type = FlowNodeInstance.node_type_transfer(parent_node.node_type)
        to_node_type = FlowNodeInstance.node_type_transfer(to_node.node_type)
        # 获取连线路径
        link_path_dict = cls.get_link_rules_config()["node_rules"][from_node_type][to_node_type]
        if "default" in link_path_dict:
            # 只有1条默认路径
            detail_path = link_path_dict["default"]["detail_path"]
        else:
            """
            只有混合计算节点 -> 存储节点 的时候，有多条路径
            例如：
            process_model->hdfs->elastic_storage
            process_model->kafka->elastic_storage
            根据混合节点的 serving_mode 来判断取哪条路径，字典结构不一样
            对于 model节点，取 node_config['serving_scheduler_params']['serving_mode']
            对于 process_model/model_ts_custom 节点，取 node_config['serving_mode']
            """
            node_config = parent_node.get_config(False)
            if from_node_type == NodeTypes.MODEL:
                serving_mode = node_config["serving_scheduler_params"]["serving_mode"]
            elif from_node_type == NodeTypes.MODEL_TS_CUSTOM or from_node_type == NodeTypes.PROCESS_MODEL:
                serving_mode = node_config["serving_mode"]
            else:
                raise NodeValidError(_("非混合计算节点不允许有多条链路，node_type=%s" % from_node_type))
            # serving_mode=offline/realtime
            if serving_mode in [
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
            ]:
                detail_path = link_path_dict["batch_path"]["detail_path"]
            elif serving_mode == NodeTypes.STREAM:
                detail_path = link_path_dict["stream_path"]["detail_path"]
            else:
                raise NodeValidError(_("混合节点节点serving_mode错误!serving_mode=%s" % serving_mode))
        return detail_path

    @staticmethod
    def get_link_path_channel(parent_node, to_node):
        """
        通用方法，获取两个节点之间的 channel 存储
        channel 有且仅有一个!
        [实时计算]->kafka->hdfs->[离线计算]。由于中间补上显式的 hdfs 存储，所以 channel 只有一个 kafka
        """
        # 获取连线路径
        detail_path = NodeInstanceLink.get_detail_path_by_node_type(parent_node, to_node)
        detail_path_list = detail_path.split("->")
        # 去掉头尾，计算节点到计算节点/计算到存储节点，中间必有存储或内存
        channel_list = detail_path_list[1 : len(detail_path_list) - 1]
        return channel_list[0]

    @classmethod
    def validate_link_limit(cls, limit_dict):
        """
        后端校验时获取最大连接限制 limit_dict 如下
        {
            "default": {
                "detail_path": "spark_structured_streaming->kafka->tsdb_storage",
                "upstream_link_limit": [1, 10]
                "downstream_link_limit": [1, 10]
            },
            "batch_path": {
                "detail_path": "spark_structured_streaming->kafka->tsdb_storage",
                "upstream_link_limit": [1, 10]
                "downstream_link_limit": [1, 10]
            }
        }
        TODO 还不知道走哪条路径，目前取最大的一条进行校验
        """
        max_up_limit_num = -1
        max_down_limit_num = -1
        for key in limit_dict:
            upstream_link_limit_value = limit_dict[key]["upstream_link_limit"][1]
            downstream_link_limit_value = limit_dict[key]["downstream_link_limit"][1]
            if upstream_link_limit_value > max_up_limit_num:
                max_up_limit_num = upstream_link_limit_value
            if downstream_link_limit_value > max_down_limit_num:
                max_down_limit_num = downstream_link_limit_value
        return max_up_limit_num, max_down_limit_num

    # 基于新连线规则的校验
    @classmethod
    def validate_node_link(cls, from_node, to_node, from_node_result_table_ids, from_nodes, is_create):
        """
        校验位置：NodeHandler，节点更新或创建之前(build_before)，在(check_link_rules)中的(validate_link_rule)函数进行校验
        :param from_node: 连线上游节点
        :param to_node: 连线下游节点
        :param from_node_result_table_ids: from_node 连接至 to_node 的结果表
        :param from_nodes: 上游节点的所有 NodeHandler 的集合
        :param is_create: 当前是否是新增节点操作
        :return:
        """
        from_node_result_table_ids_num = len(from_node_result_table_ids)
        if from_node_result_table_ids_num == 0:
            if not from_node.result_table_ids:
                raise LinkRuleError(_("获取上游节点(id=%s)结果表失败") % from_node.node_id)
            raise LinkRuleError(_("源结果表列表中未包含上游节点结果表%s") % "、".join(from_node.result_table_ids))
        # 获取连线规则，校验节点类型时要做转化操作，realtime 转化为 stream 等
        link_rules_config_dict = cls.get_link_rules_config()
        # 1. 校验 from_node 的下游是否可连其它节点
        from_node_type = FlowNodeInstance.node_type_transfer(from_node.node_type)
        from_node_rules = link_rules_config_dict["node_rules"].get(from_node_type)
        if not from_node_rules:
            raise LinkRuleError(_("节点%s的下游无任何可连接节点") % from_node.get_node_type_display())
        # 2. 校验 to_node 的合法性
        to_node_type = FlowNodeInstance.node_type_transfer(to_node.node_type)
        if to_node_type not in list(from_node_rules.keys()):
            raise LinkRuleError(
                _("暂不支持%(from_node)s连接至%(to_node)s")
                % {
                    "from_node": from_node.get_node_type_display(),
                    "to_node": to_node.get_node_type_display(),
                }
            )
        # 3. 统计上游的组合情况
        validate_group_dict = {}
        for item in from_nodes:
            item_node_type = FlowNodeInstance.node_type_transfer(item.node_type)
            if item_node_type not in validate_group_dict:
                validate_group_dict[item_node_type] = 0
            validate_group_dict[item_node_type] += 1
        # 4. 校验组合规则
        if list(validate_group_dict.keys()) == [NodeTypes.KV_SOURCE]:
            raise LinkRuleError(_("TRedis实时关联数据源不能单独保存"))
        # unified_kv_source 的下游如果是 stream，则不能单独保存; 下游如果是 batch，则允许保存
        # 'realtime' 转 'stream'
        stream_type = FlowNodeInstance.node_type_transfer(NodeTypes.STREAM)
        if list(validate_group_dict.keys()) == [NodeTypes.UNIFIED_KV_SOURCE] and to_node_type == stream_type:
            raise LinkRuleError(_("关联数据源的下游为实时计算节点时不能单独保存"))
        for item in link_rules_config_dict["group_rules"]:
            if to_node_type == item["downstream_instance"]:
                if list(validate_group_dict.keys()) == list(item["upstream_max_link_limit"].keys()):
                    # 命中了组合规则，校验组合数是否正确
                    for key in item["upstream_max_link_limit"]:
                        if validate_group_dict[key] > item["upstream_max_link_limit"][key]:
                            raise LinkRuleError(_("不满足组合规则%s" % json.dumps(item["upstream_max_link_limit"])))
                    # 组合规则校验通过后，不需要再校验单个规则
                    return
        # 5. 校验单个规则
        """
        merge ->
                 -> stream
        stream ->
        对于不满足组合规则的情况，例如上，stream->stream 中上游限制2，其中要包括 merge->stream 这条线
        """
        max_up_limit_num, max_down_limit = cls.validate_link_limit(from_node_rules[to_node_type])
        if len(list(validate_group_dict.keys())) > max_up_limit_num:
            raise LinkRuleError(
                _("下游%(to_node)s仅支持连接至多%(num)s个上游%(from_node)s节点")
                % {
                    "to_node": to_node_type,
                    "num": max_up_limit_num,
                    "from_node": from_node.get_node_type_display(),
                }
            )
        # 校验 from_node 到 to_node 的下游限制
        downstream_match_instances = list(
            filter(
                lambda _n: _n.node_type == to_node.node_type,
                from_node.get_to_nodes_handler(),
            )
        )
        # 创建的时候，校验 +1 ；更新的时候不需要
        if len(downstream_match_instances) + bool(is_create) > max_down_limit:
            raise LinkRuleError(
                _("上游%(from_node)s仅支持连接至多%(num)s个下游%(to_node)s节点")
                % {
                    "from_node": from_node.get_node_type_display(),
                    "num": max_down_limit,
                    "to_node": to_node_type,
                }
            )

    @staticmethod
    def validate_support_node_types():
        db_node_types = list(FlowNodeInstance.node_type_instance_config().keys())
        for real_node_type, db_node_type in list(FlowNodeInstance.NODE_TYPE_MAP.items()):
            if db_node_type in db_node_types:
                db_node_types.remove(db_node_type)
                db_node_types.append(real_node_type)
        return db_node_types
