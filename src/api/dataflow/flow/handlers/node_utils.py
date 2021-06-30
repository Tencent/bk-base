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

from common.exceptions import ApiResultError
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowJob, FlowLinkInfo, FlowNodeInfo, FlowNodeRelation
from dataflow.flow.node_types import NodeTypes
from dataflow.pizza_settings import DEFAULT_HDFS_TAG
from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.udf.functions.function_driver import is_modify_with_udf_released


class NodeUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def update_job_id(flow_id, node_id, job_id, job_type):
        FlowJob.objects.update_or_create(
            defaults={
                "flow_id": flow_id,
                "node_id": node_id,
                "job_id": job_id,
                "job_type": job_type,
                "description": "创建任务",
            },
            node_id=node_id,
        )

    @staticmethod
    def get_project_cluster_groups(project_id, resource_type, service_type=None, private_only=True):
        """
        :return: 返回项目的私有集群列表
        """
        cluster_groups = NodeUtils.get_cluster_groups_by_resource_type(project_id, resource_type, service_type)
        if not cluster_groups:
            raise Errors.FlowError(_("获取当前项目的集群组信息为空(project_id=%s)") % project_id)
        private_cluster_groups = []
        public_cluster_groups = []
        other_cluster_groups = []
        for one_cluster_group in cluster_groups:
            if one_cluster_group["scope"] == "private":
                private_cluster_groups.append(one_cluster_group)
            elif one_cluster_group["scope"] == "public":
                public_cluster_groups.append(one_cluster_group)
            else:
                other_cluster_groups.append(one_cluster_group)
        if not public_cluster_groups:
            raise Errors.FlowError(_("获取当前项目的公有集群组信息为空(project_id=%s)") % project_id)
        if private_only:
            return private_cluster_groups
        else:
            return private_cluster_groups + public_cluster_groups + other_cluster_groups

    @staticmethod
    def get_cluster_groups_by_resource_type(project_id, resource_type, service_type):
        """获取指定项目的特定 resource_type 下特定 service_type 的集群组列表
        :param project_id:
        :param resource_type: processing / storage
        :param service_type: [stream, batch] / [hdfs]
        :return:
        """
        # get geog_area
        geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
        # get all cluster groups
        cluster_groups = AuthHelper.list_project_cluster_group(project_id)
        result_cluster_groups = []
        # 遍历所有有权限的cluster_group
        for one_cluster_group in cluster_groups:
            # 获取 resource_group_id
            resource_group_id_data = ResourceCenterHelper.get_res_group_id_by_cluster_group(
                one_cluster_group["cluster_group_id"], geog_area_code
            )
            resource_group_id = None
            if resource_group_id_data:
                resource_group_id = resource_group_id_data[0]["resource_group_id"]
            if not resource_group_id:
                continue
            # 获取 processing 的 cluster_group
            clusters_data = ResourceCenterHelper.get_cluster_info(geog_area_code, resource_group_id, resource_type)
            if clusters_data:
                for one_cluster in clusters_data:
                    # 获取包含 stream / batch 类型的 cluster_group
                    if one_cluster["service_type"] in service_type:
                        result_cluster_groups.append(one_cluster_group)
                        break
        return result_cluster_groups

    @staticmethod
    def get_storage_cluster_name(project_id, storage_type, result_table_id=None, tag=DEFAULT_HDFS_TAG):
        """
        获取节点对应的存储类型所在的 cluster_name，
        如果该rt存在对应存储类型的存储，则直接返回已存在的类型的存储所在的 cluster_name
        :param project_id:
        :param storage_type:
        :param result_table_id: 如果 rt 非空，则返回 rt 对应的特定类型存储所在的集群的 cluster_name
        :param tag: 默认为 DEFAULT_HDFS_TAG
        :return:
        """
        if result_table_id:
            # 如果指定rt，则获取rt所在的存储集群信息
            default_storage_config = None
            try:
                default_storage_config = StorekitHelper.get_physical_table(result_table_id, storage_type)
            except BaseException:
                # 当 rt 对应的特定类型的存储不存在时，忽略异常
                pass
            if default_storage_config:
                return default_storage_config["cluster_name"]

        _return_cluster_group = None
        private_cluster_groups = NodeUtils.get_project_cluster_groups(
            project_id, "storage", service_type=[storage_type]
        )
        for cluster_group in private_cluster_groups:
            response_data = StorekitHelper.get_storage_cluster_configs(storage_type, cluster_group["cluster_group_id"])
            # 对于 HDFS/TDW 存储，可选存储应该只有一个
            if len(response_data) > 1:
                raise Exception(
                    _("配置错误，项目%(project_id)s, 默认 %(storage_type)s 存储集群数量(%(storage_config_size)s)不唯一")
                    % {
                        "project_id": project_id,
                        "storage_type": storage_type,
                        "storage_config_size": len(response_data),
                    }
                )
            if response_data:
                _return_cluster_group = response_data[0]["cluster_name"]

        if not _return_cluster_group:
            # 获取默认存储
            geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
            default_storage_cluster = StorekitHelper.get_default_storage(storage_type, geog_area_code, tag=tag)
            if default_storage_cluster:
                _return_cluster_group = default_storage_cluster["cluster_name"]
            else:
                raise Errors.FlowError(
                    _("获取当前项目的默认存储集群组信息为空(project_id=%s，geog_area_code=%s, storage_type=%s, tag=%s)")
                    % (project_id, geog_area_code, storage_type, tag)
                )
        return _return_cluster_group

    @staticmethod
    def get_related_nodes_by_rt(result_table_id, node_category=None):
        """
        获取所有输出为 result_table_id 的节点
        @param result_table_id:
        @param node_category:
        @return:
        """
        if not node_category:
            related_source_nodes = FlowNodeRelation.objects.filter(result_table_id=result_table_id)
        else:
            related_source_nodes = FlowNodeRelation.objects.filter(
                node_type__in=node_category, result_table_id=result_table_id
            )
        return related_source_nodes

    @staticmethod
    def get_related_source_nodes(node_handler, related_category=NodeTypes.SOURCE_CATEGORY):
        """
        获取当前 processing 节点所产生结果表被使用的数据源节点列表
        @return:
        """
        if node_handler.node_type not in NodeTypes.PROCESSING_CATEGORY:
            return []
        related_source_nodes = FlowNodeRelation.objects.filter(
            node_type__in=related_category,
            result_table_id__in=node_handler.result_table_ids,
        )
        return related_source_nodes

    @staticmethod
    def list_from_nodes_handler(from_node_ids):
        """
        通过节点 ID 列表查询对象
        """
        return [NODE_FACTORY.get_node_handler(_node_id) for _node_id in from_node_ids]

    @staticmethod
    def get_to_nodes_handler_by_id(node_id):
        return [
            NODE_FACTORY.get_node_handler(_id)
            for _id in FlowLinkInfo.objects.filter(from_node_id=node_id).values_list("to_node_id", flat=True)
        ]

    @staticmethod
    def get_storage_upstream_node_handler(node_id):
        """
        获得存储节点的上游节点，只有一个
        """
        from_node_id = FlowLinkInfo.objects.filter(to_node_id=node_id)[0].from_node_id
        return NODE_FACTORY.get_node_handler(from_node_id)

    @staticmethod
    def get_stream_to_nodes_handler_by_id(flow_id, node_id, node_types):
        """
        返回flow_id, node_id对应的下游node_id
        :param flow_id:
        :param node_id:
        :param node_types: 需过滤的节点类型集合
        :return:
        """
        ret = []
        for _id in FlowLinkInfo.objects.filter(flow_id=flow_id, from_node_id=node_id).values_list(
            "to_node_id", flat=True
        ):
            one_node_handler = NODE_FACTORY.get_node_handler(_id)
            if one_node_handler.node_type in node_types:
                ret.append(_id)
        return ret

    @staticmethod
    def gene_version():
        """
        生成唯一版本号
        """
        return FlowNodeInfo.gene_version()

    @staticmethod
    def validate_config(params, form_class, is_create):
        """
        验证参数
        @param {dict} params  参数
        @param {object} form_class 校验参数的 Form 表单
        """
        form = form_class(params, is_create)
        if not form.is_valid():
            err_msg = _("参数不对")
            if hasattr(form, "format_errmsg"):
                err_msg = form.format_errmsg()
            raise Errors.NodeValidError(err_msg)

        return form.cleaned_data

    @staticmethod
    def get_from_result_table_ids(node_type, node_config, specified_from_node_id=None):
        """
        获取当前节点的源rt列表
        @param node_type: 节点类型
        @param node_config: 节点配置
        @param specified_from_node_id: 获取指定上游节点的输入 RT
        """
        from_result_table_ids = []
        if node_type in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY + NodeTypes.NEW_INPUTS_CATEGORY:
            # 新节点配置
            for from_result_table in NodeUtils.get_input_nodes_config(node_type, node_config):
                if specified_from_node_id:
                    if str(from_result_table["id"]) == str(specified_from_node_id):
                        return from_result_table["from_result_table_ids"]
                else:
                    from_result_table_ids.extend(from_result_table["from_result_table_ids"])
        else:
            if specified_from_node_id:
                return NODE_FACTORY.get_node_handler(specified_from_node_id).result_table_ids
            elif "from_result_table_ids" in node_config:
                # 老节点配置
                from_result_table_ids = node_config["from_result_table_ids"]
        return from_result_table_ids

    @staticmethod
    def get_input_nodes_config(node_type, config):
        """
        [
            {
                "id": 22771,
                "from_result_table_ids": [
                    "591_test_1_clean"
                ]
            },
            {
                "id": 22772,
                "from_result_table_ids": [
                    "591_test_2_clean"
                ]
            }
        ]
        :param node_type:
        :param config:
        :return:
        """
        if "inputs" in config:
            return config["inputs"]
        elif "from_nodes" in config:
            return config["from_nodes"]
        elif "from_result_table_ids" in config:
            return [{"id": 0, "from_result_table_ids": config["from_result_table_ids"]}]
        else:
            raise Errors.NodeValidError(_("节点的数据result table列表参数不正确"))

    @staticmethod
    def set_input_nodes_config(node_type, config, from_nodes_info):
        """
        [
            {
                "id": 22771,
                "from_result_table_ids": [
                    "591_test_1_clean"
                ]
            },
            {
                "id": 22772,
                "from_result_table_ids": [
                    "591_test_2_clean"
                ]
            }
        ]
        :param from_nodes_info:
        :param node_type:
        :param config:
        :return:
        """
        if node_type in NodeTypes.NEW_INPUTS_CATEGORY:
            config["inputs"] = from_nodes_info
            return config
        else:
            config["from_nodes"] = from_nodes_info
            return config

    @staticmethod
    def get_upstream_nodes_info(node_type, from_node_ids, form_data):
        """
        保存、更新节点时，获取当前节点的上游节点信息，用于创建元数据信息
        """
        upstream_nodes_info = []
        # 参数改造节点：未来 flow 表单的统一，多输入、多输出的 RT
        if node_type in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY + NodeTypes.NEW_INPUTS_CATEGORY:
            for from_result_table in NodeUtils.get_input_nodes_config(node_type, form_data):
                upstream_node = NODE_FACTORY.get_node_handler(from_result_table["id"])
                if upstream_node.node_type in NodeTypes.ALL_KV_SOURCE_CATEGORY:
                    continue
                upstream_nodes_info.append(
                    {
                        "upstream_node": upstream_node,
                        "result_table_id": from_result_table["from_result_table_ids"],
                    }
                )
        else:
            for _n in NodeUtils.list_from_nodes_handler(from_node_ids):
                # 上游是 kv 类型的节点，不需要对元数据进行更新操作
                if _n.node_type in NodeTypes.ALL_KV_SOURCE_CATEGORY:
                    continue
                upstream_nodes_info.append(
                    {
                        "upstream_node": _n,
                        "result_table_id": list(set(_n.result_table_ids) & set(form_data["from_result_table_ids"])),
                    }
                )
        return upstream_nodes_info

    @staticmethod
    def transform_time_unit_to_day(window_size, window_size_unit, window_offset, window_offset_unit):
        # 全部转换成hour再计算差值
        window_size_in_hour = 0
        if window_size_unit == "hour":
            window_size_in_hour = int(window_size)
        elif window_size_unit == "day":
            window_size_in_hour = int(window_size) * 24
        elif window_size_unit == "week":
            window_size_in_hour = int(window_size) * 24 * 7
        elif window_size_unit == "month":
            window_size_in_hour = int(window_size) * 24 * 31
        window_offset_in_hour = 0
        if window_offset_unit == "hour":
            window_offset_in_hour = int(window_offset)
        elif window_offset_unit == "day":
            window_offset_in_hour = int(window_offset) * 24
        elif window_offset_unit == "week":
            window_offset_in_hour = int(window_offset) * 24 * 7
        elif window_offset_unit == "month":
            window_offset_in_hour = int(window_offset) * 24 * 31
        max_delay_in_hour = window_offset_in_hour + window_size_in_hour
        return int(max_delay_in_hour / 24) + 1

    @staticmethod
    def get_max_window_size_by_day(form_data):
        """
        获取当前节点的计算的数据窗口长度，用于设置当前及下游节点的 HDFS 过期时间，单位为天
        1. 若为固定窗口
            1. 统一配置且窗口长度
                a.单位为小时，除以24 + 1(避免不整除)
                b.单位为周，取7
                c.单位为月，取31
                d.单位为日，取窗口长度
            2. 若为自定义配置，单位原则与1一样，计算结果取大者
        2. 若为累加窗口，返回1
        @param form_data:
        @return:
        """
        # batch_window_config = {
        #     'window_type': '',
        #     'dependency_config_type': '',
        #     'unified_config': {
        #         'window_size_period': '',
        #         'window_size': ''
        #     },
        #     'custom_config': {
        #         'result_table_id': {
        #             'window_size_period': '',
        #             'window_size': ''
        #         }
        #     }
        # }
        # TODO 以后各个离线类节点参数对齐
        if "window_type" not in form_data and "window_config" in form_data:
            # 离线指标节点
            form_data = form_data["window_config"]
        elif "schedule_config" in form_data and "serving_scheduler_params" in form_data["schedule_config"]:
            # 时序自定义节点
            return int(form_data["schedule_config"]["serving_scheduler_params"]["data_period"])
        elif "serving_scheduler_params" in form_data:
            # modelflow 节点，给个默认值 1。外部调用 max(7, 1) 返回7天
            return 1
        if "node_type" in form_data and form_data["node_type"] == "batchv2":
            # batchv2 节点
            max_window_size = 0
            for one_window_info in form_data["window_info"]:
                window_type = one_window_info["window_type"]
                if window_type == "scroll" or window_type == "whole":
                    window_size = form_data["dedicated_config"]["schedule_config"]["count_freq"]
                    window_size_unit = form_data["dedicated_config"]["schedule_config"]["schedule_period"]
                else:
                    window_size = one_window_info["window_size"]
                    window_size_unit = one_window_info["window_size_unit"]
                if window_type == "whole":
                    window_offset = "0"
                    window_offset_unit = "hour"
                else:
                    window_offset = one_window_info["window_offset"]
                    window_offset_unit = one_window_info["window_offset_unit"]
                window_size_in_day = NodeUtils.transform_time_unit_to_day(
                    window_size, window_size_unit, window_offset, window_offset_unit
                )
                if window_size_in_day > max_window_size:
                    max_window_size = window_size_in_day
            return max_window_size
        else:
            # 老batch节点
            if form_data["window_type"] == "accumulate_by_hour":
                return 1
            else:
                if form_data["dependency_config_type"] == "unified":
                    if form_data["unified_config"]["window_size_period"] == "hour":
                        return int(form_data["unified_config"]["window_size"] / 24) + 1
                    elif form_data["unified_config"]["window_size_period"] == "week":
                        return 7
                    elif form_data["unified_config"]["window_size_period"] == "month":
                        return 31
                    else:
                        return int(form_data["unified_config"]["window_size"])
                else:
                    max_window_size = 0
                    for result_table_id, config in list(form_data["custom_config"].items()):
                        if config["window_size_period"] == "hour":
                            current_window_size = config["window_size"] / 24 + 1
                        elif config["window_size_period"] == "week":
                            current_window_size = 7
                        elif config["window_size_period"] == "month":
                            current_window_size = 31
                        else:
                            current_window_size = config["window_size"]
                        if max_window_size < int(current_window_size):
                            max_window_size = int(current_window_size)
                    return max_window_size

    @staticmethod
    def get_max_related_window_size(result_table_id):
        """
        获取所有输出为 result_table_id 的节点的下游离线节点的最大时间窗口长度
        @param result_table_id:
        @return:
        """
        max_window_size_by_day = 0
        # 根据指定类型(离线节点上游合法节点类型)获取 result_table_id 的相关节点
        related_nodes = NodeUtils.get_related_nodes_by_rt(result_table_id)
        for related_node in related_nodes:
            to_nodes = NodeUtils.get_to_nodes_handler_by_id(related_node.node_id)
            for to_node in to_nodes:
                if to_node.node_type in [
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                    NodeTypes.MODEL_APP,
                    NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                ]:
                    to_node_max_window_size_by_day = NodeUtils.get_max_window_size_by_day(to_node.get_config(False))
                    if to_node_max_window_size_by_day > max_window_size_by_day:
                        max_window_size_by_day = to_node_max_window_size_by_day
        return max_window_size_by_day

    @staticmethod
    def is_modify_dp_node_with_udf(processing_id):
        """
        判断节点是否为实时节点、离线节点（可以使用udf的节点），并且udf最新版本和节点使用的udf版本不一致，则需要修改

        :return: True (需要修改)、False（不需要修改）
        """
        return is_modify_with_udf_released(processing_id)

    @staticmethod
    def build_from_nodes_list(from_node_ids):
        """
        组装 SQL 中 from 表名等标识对应的结果表(静态或非静态)
        @param from_node_ids:
        @return:
        """
        from_nodes = [NODE_FACTORY.get_node_handler(_id) for _id in from_node_ids]

        not_static_rt_ids = []
        static_rt_ids = []
        source_rt_ids = []
        for _node in from_nodes:
            _table_info = {
                "category": _node.node_type,
                "result_table_ids": _node.result_table_ids,
            }
            parent_node_type = _table_info["category"]

            if parent_node_type in NodeTypes.STREAM_KV_SOURCE_CATEGORY:
                static_rt_ids.extend(_table_info["result_table_ids"])
            else:
                not_static_rt_ids.extend(_table_info["result_table_ids"])

            if parent_node_type in NodeTypes.SOURCE_CATEGORY:
                source_rt_ids.extend(_table_info["result_table_ids"])

        return not_static_rt_ids, static_rt_ids, source_rt_ids

    @staticmethod
    def filter_batch(node_handlers):
        """
        从节点列表中，过滤离线节点，如果是存储数据节点，需要找到关联的计算节点
        @param node_handlers 节点列表
        """
        # 取出离线型节点
        _from_node_handlers = list(
            filter(
                lambda _n: _n.node_type in NodeTypes.BATCH_CATEGORY + NodeTypes.BATCH_SOURCE_CATEGORY,
                node_handlers,
            )
        )
        batch_nodes = []
        for one_from_node_handler in _from_node_handlers:
            if one_from_node_handler.node_type in NodeTypes.BATCH_CATEGORY:
                batch_nodes.append(one_from_node_handler)
            elif one_from_node_handler.node_type in NodeTypes.BATCH_SOURCE_CATEGORY:
                _origin_node = one_from_node_handler.get_rt_generated_node()
                # 当实时节点接HDFS存储，该节点产生的RT可以作为离线数据源节点的结果表
                if _origin_node is not None and _origin_node.node_type in NodeTypes.BATCH_CATEGORY:
                    batch_nodes.append(_origin_node)
        return batch_nodes

    @staticmethod
    def get_origin_batch_node(node_handler):
        """
        通过节点的上下关系，找到离线节点的起始节点，此处定义为根"离线属性"节点，
        这里需要考虑以下两种场景
        1. 场景一（F 的根离线节点为 C）
        A(实时) -> B(HDFS) -> C(离线) -> D(离线) -> E(离线)
                                      |
                                      v
                                      F(离线)
        2. 场景二（H 的根离线节点为 C）
        A(实时) -> B(HDFS) -> C(离线 rt_c)
        D(存储数据 rt_c) -> E(离线) -> F(离线) -> G(离线)
                                      |
                                      v
                                      H(离线)
        """
        candidate_node = node_handler
        while True:
            batch__from_nodes = NodeUtils.filter_batch(candidate_node.get_from_nodes_handler())
            if not batch__from_nodes:
                break
            # 如果不是根离线节点，则任意取一父节点作为候选节点，继续遍历校验
            candidate_node = batch__from_nodes[0]
        return candidate_node

    @staticmethod
    def get_storage_cluster_groups(storage_type, cluster_name):
        """
        根据集群名称获取存储集群所在的集群组
        """
        res_data = StorekitHelper.get_storage_cluster_config(storage_type, cluster_name)
        if not res_data:
            raise ApiResultError(
                _("存储集群信息未包含集群%(cluster_name)s，存储类型%(storage_type)s信息.")
                % {"cluster_name": cluster_name, "storage_type": storage_type}
            )
        cluster_groups = [res_data["cluster_group"]]
        return cluster_groups

    @staticmethod
    def check_expires(node_type, from_nodes, form_data):
        """
        校验当前设置的过期时间是否合法
            HDFS:
                1. 对于上游是离线计算的情况，需要校验当前过期时间是否大于其数据窗口长度
                2. 对于上游 RT 被关联为数据源的情况，需要校验其是否被其它下游离线用到，并计算最大数据窗口长度
        """
        if "expires" not in form_data or form_data["expires"] == -1:
            return True
        if node_type in [NodeTypes.HDFS_STORAGE]:
            for from_node in from_nodes:
                if from_node.node_type in [
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                    NodeTypes.MODEL_APP,
                    NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                ]:
                    max_window_size_by_day = NodeUtils.get_max_window_size_by_day(from_node.get_config(False))
                    if form_data["expires"] < max_window_size_by_day:
                        raise Errors.NodeError(_("存储过期时间应大于或等于上游离线节点数据窗口长度"))
            max_related_window_size = NodeUtils.get_max_related_window_size(form_data["result_table_id"])
            if form_data["expires"] < max_related_window_size:
                raise Errors.NodeError(_("当前结果表被离线计算使用，过期时间应大于或等于其数据窗口长度(%s天)") % max_related_window_size)

    @staticmethod
    def get_rt_related_storage_node(flow_id, result_table_ids):
        """
        获取当前flow中某个rt相关的存储节点id列表（节点的from_result_table_ids中包含该rt）
        """
        ret = []
        nodes = FlowNodeInfo.objects.filter(flow_id=flow_id)
        for node in nodes:
            node_id = node.node_id
            node_type = node.node_type
            if node_type not in NodeTypes.STORAGE_CATEGORY:
                continue
            node_config = node.load_config(True, loading_concurrently=True)
            if "from_result_table_ids" in node_config:
                from_result_table_ids = [x.encode("utf-8") for x in node_config["from_result_table_ids"]]
                if set(result_table_ids) <= set(from_result_table_ids):
                    ret.append(node_id)

        return ret
