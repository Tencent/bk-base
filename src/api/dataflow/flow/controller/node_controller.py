# -*- coding: utf-8 -*
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

import time

from common.base_utils import model_to_dict
from common.local import get_request_username
from common.transaction import auto_meta_sync
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowError, FlowManageError, NodeValidError, RunningFlowRemoveError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.flow.handlers.monitor import NodeMonitorHandlerSet
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.node_handler import NodeHandler
from dataflow.flow.handlers.nodes.base_node.node_validate import NodeValidate
from dataflow.flow.models import FlowInfo, FlowNodeInstanceLinkChannel
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.utils.decorators import retry_db_operation_on_exception
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.transaction.meta_transaction_helper import MetaTransactionHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class MetaApiUtil(object):
    def __init__(self):
        # 创建元数据相关
        self._meta_api_operate_list = []

    def clean_meta_api_operate(self):
        """
        清除MetaApi集合事务信息
        """
        self._meta_api_operate_list = []

    def create_meta_api_operate(self):
        """
        调用MetaApi集合事务接口，配合 clean_meta_api_operate 使用
        """
        MetaTransactionHelper.create_meta_transaction(self._meta_api_operate_list)

    def append_meta_api_operate_list(self, operate_object, operate_type, operate_params):
        """
        追加元数据接口的写操作，最终会一起调用，形成一个事务操作
        """
        self._meta_api_operate_list.append(
            {
                "operate_object": operate_object,
                "operate_type": operate_type,
                "operate_params": operate_params,
            }
        )


class NodeController(object):
    """
    节点的控制类
    根据连线规则来优化节点操作逻辑，节点之间的交互以及创建DT等操作
    """

    @staticmethod
    def create_storages_node(current_node, result_table_id, form_data, channel_mode, ignore_channel=False):
        """
        创建结果表对应的存储，由 databus 保证元数据的正确性
        已存在则更新存储，已存在。batch->hdfs->batch，把 batch 后的隐藏 hdfs 拖出来
        """
        o_rt = ResultTable(result_table_id)
        if current_node.node_type == NodeTypes.HDFS_STORAGE and o_rt.has_storage_by_generate_type(
            NodeTypes.HDFS, NodeTypes.SYSTEM_TYPE
        ):
            NodeController.update_hdfs_physical_storage(result_table_id, NodeTypes.USER_TYPE, form_data)
        else:
            physical_table_args = current_node.build_rt_storage_config(
                result_table_id, form_data, get_request_username()
            )
            physical_table_args["channel_mode"] = channel_mode
            physical_table_args["ignore_channel"] = ignore_channel
            DatabusHelper.create_data_storages(**physical_table_args)

    @staticmethod
    def update_storages_node(current_node, result_table_id, form_data, channel_mode):
        """
        更新结果表对应的存储，由 databus 保证元数据的正确性
        """
        physical_table_args = current_node.build_rt_storage_config(result_table_id, form_data, get_request_username())
        physical_table_args["channel_mode"] = channel_mode
        DatabusHelper.update_data_storages(**physical_table_args)

    @staticmethod
    def update_hdfs_physical_storage(result_table_id, update_status, form_data=None):
        """
        update_status = system/user
        更新 hdfs 存储，hdfs 有两种状态：system 和 user
        system -> user，用户在画布拖出 hdfs，根据 form_data 来确定过期时间和集群位置
        user -> system，删除画布上的 hdfs，当 hdfs 链路还在时，更新为 system，过期时间和集群位置保持不变
        """
        if update_status == NodeTypes.USER_TYPE:
            physical_table_args = {
                "result_table_id": result_table_id,
                "cluster_name": form_data["cluster"],
                "cluster_type": NodeTypes.HDFS,
                "expires": str(form_data["expires"]) + ("d" if form_data["expires"] != -1 else ""),
                "storage_config": "{}",
                "generate_type": NodeTypes.USER_TYPE,
            }
        else:
            physical_table = StorekitHelper.get_physical_table(result_table_id, NodeTypes.HDFS)
            physical_table_args = {
                "result_table_id": result_table_id,
                "cluster_name": physical_table["cluster_name"],
                "cluster_type": NodeTypes.HDFS,
                "expires": physical_table["expires"],
                "storage_config": "{}",
                "generate_type": NodeTypes.SYSTEM_TYPE,
            }
        StorekitHelper.update_physical_table(**physical_table_args)

    @staticmethod
    def create_or_update_hdfs_channel(parent_node, hdfs_expires, parent_node_rt_id, is_create):
        """
        创建上游节点的 hdfs channel 存储，和其它链路不同，可以更新过期时间
        hdfs 存储为 system
        is_create=True，创建；is_create=False，更新
        """
        expires = str(hdfs_expires) + ("d" if hdfs_expires != -1 else "")
        physical_table_args = {
            "result_table_id": parent_node_rt_id,
            "cluster_name": NodeUtils.get_storage_cluster_name(
                parent_node.project_id,
                parent_node.default_storage_type,
                result_table_id=parent_node_rt_id,
            ),
            "cluster_type": NodeTypes.HDFS,
            "expires": expires,
            "storage_config": "{}",
            "generate_type": NodeTypes.SYSTEM_TYPE,
        }
        if is_create:
            StorekitHelper.create_physical_table(**physical_table_args)
        else:
            StorekitHelper.update_physical_table(**physical_table_args)

    @staticmethod
    def create_kafka_channel_storage(parent_node_rt_id):
        response_data = DatabusHelper.get_inner_kafka_channel_info(parent_node_rt_id)
        physical_table_args = {
            "result_table_id": parent_node_rt_id,
            "cluster_name": response_data["cluster_name"],
            "cluster_type": NodeTypes.KAFKA,
            # 为实时节点kafka存储添加过期时间，讨论结果：先用默认值3d
            "expires": "3d",
            "storage_config": "{}",
            "storage_channel_id": response_data["id"],
            "priority": response_data["priority"],
            "generate_type": NodeTypes.SYSTEM_TYPE,
        }
        return StorekitHelper.create_physical_table(**physical_table_args)

    @staticmethod
    def is_rt_meta_exist(result_table_id):
        meta_data = ResultTableHelper.get_result_table(result_table_id, not_found_raise_exception=False)
        if meta_data:
            return True
        else:
            return False

    @staticmethod
    def delete_rt_system_meta_storage(result_table_id, system_cluster_type):
        """
        删除节点的 system 存储
        """
        if NodeController.is_rt_meta_exist(result_table_id):
            o_rt = ResultTable(result_table_id)
            # merge->kafka->stream，删除 stream 的时候，kafka 不会被删
            # merge 后面的 kafka 是 user 存储
            if o_rt.has_storage_by_generate_type(system_cluster_type, NodeTypes.SYSTEM_TYPE):
                StorekitHelper.delete_physical_table(result_table_id, system_cluster_type)

    @staticmethod
    def update_meta_outputs(
        processing_id,
        result_table_id,
        storage_cluster_config_id,
        channel_cluster_config_id,
        storage_type,
    ):
        # 更新元数据
        metaApiUtil = MetaApiUtil()
        operate_params = {
            "processing_id": processing_id,
            "outputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": result_table_id,
                    "storage_cluster_config_id": storage_cluster_config_id,
                    "channel_cluster_config_id": channel_cluster_config_id,
                    "storage_type": storage_type,
                }
            ],
        }
        metaApiUtil.append_meta_api_operate_list("data_processing", "update", operate_params)
        metaApiUtil.create_meta_api_operate()

    @staticmethod
    def update_meta_hdfs_outputs(parent_node, parent_rt_id, downstream_node_id):
        """
        创建上游节点的 hdfs 存储，更新元数据
        例如：batch->hdfs->batch
             batch->hdfs->hdfs_storage
        父节点需补充 hdfs

        1. 没有 hdfs system，增加
        2. 有 hdfs system，更新
        """
        o_rt = ResultTable(parent_rt_id)
        parent_node_config = parent_node.get_config(False)
        if not o_rt.has_storage(NodeTypes.HDFS):
            expires = max(7, NodeUtils.get_max_window_size_by_day(parent_node_config) * 2)
            NodeController.create_or_update_hdfs_channel(parent_node, expires, parent_rt_id, True)
        elif o_rt.has_storage_by_generate_type(NodeTypes.HDFS, NodeTypes.SYSTEM_TYPE):
            # 否则若有 system 的存储，更新之(用户可能修改了窗口长度)
            # 获取 HDFS 的过期时间，转换为天
            max_related_window_size = NodeUtils.get_max_related_window_size(parent_rt_id)
            expires = max(
                7,
                NodeUtils.get_max_window_size_by_day(parent_node_config) * 2,
                max_related_window_size * 2,
            )
            NodeController.create_or_update_hdfs_channel(parent_node, expires, parent_rt_id, False)

        # 记录 dataflow_node_instance_link_channel 表
        FlowNodeInstanceLinkChannel.save_channel_storage(
            result_table_id=parent_rt_id,
            channel_storage=NodeTypes.HDFS,
            downstream_node_id=downstream_node_id,
            channel_mode=parent_node.channel_mode,
            created_by=get_request_username(),
        )
        # 获取上一步创建的存储信息
        storage_cluster_config_id, channel_cluster_config_id = ResultTable(parent_rt_id).get_storage_id(
            NodeTypes.HDFS, "storage"
        )
        NodeController.update_meta_outputs(
            parent_node.processing_id,
            parent_rt_id,
            storage_cluster_config_id,
            channel_cluster_config_id,
            "storage",
        )

    @staticmethod
    def update_meta_kafka_outputs(parent_node, parent_node_rt_id, downstream_node_id):
        """
        创建上游节点的 kafka 存储，更新元数据
        例如：stream->kafka->flink_streaming，父节点需补充 kafka
        """
        o_rt = ResultTable(parent_node_rt_id)
        if not o_rt.has_storage(NodeTypes.KAFKA):
            channel_info = NodeController.create_kafka_channel_storage(parent_node_rt_id)
            storage_cluster_config_id, channel_cluster_config_id = (
                None,
                channel_info["storage_channel_id"],
            )
        else:
            storage_cluster_config_id, channel_cluster_config_id = o_rt.get_storage_id(NodeTypes.KAFKA, "channel")
        # kafka 被下游多个节点公用，都要记录
        # 记录 dataflow_node_instance_link_channel 表
        FlowNodeInstanceLinkChannel.save_channel_storage(
            result_table_id=parent_node_rt_id,
            channel_storage=NodeTypes.KAFKA,
            downstream_node_id=downstream_node_id,
            channel_mode=parent_node.channel_mode,
            created_by=get_request_username(),
        )
        NodeController.update_meta_outputs(
            parent_node.processing_id,
            parent_node_rt_id,
            storage_cluster_config_id,
            channel_cluster_config_id,
            "channel",
        )

    @staticmethod
    def create_or_update_dp_meta(parent_node, current_node, parent_node_rt_id):
        """
        创建和更新计算节点之间的元数据，因为是一个，所以抽成公共函数
        增加链路的时候都是先判断是否存在，不存在再创建。对于 merge->[实时]，
        本身 merge 节点下游存在 kafka，则只更新元数据就行
        """
        # 获取 channel 链路
        channel_type = NodeInstanceLink.get_link_path_channel(parent_node, current_node)
        # 当前节点已经入库，生成自增的节点 id
        downstream_node_id = current_node.node_id
        # stream->memory->stream 不会进入判断逻辑
        # TODO 应该是通用的方法，但因为每个 channel 创建逻辑不同，才拆开
        if channel_type == NodeTypes.HDFS:
            # 路径中有 hdfs 存储，需要补充 hdfs
            NodeController.update_meta_hdfs_outputs(parent_node, parent_node_rt_id, downstream_node_id)
        elif channel_type == NodeTypes.KAFKA:
            # 路径中有 kafka 存储，需要补充 kafka
            NodeController.update_meta_kafka_outputs(parent_node, parent_node_rt_id, downstream_node_id)
        elif channel_type == NodeTypes.TDW:
            # 路径中有 tdw 存储，需要补充 tdw
            NodeController.update_meta_tdw_outputs(parent_node, parent_node_rt_id, downstream_node_id)

    @staticmethod
    def create_upstream_meta_info(current_node, from_node_ids, form_data):
        """
        保存节点时创建上游的元数据相关配置，具体如下（计算到存储，计算到计算之间必有链路）
        数据源 -> 计算节点，不需要在元数据中做额外的记录。需要考虑的有：
        a)  计算/算法 -> 存储节点，中间会新加 kafka 以及 kafka 到该存储的 DT 操作
            目前这部分操作交给 databus 模块处理，但是 flow 需要传参 ignore_channel，
            默认为 false。若为 true，意为当上游连接的存储节点都被删除，仍然保留元数据
            中的 kafka 链路；若为 false，当计算节点下游的存储都被删除后，kafka 链路也
            会被删除
        b)  计算/算法 -> 计算/算法，中间补 kafka 或者 hdfs 节点，并更新 DP 的输出为新创建
            的 kafka/hdfs 链路
        """
        # 获取上游节点信息
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(current_node.node_type, from_node_ids, form_data)
        for upstream_node in upstream_nodes_info:
            parent_node = upstream_node["upstream_node"]
            # 目前计算节点只有一个 rt 输出
            from_result_table_id = upstream_node["result_table_id"][0]
            # a) 当前是存储节点。注意：TDW 存储节点是属于需要创建 DP 的存储节点，不属于 STORAGE_CATEGORY
            if current_node.node_type in NodeTypes.STORAGE_CATEGORY:
                """
                对于 [实时] -> [存储]，create_data_storages，databus 会补上 kafka，但 flow 补了也不会影响
                对于 [离线] -> [存储]，databus 不会补 hdfs，需要自己补
                1. stream -> kafka -> tspider，补 kafka，再调 databus
                2. batch -> hdfs -> tspider，补 hdfs，再调 databus
                3. batch -> hdfs -> hdfs_storage，补 hdfs，调 databus 发现 hdfs 已存在，只更新 system 为 user
                对于 hdfs_storage，需要有特殊的逻辑
                """
                if parent_node.node_type in NodeTypes.PROCESSING_CATEGORY:
                    # [数据源]->[存储] 不会进入此逻辑
                    NodeController.create_or_update_dp_meta(parent_node, current_node, from_result_table_id)
                NodeController.create_storages_node(
                    current_node,
                    from_result_table_id,
                    form_data,
                    parent_node.channel_mode,
                )
            # b) 当前是计算节点，且上游也是计算节点。利用连线规则在中间补 kafka 或 hdfs 或 tdw，更新元数据
            elif (
                current_node.node_type in NodeTypes.PROCESSING_CATEGORY
                and parent_node.node_type in NodeTypes.PROCESSING_CATEGORY
            ):
                # 创建当前节点信息，需要当前节点的 form_data，而创建上游链路是不需要当前节点的表单信息的
                NodeController.create_or_update_dp_meta(parent_node, current_node, from_result_table_id)

    @staticmethod
    def delete_upstream_meta_info(current_node):
        """
        删除节点前先删除上游元数据，通过表 dataflow_node_instance_link_channel 中的记录删除
        记录中有，先删除表，确认链路是否删除，若链路被其它 rt 使用，则不删；否则删
        """
        # 获取上游节点
        del_node_id = current_node.node_id
        del_node_type = current_node.node_type
        # 只处理当前为存储和计算节点,数据源节点删除时无需额外操作。
        if del_node_type in NodeTypes.STORAGE_CATEGORY:
            channel_storage_list = FlowNodeInstanceLinkChannel.get_channel_storage(del_node_id)
            channel_storage_list_num = len(channel_storage_list)
            if channel_storage_list_num == 0:
                # 无链路，数据源->存储
                DatabusHelper.delete_data_storages(current_node.result_table_ids[0], current_node.storage_type, "other")
            else:
                # 存储节点只有 1 个上游
                channel_obj = channel_storage_list[0]
                rt_id = channel_obj.result_table_id
                del_channel_storage = channel_obj.channel_storage
                channel_mode = channel_obj.channel_mode
                if not FlowNodeInstanceLinkChannel.is_channel_storage_used(rt_id, del_channel_storage):
                    # 若当前链路没用到
                    DatabusHelper.delete_data_storages(rt_id, current_node.storage_type, channel_mode)
                    NodeController.delete_rt_system_meta_storage(rt_id, del_channel_storage)
                else:
                    # 链路有被用到
                    if del_channel_storage == NodeTypes.HDFS:
                        # batch -> hdfs -> hdfs_storage, batch -> hdfs -> tspider
                        if del_node_type == NodeTypes.HDFS_STORAGE:
                            NodeController.update_hdfs_physical_storage(rt_id, NodeTypes.SYSTEM_TYPE)
                        else:
                            # batch->hdfs->[其它存储]，中间必有 pulsar
                            # 对于 hdfs->pulsar，ignore_channel=False 才能删除级联的 pulsar
                            DatabusHelper.delete_data_storages(rt_id, current_node.storage_type, channel_mode)
                    else:
                        DatabusHelper.delete_data_storages(rt_id, current_node.storage_type, channel_mode, True)
                # 无论是否删除链路，表记录都要删除
                channel_obj.delete()
        elif del_node_type in NodeTypes.PROCESSING_CATEGORY:
            # 当前是计算节点，必有链路存在(除了走内存)，且可能是多条
            channel_storage_list = FlowNodeInstanceLinkChannel.get_channel_storage(del_node_id)
            for channel_obj in channel_storage_list:
                rt_id = channel_obj.result_table_id
                del_channel_storage = channel_obj.channel_storage
                if not FlowNodeInstanceLinkChannel.is_channel_storage_used(rt_id, del_channel_storage):
                    # 未被使用，直接删除
                    NodeController.delete_rt_system_meta_storage(rt_id, del_channel_storage)
                else:
                    pass
                # 无论是否删除链路，表记录都要删除
                channel_obj.delete()

    @staticmethod
    def update_upstream_meta_info(current_node, from_node_ids, form_data):
        """
        更新节点时更新上游的元数据相关配置
        思想同 create_upstream_meta_info 函数
        """
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(current_node.node_type, from_node_ids, form_data)
        for upstream_node in upstream_nodes_info:
            parent_node = upstream_node["upstream_node"]
            from_result_table_id = upstream_node["result_table_id"][0]
            if current_node.node_type in NodeTypes.STORAGE_CATEGORY:
                NodeController.update_storages_node(
                    current_node,
                    from_result_table_id,
                    form_data,
                    parent_node.channel_mode,
                )
            elif (
                current_node.node_type in NodeTypes.PROCESSING_CATEGORY
                and parent_node.node_type in NodeTypes.PROCESSING_CATEGORY
            ):
                NodeController.create_or_update_dp_meta(parent_node, current_node, from_result_table_id)

    @staticmethod
    @auto_meta_sync(using="default")
    @retry_db_operation_on_exception
    def create(node_type, operator, flow_id, from_links, config_params, frontend_info=None):
        """
        增加节点
        """
        # 1. 校验相关
        NodeValidate.node_count_validate(flow_id)
        NodeValidate.tdw_auth_validate(flow_id, node_type)
        NodeValidate.from_links_validate(flow_id, from_links)
        # 2. 创建待添加节点
        create_node = NODE_FACTORY.get_node_handler_by_type(node_type)
        form_data = create_node.clean_config(config_params, True)
        # 节点权限校验
        NodeValidate.node_auth_check(create_node, flow_id, form_data)
        # 3. 添加节点本身
        build_form_data = create_node.add_node_info(
            operator, flow_id, from_links, form_data, frontend_info=frontend_info
        )
        # 4. 补全上下文DP，DT关系（记录在元数据中）
        from_node_ids = [link["source"]["node_id"] for link in from_links]
        NodeController.create_upstream_meta_info(create_node, from_node_ids, build_form_data)
        # 5. 创建额外的资源，例如：计算节点创建 processing 等
        rts_heads_tails = create_node.add_after(operator, from_node_ids, build_form_data)
        # 6. 新增上下游关系在 relation 表中
        bk_biz_id = form_data["bk_biz_id"]
        create_node.update_node_metadata_relation(
            bk_biz_id,
            rts_heads_tails["result_table_ids"],
            rts_heads_tails["heads"],
            rts_heads_tails["tails"],
        )
        return create_node

    @staticmethod
    @auto_meta_sync(using="default")
    @retry_db_operation_on_exception
    def partial_update(node_id, request_data, from_nodes):
        """
        局部更新节点参数
        相比 update，参数是从数据库中读取而不完全依赖表单
        """
        partial_update_node = NODE_FACTORY.get_node_handler(node_id=node_id)
        # 配置参数
        node_params = partial_update_node.get_config()
        node_params.update(request_data)
        # 当前节点校验器基本都需要 from_result_table_ids
        if from_nodes:
            from_result_table_ids = []
            for from_node in from_nodes:
                from_result_table_ids.extend(from_node["from_result_table_ids"])
            node_params["from_result_table_ids"] = from_result_table_ids
        form_data = node_params
        # 坐标信息
        from_node_ids = partial_update_node.from_node_ids
        frontend_info = partial_update_node.frontend_info
        # 保存旧节点信息，主要用于更新操作的回滚
        prev_node = model_to_dict(partial_update_node.node_info)
        # 存储节点用到
        prev_node["result_table_ids"] = partial_update_node.result_table_ids
        # 1. 更新节点自身
        # 节点权限校验
        build_form_data = partial_update_node.update_node_info(
            get_request_username(), from_node_ids, form_data, frontend_info
        )
        # 保存新节点信息，校验新旧节点
        after_node = model_to_dict(partial_update_node.node_info)
        # 2. 更新上游元数据关系
        NodeController.update_upstream_meta_info(partial_update_node, from_node_ids, form_data)
        # 3. 更新额外的资源，例如：更新 processing 等
        rts_heads_tails = partial_update_node.update_after(
            get_request_username(),
            from_node_ids,
            build_form_data,
            prev=prev_node,
            after=after_node,
        )
        # 4. 更新 relation 表
        bk_biz_id = form_data["bk_biz_id"]
        partial_update_node.update_node_metadata_relation(
            bk_biz_id,
            rts_heads_tails["result_table_ids"],
            rts_heads_tails["heads"],
            rts_heads_tails["tails"],
        )
        return partial_update_node

    @staticmethod
    @auto_meta_sync(using="default")
    @retry_db_operation_on_exception
    def update(
        operator,
        flow_id,
        node_id,
        from_links,
        request_data,
        frontend_info=None,
        is_self_param=False,
        is_self_link=False,
    ):
        """
        更新分为两种情况：
        1. 用户修改画布，更新表单。is_self_param,is_self_link=False
        2. flow 启动的时候，校验会保存每个节点，此时更新每个节点。is_self_param,is_self_link=True
        is_self_param,is_self_link 代表更新数据来自表单还是数据库，默认为 False 来自表单
        """
        # 1. 参数校验
        update_node = NODE_FACTORY.get_node_handler(node_id=node_id)
        if is_self_param:
            frontend_info = update_node.frontend_info
        form_data = update_node.clean_config(request_data, False)
        # 节点权限校验
        NodeValidate.node_auth_check(update_node, flow_id, form_data, False)
        if is_self_link:
            from_node_ids = update_node.from_node_ids
            frontend_info = update_node.frontend_info
        else:
            NodeValidate.from_links_validate(flow_id, from_links)
            from_node_ids = [link["source"]["node_id"] for link in from_links]
            update_node.node_info.create_or_update_link(from_links)
        # 保存旧节点信息，主要用于更新操作的回滚
        prev_node = model_to_dict(update_node.node_info)
        # 存储节点用到
        prev_node["result_table_ids"] = update_node.result_table_ids
        # 2. 更新节点自身
        build_form_data = update_node.update_node_info(operator, from_node_ids, form_data, frontend_info=frontend_info)
        # 保存新节点信息，校验新旧节点
        after_node = model_to_dict(update_node.node_info)
        # 3. 更新上游元数据关系
        NodeController.update_upstream_meta_info(update_node, from_node_ids, form_data)
        # 4. 更新额外的资源，例如：更新 processing 等
        rts_heads_tails = update_node.update_after(
            operator, from_node_ids, build_form_data, prev=prev_node, after=after_node
        )
        # 5. 更新 relation 表
        bk_biz_id = form_data["bk_biz_id"]
        update_node.update_node_metadata_relation(
            bk_biz_id,
            rts_heads_tails["result_table_ids"],
            rts_heads_tails["heads"],
            rts_heads_tails["tails"],
        )
        return update_node

    @staticmethod
    @auto_meta_sync(using="default")
    @retry_db_operation_on_exception
    def delete(operator, node_id):
        """
        删除节点
        和增加节点相反，先删除上游元数据，再删除节点自身
        """
        del_node = NODE_FACTORY.get_node_handler(node_id=node_id)
        if del_node.status in FlowInfo.PROCESS_STATUS:
            raise Errors.RunningNodeRemoveError(_("任务处于操作状态，当前节点不允许删除"))
        FlowHandler.validate_related_source_nodes(del_node)
        status = {}
        for to_node in del_node.get_to_nodes_handler():
            status[to_node.node_id] = to_node.display_status
        # 1. 先删除上游元数据
        NodeController.delete_upstream_meta_info(del_node)
        # 2. 删除节点关联的processing等（如果存在）
        del_node.remove_before(operator)
        # 3. 再删除节点
        del_node.remove_node_info()
        return status

    @staticmethod
    def get_node_info_dict(node_id):
        """
        获取节点
        """
        node = NODE_FACTORY.get_node_handler(node_id=node_id)
        node_info_dict = node.to_dict(add_has_modify=True)
        return node_info_dict

    # 对 flow 的批量操作
    @staticmethod
    def add_nodes(flow_handler, username, nodes):
        """
        为flow按顺序增加节点，并保证操作失败后回退删除创建成功的节点
        """
        import_node_id_to_real_node_id_map = {}
        if flow_handler.nodes:
            for _n in flow_handler.nodes:
                import_node_id_to_real_node_id_map[_n.node_id] = _n.node_id
        # 保存成功的节点信息，key为node_id, value为创建成功后的相关节点信息
        success_added_nodes = []
        # 缓存未保存的的节点列表
        node_arr = nodes
        try:
            # 一轮一轮把可以准确的前后次序的节点保存下来，如果节点次序是准确的，则一轮就可以全部保存
            # sample 1: [A, B(from A), C(from B)]，一轮保存 A、B、C
            # sample 2: [A, C(from B), B(from A)]，一轮保存 A、B，二轮保存 C
            node_arr_num = len(node_arr)
            while node_arr_num > 0:
                origin_node_arr = node_arr
                # 将 iter_arr 中未保存的节点保存到 node_arr 中
                node_arr = []
                for node_info in origin_node_arr:
                    logger.info(
                        _("尝试保存节点，节点名称=%(node_name)s, 上游节点信息=%(from_nodes)s")
                        % {
                            "node_name": node_info["name"],
                            "from_nodes": node_info["from_nodes"],
                        }
                    )
                    # 检查节点所依赖的父节点是否保存，如果已经保存，则可以保存当前节点
                    # 若当前待保存节点的所有父节点都在已保存的节点列表 parent_id_node_id_map 中，则可以创建 (数据源节点必先保存成功)
                    if {x["id"] for x in node_info["from_nodes"]}.issubset(
                        list(import_node_id_to_real_node_id_map.keys())
                    ) or node_info["node_type"] in NodeTypes.SOURCE_CATEGORY:
                        node = NodeController.insert_flow_node(
                            flow_handler,
                            username,
                            node_info,
                            import_node_id_to_real_node_id_map,
                        )
                        logger.info(
                            _("成功保存节点，节点名称=%(node_name)s, 上游节点信息=%(from_nodes)s")
                            % {
                                "node_name": node_info["name"],
                                "from_nodes": node_info["from_nodes"],
                            }
                        )
                        import_node_id_to_real_node_id_map[node_info["id"]] = node.node_id
                        success_added_nodes.append(node)
                        # 循环导入控制时间
                        time.sleep(0.5)
                    else:
                        node_arr.append(node_info)

                # 经过一轮循环，跳过的节点数 = 循环前的节点数，则表示节点中存在非法父节点，或者节点间存在回路
                if len(node_arr) == len(origin_node_arr):
                    invalid_ids = [str(node_info["id"]) for node_info in node_arr]
                    raise NodeValidError("存在非法节点关系，id=%s" % ",".join(invalid_ids))

            return {"node_ids": [_n.node_id for _n in success_added_nodes]}
        except FlowError as e1:
            try:
                logger.exception(e1)
                rollback_msg = ""
                # 增加节点回滚操作，按添加节点的反顺序删除成功增加的节点
                for _n in success_added_nodes[::-1]:
                    NodeController.delete(username, _n.node_id)
                    # _n.remove_node_info()
            except FlowError as e2:
                logger.exception(e2)
                rollback_msg = _("，回滚失败(flow_id=%(flow_id)s)，%(message)s") % {
                    "flow_id": flow_handler.flow_id,
                    "message": e2.message,
                }
            # 若是创建全新的flow，删除flow的操作由外围调用完成
            raise FlowManageError(
                _("增加节点失败，明细(%(message)s)%(rollback_msg)s") % {"message": e1.message, "rollback_msg": rollback_msg}
            )

    @staticmethod
    def insert_flow_node(flow_handler, username, node_info, import_node_id_to_real_node_id_map):
        """
        根据节点信息，将节点创建到当前flow中
        """
        node_type = node_info["node_type"]
        if node_type not in list(NODE_FACTORY.NODE_MAP.keys()):
            raise NodeValidError(
                _("节点(%(name)s)创建失败，非法节点类型(%(node_type)s)") % {"name": node_info["name"], "node_type": node_type}
            )

        # 生成默认节点位置
        if "frontend_info" in node_info and node_info["frontend_info"]:
            frontend_info = node_info["frontend_info"]
        else:
            frontend_info = flow_handler._gene_default_frontend_info()
        # 构造上游节点关联关系
        from_links = []
        if node_type not in NodeTypes.SOURCE_CATEGORY:
            for from_node in node_info["from_nodes"]:
                # 外围需要保证_id_node_id_map中包含上游节点的node_id
                from_links.append(
                    {
                        "source": {
                            "node_id": import_node_id_to_real_node_id_map[from_node["id"]],
                            "id": NodeHandler._gene_default_link_id(),
                            "arrow": "Right",
                        },
                        "target": {
                            "id": NodeHandler._gene_default_link_id(),
                            "arrow": "Left",
                        },
                    }
                )
                # 更新 node_id
                from_node["id"] = import_node_id_to_real_node_id_map[from_node["id"]]
        create_node_id = None
        try:
            node_obj = NodeController.create(
                node_type,
                username,
                flow_handler.flow_id,
                from_links,
                node_info,
                frontend_info,
            )
            create_node_id = node_obj.node_id
        except Exception as e:
            logger.exception(e)
            try:
                if create_node_id:
                    NodeController.delete(username, create_node_id)
            except Exception as del_ex:
                raise NodeValidError(_(u"创建节点(%s)失败后，回滚失败，message=(%s)") % (node_info["name"], "{}".format(del_ex)))
            raise NodeValidError(
                _("节点(%(name)s)创建失败，%(message)s") % {"name": node_info["name"], "message": "{}".format(e)}
            )

        return node_obj

    @staticmethod
    def del_flow_nodes(flow_handler, is_delete_flow=True):
        """
        批量删除 flow 的所有节点
        is_delete_flow 是否连画布也一并删除
        """
        if flow_handler.status in FlowInfo.PROCESS_STATUS:
            raise RunningFlowRemoveError()
        for _n in flow_handler.nodes:
            FlowHandler.validate_related_source_nodes(_n, include_current_flow=False)

        for _node in flow_handler.get_ordered_nodes(reverse=True):
            node_id = _node.node_id
            node_name = _node.name
            try:
                NodeController.delete(get_request_username(), node_id)
                logger.info(_("删除节点(%(node_name)s)，node_id=%(node_id)s") % {"node_name": node_name, "node_id": node_id})
            except Exception as e:
                logger.warning(
                    _("删除任务期间节点(%(node_name)s)删除失败，node_id=%(node_id)s, exception=%(ex_msg)s")
                    % {"node_name": node_name, "node_id": node_id, "ex_msg": "{}".format(e)}
                )
                # raise Exception(_(u"删除任务期间节点(%(node_name)s)删除失败，node_id=%(node_id)s") %
                #                 {'node_name': node_name, 'node_id': node_id})

        # 清理 flow 的节点 cache 信息
        flow_handler.init_nodes_cache()

        if is_delete_flow:
            with auto_meta_sync(using="default"):
                flow_handler.flow.delete()

    @staticmethod
    def list_monitor_data(flow_id, node_id, monitor_type):
        """
        获取监控数据
        """
        node_handler = NODE_FACTORY.get_node_handler(node_id=node_id)
        if node_handler.flow_id != int(flow_id):
            raise NodeValidError(_("节点(id:{node_id})不属于该Flow").format(node_id=node_id))
        monitor = NodeMonitorHandlerSet(node_handler, monitor_type)
        response_data = monitor.list_monitor_data()
        return response_data
