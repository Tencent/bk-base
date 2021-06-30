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

from common.local import get_request_username
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.node_handler import NodeHandler, call_func_with_rollback
from dataflow.flow.models import FlowInfo, FlowNodeProcessing
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class ProcessingNode(NodeHandler):
    """
    包括stream|batch|model_app等计算节点，也包括计算型的固化节点(合流、分流)
    """

    # 一个节点对应一个processing_id
    _processing_id = None

    @property
    def processing_id(self):
        if self._processing_id is None:
            processing_ids = [
                processing.processing_id
                for processing in FlowNodeProcessing.objects.filter(node_id=self.node_info.node_id)
            ]
            if processing_ids:
                self._processing_id = processing_ids[0]
        return self._processing_id

    @processing_id.setter
    def processing_id(self, processing_id):
        self._processing_id = processing_id

    @property
    def processing_type(self):
        """
        计算节点的processing_type，如batch|stream|model_app，当前应用的场景包括：
        1. 为计算节点创建存储时获取物理表名
        @return:
        """
        raise NotImplementedError("计算节点类型（%s）未定义processing_type." % self.node_type)

    def build_processing_id(self, bk_biz_id, table_name):
        self.processing_id = "{}_{}".format(bk_biz_id, table_name)
        return self.processing_id

    def update_node_metadata_relation(self, bk_biz_id, result_table_ids, heads=None, tails=None):
        """
        更新节点与rt、processing等实体的关联关系
        @param bk_biz_id:
        @param result_table_ids: 不允许为空，对于没有虚拟RT的情况，一般heads=tails=result_table_ids
        @param heads: 当前processing的heads
        @param tails: 当前processing的tails，不允许为空
        """
        if not tails or not result_table_ids:
            raise Errors.APICallError(_("更新processing操作返回RT信息为空，请联系管理员."))
        if not set(heads).issubset(result_table_ids) or not set(tails).issubset(result_table_ids):
            logger.exception(
                _("heads(%(heads)s),tails(%(tails)s),result_table_ids(%(result_table_ids)s)")
                % {
                    "heads": ",".join(heads),
                    "tails": ",".join(tails),
                    "result_table_ids": ",".join(result_table_ids),
                }
            )
            raise Errors.APICallError(_("更新processing操作返回RT信息错误，请联系管理员."))
        # 构建已创建RT与节点的关联关系
        rt_info_dict = {}
        for result_table_id in result_table_ids:
            generate_type = "system"
            is_head = False
            if result_table_id in tails:
                generate_type = "user"
            if result_table_id in heads:
                is_head = True
            rt_info_dict[result_table_id] = {
                "generate_type": generate_type,
                "is_head": is_head,
            }
        self.node_info.update_as_processing(bk_biz_id, self.processing_id, self.processing_type, rt_info_dict)

    def get_delete_result_table_detail(self, result_table_ids_to_remove):
        """
        分析待删除的 RT 列表，判断哪些 RT 需要删除
        @param result_table_ids_to_remove: set 类型
        @return:
        """
        ret = {"downstream_link_to_delete": [], "result_table_ids_to_delete": []}
        if not result_table_ids_to_remove:
            return ret
        # 需要删除线的
        downstream_link_to_delete = set()
        # 可能需要删除线的(跨画布不删线)，不删除RT
        rt_ids_used_as_related_storage = set()
        for to_node in self.get_to_nodes_handler():
            # 对于当前下游节点，使用到即将删除的 RT 列表
            rt_ids_used_as_downstream_node = set(to_node.from_result_table_ids) & result_table_ids_to_remove
            if not rt_ids_used_as_downstream_node:
                continue
            if to_node.node_type in NodeTypes.STORAGE_CATEGORY:
                # rt_ids_used_as_downstream_storage 的 RT 列表不可删除，其它都可以删除(如计算节点使用了)
                rt_ids_used_as_related_storage.update(to_node.from_result_table_ids)
            downstream_link_to_delete.add(to_node.node_id)

        # PS: 跨画布，若仅被引用为数据源，可以删除
        # 获取被引用为存储节点的 RT
        for result_table_id in result_table_ids_to_remove:
            if NodeUtils.get_related_nodes_by_rt(result_table_id, NodeTypes.STORAGE_CATEGORY):
                rt_ids_used_as_related_storage.add(result_table_id)
        logger.info(
            "RT has related storages for node({}): {}".format(self.node_id, ",".join(rt_ids_used_as_related_storage))
        )
        ret["downstream_link_to_delete"] = list(downstream_link_to_delete)
        ret["result_table_ids_to_delete"] = list(result_table_ids_to_remove - rt_ids_used_as_related_storage)
        return ret

    def delete_multi_dp_with_rollback(self, delete_func, result_table_ids, recycle_system_storage=True):
        """
        多输出删除 DP 方法。先删存储再删 processing，若 processing 删除失败则回滚
        @param delete_func: 待调用的删除函数
        @param result_table_ids: dp 产生的结果表列表
        @param recycle_system_storage: 是否回收系统存储，删除失败要回滚
        @return:
        """
        _delete_result_table_detail = self.get_delete_result_table_detail(set(result_table_ids))
        delete_result_tables = _delete_result_table_detail["result_table_ids_to_delete"]
        if self.node_type == NodeTypes.SPLIT_KAFKA:
            with_data = True
        else:
            with_data = len(delete_result_tables) == len(self.result_table_ids)
        rollback_func_info_list = []
        # 回滚删除操作
        call_func_with_rollback(
            delete_func,
            {
                "processing_id": self.processing_id,
                "with_data": with_data,
                "delete_result_tables": delete_result_tables,
            },
            rollback_func_info_list,
        )
        self.node_info.remove_as_processing()

    def delete_dp_with_rollback(self, delete_func, recycle_system_storage=True):
        """
        单输出删除 DP 方法。先删存储再删 processing，若 processing 删除失败则回滚。
        TODO: DP 若支持多输出，需切换到调用 delete_multi_output_dp_with_rollback
        @param delete_func: 待调用的删除函数
        @param recycle_system_storage: 是否回收系统存储，删除失败要回滚
        @return:
        """
        _delete_result_table_detail = self.get_delete_result_table_detail(set(self.result_table_ids))
        delete_result_tables = _delete_result_table_detail["result_table_ids_to_delete"]
        # 若待删除 RT 列表和 DP 的输出列表一致，with_data 为 true，删除 DP 时所有的 RT
        with_data = len(delete_result_tables) == len(self.result_table_ids)
        # 只要是系统存储，在删除 DP 时都需要删除
        # system_cluster_types = [self.default_storage_type, self.channel_storage_type]
        rollback_func_info_list = []
        # 回滚操作
        call_func_with_rollback(
            delete_func,
            {"processing_id": self.processing_id, "with_data": with_data},
            rollback_func_info_list,
        )
        self.node_info.remove_as_processing()

    def collect_multi_output_update_info(self, result_table_ids):
        """
        收集多输出节点更新操作时删除操作信息
        注意，运行中的 RT 关联节点连线不可删除
        @param result_table_ids:
        @return:
            {
                'downstream_link_to_delete'   // 需要删除的下游节点与当前节点对应的连线
                'result_table_ids_to_delete'     // 需要删除的结果表
            }
        """
        data_processing = DataProcessingHelper.get_data_processing(self.processing_id)
        pre_result_table_ids = [output["data_set_id"] for output in data_processing["outputs"]]
        result_table_ids_to_remove = set(pre_result_table_ids) - set(result_table_ids)
        if result_table_ids_to_remove and self.status not in [FlowInfo.STATUS.NO_START]:
            raise Errors.NodeError("运行中的任务不允许移除结果表(%s)" % ", ".join(result_table_ids_to_remove))
        return self.get_delete_result_table_detail(result_table_ids_to_remove)

    def refresh_result_table_storages(self, result_table_ids):
        """
        与即将更新的 RT 进行比较，删除即将移除的 RT 关联关系(需检查是否被关联使用)
        @param result_table_ids:
        @return:
            {
                'downstream_link_to_delete'   // 需要删除的下游节点与当前节点对应的连线
                'result_table_ids_to_delete'  // 需要删除的结果表
                'rollback_func_info_list'     // 回滚操作列表
            }
        """
        _delete_info = self.collect_multi_output_update_info(result_table_ids)
        result_table_ids_to_delete = _delete_info["result_table_ids_to_delete"]
        rollback_func_info_list = []
        for result_table_id in result_table_ids_to_delete:
            o_rt = ResultTable(result_table_id)
            for system_storage in {self.default_storage_type, self.channel_storage_type}:
                if o_rt.has_storage_by_generate_type(system_storage, "system"):
                    # 删除关联关系若错误需要回滚
                    call_func_with_rollback(
                        StorekitHelper.delete_physical_table,
                        {
                            "result_table_id": result_table_id,
                            "cluster_type": system_storage,
                        },
                        rollback_func_info_list,
                    )
                    # 记录已正常删除的关联关系列表
                    rollback_func_info_list.append(
                        {
                            "func": StorekitHelper.delete_rollback,
                            "params": {
                                "result_table_id": result_table_id,
                                "cluster_type": system_storage,
                            },
                        }
                    )
        ret = {"rollback_func_info_list": rollback_func_info_list}
        ret.update(_delete_info)
        return ret

    # 增加数据修正功能：增删改查
    def create_or_update_data_correct(self, node_params):
        node_config = self.get_config()
        flow_id = self.flow_id
        nid = self.node_id
        """
        增加数据修正功能
        1. 必须有 sql 参数，即只有实时和离线有修正需求
        2. 点击更新按钮后:
            a)判断是更新修正还是创建修正
            b)如果有修正的参数，需要创建修正
        """
        if self.node_type in NodeTypes.CORRECT_SQL_CATEGORY and "sql" in node_params:
            if "data_correct" in node_params and "is_open_correct" in node_params["data_correct"]:
                # 开启开关后，进行数据修正相关逻辑
                if node_params["data_correct"]["is_open_correct"]:
                    # 更新或者创建数据修正
                    rt_id = "{}_{}".format(
                        node_params["bk_biz_id"],
                        node_params["table_name"],
                    )
                    if "correct_config_id" in node_config and node_config["correct_config_id"]:
                        # 修正 ID 已经存在，即更新修正配置
                        response_data = DatamanageHelper.update_data_correct(
                            {
                                "correct_config_id": node_config["correct_config_id"],
                                "bk_username": get_request_username(),
                                "source_sql": node_params["sql"],
                                "correct_configs": node_params["data_correct"]["correct_configs"],
                            }
                        )
                    else:
                        # 修正 ID 不存在，即创建修正配置
                        response_data = DatamanageHelper.create_data_correct(
                            {
                                "bk_username": get_request_username(),
                                "data_set_id": rt_id,
                                "bk_biz_id": node_params["bk_biz_id"],
                                "flow_id": flow_id,
                                "node_id": nid,
                                "source_sql": node_params["sql"],
                                "correct_configs": node_params["data_correct"]["correct_configs"],
                            }
                        )
                    # 保存 correct_config_id
                    node_params["correct_config_id"] = response_data["correct_config_id"]
                # 无论是否开启开关，都应该更新 is_open_correct 参数
                node_params["is_open_correct"] = node_params["data_correct"]["is_open_correct"]
        # 返回增加修正后的参数
        return node_params

    def del_data_correct(self):
        node_config = self.get_config(False)
        # 删除数据修正配置
        if "correct_config_id" in node_config and node_config["correct_config_id"]:
            # correct_config_id 配置不为空
            DatamanageHelper.del_data_correct({"correct_config_id": node_config["correct_config_id"]})

    @staticmethod
    def get_data_correct(node_handler, response_data):
        """
        增加获取修正配置
        """
        node_config = node_handler.get_config()
        if "correct_config_id" in node_config and node_config["correct_config_id"]:
            # correct_config_id 配置不为空
            correct_data = DatamanageHelper.get_data_correct({"correct_config_id": node_config["correct_config_id"]})
            # 返回给前端的配置, 增加数据修正相关
            response_data["node_config"]["data_correct"] = {
                "is_open_correct": node_config["is_open_correct"],
                "correct_configs": correct_data["correct_configs"],
            }
        return response_data

    # 模型应用实例
    @staticmethod
    def get_data_model_instance(model_instance_id):
        model_app_data = DatamanageHelper.get_data_model_instance({"model_instance_id": model_instance_id})
        return model_app_data

    @staticmethod
    def create_data_model_instance(params):
        return DatamanageHelper.create_data_model_instance(params)

    @staticmethod
    def update_data_model_instance(params):
        return DatamanageHelper.update_data_model_instance(params)

    @staticmethod
    def del_data_model_instance(model_instance_id):
        return DatamanageHelper.del_data_model_instance({"model_instance_id": model_instance_id})

    @staticmethod
    def rollback_data_model_instance(model_instance_id, rollback_id):
        DatamanageHelper.rollback_data_model_instance(
            {"model_instance_id": model_instance_id, "rollback_id": rollback_id}
        )
        return

    # 模型实例指标
    @staticmethod
    def get_data_model_indicator(model_instance_id, result_table_id):
        model_app_data = DatamanageHelper.get_data_model_indicator(
            {"model_instance_id": model_instance_id, "result_table_id": result_table_id}
        )
        return model_app_data

    @staticmethod
    def create_data_model_indicator(params):
        return DatamanageHelper.create_data_model_indicator(params)

    @staticmethod
    def update_data_model_indicator(params):
        return DatamanageHelper.update_data_model_indicator(params)

    @staticmethod
    def del_data_model_indicator(model_instance_id, result_table_id):
        return DatamanageHelper.del_data_model_indicator(
            {"model_instance_id": model_instance_id, "result_table_id": result_table_id}
        )

    @staticmethod
    def rollback_data_model_indicator(model_instance_id, result_table_id, rollback_id):
        DatamanageHelper.rollback_data_model_indicator(
            {
                "model_instance_id": model_instance_id,
                "result_table_id": result_table_id,
                "rollback_id": rollback_id,
            }
        )
        return
