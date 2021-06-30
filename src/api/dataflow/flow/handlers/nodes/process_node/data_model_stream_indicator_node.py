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

from dataflow.flow.handlers import forms
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_rt_node import RTNode
from dataflow.flow.handlers.nodes.base_node.node_handler import call_func_with_rollback
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.stream.stream_helper import StreamHelper


class DataModelIndicatorRealTimeNode(RTNode):
    default_storage_type = "hdfs"
    node_type = NodeTypes.DATA_MODEL_STREAM_INDICATOR
    node_form = forms.FlowProcessingNodeStandardForm
    config_attrs = [
        "bk_biz_id",
        "processing_name",
        "name",
        "outputs",
        "from_nodes",
        "window_config",
        "dedicated_config",
    ]

    # 是否需要写channel由子节点决定
    def add_after(self, username, from_node_ids, form_data):
        # 创建模型实例指标
        model_indicator_params = self.build_create_model_indicator_params(form_data, from_node_ids)
        model_indicator_response = self.create_data_model_indicator(model_indicator_params)
        form_data["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
        form_data["window_config"] = model_indicator_response["scheduling_content"]

        # 2. 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params["processing_id"] = self.build_processing_id(
            form_data["bk_biz_id"], form_data["outputs"][0]["table_name"]
        )
        rt_params["component_type"] = "flink"
        result_table_id = "{}_{}".format(
            form_data["bk_biz_id"],
            form_data["outputs"][0]["table_name"],
        )

        create_processing_rollbacks = [
            {
                "func": self.delete_dp_with_rollback,
                "params": {"delete_func": StreamHelper.delete_processing},
            },
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": model_indicator_response["model_instance_id"],
                    "rollback_id": model_indicator_response["rollback_id"],
                    "result_table_id": result_table_id,
                },
            },
        ]
        response_data = call_func_with_rollback(StreamHelper.create_processing, rt_params, create_processing_rollbacks)

        # get current node config
        current_node_config = self.get_config(False)
        current_node_config["dedicated_config"]["model_instance_id"] = model_indicator_response["model_instance_id"]
        current_node_config["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
        current_node_config["dedicated_config"]["result_table_id"] = result_table_id
        self.node_info.node_config = json.dumps(current_node_config)
        self.node_info.save()

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        # 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        # response_data = StreamHelper.update_processing(**rt_params)
        result_table_id = ("{}_{}".format(form_data["bk_biz_id"], form_data["outputs"][0]["table_name"]),)

        update_processing_rollbacks = [
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": form_data["dedicated_config"]["model_instance_id"],
                    "rollback_id": form_data["dedicated_config"]["rollback_id"],
                    "result_table_id": result_table_id,
                },
            }
        ]
        response_data = call_func_with_rollback(StreamHelper.update_processing, rt_params, update_processing_rollbacks)

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    # 当前计算节点remove操作无需调用remove_after
    def remove_before(self, username):
        """
        先删processing再删存储，防止processing删除失败存储却删了
        @param username:
        @return:
        """
        super(DataModelIndicatorRealTimeNode, self).remove_before(username)
        # 从数据库读出 node_config
        current_config = self.get_config(True)
        result_table_id = "{}_{}".format(
            current_config["bk_biz_id"],
            current_config["outputs"][0]["table_name"],
        )
        del_data_model_response = self.del_data_model_indicator(
            current_config.get("dedicated_config").get("model_instance_id"),
            result_table_id,
        )
        delete_processing_rollbacks = [
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": del_data_model_response["model_instance_id"],
                    "result_table_id": del_data_model_response["result_table_id"],
                    "rollback_id": del_data_model_response["rollback_id"],
                },
            }
        ]
        call_func_with_rollback(
            self.delete_dp_with_rollback,
            StreamHelper.delete_processing,
            delete_processing_rollbacks,
        )

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(DataModelIndicatorRealTimeNode, self).build_before(from_node_ids, form_data, is_create)
        if not is_create:
            # 如果是更新操作，则需取到原有的 node_config 中的 model_instance_id 参数
            # 然后填充新的更新参数并更新，返回新的 data_model_sql 和 rollback_id
            current_node_config = self.get_config(False)
            if (
                "dedicated_config" in current_node_config
                and "model_instance_id" in current_node_config["dedicated_config"]
                and current_node_config["dedicated_config"]["model_instance_id"]
            ):
                # 更新数据模型指标
                model_indicator_params = self.build_update_model_indicator_params(form_data, from_node_ids)
                model_indicator_params["model_instance_id"] = current_node_config["dedicated_config"][
                    "model_instance_id"
                ]
                model_indicator_response = self.update_data_model_indicator(model_indicator_params)
                form_data["dedicated_config"]["model_instance_id"] = model_indicator_response["model_instance_id"]
                form_data["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
                form_data["dedicated_config"]["rollback_id"] = model_indicator_response["rollback_id"]

        return form_data

    def build_rt_params(self, rt_dict, from_node_ids, username):
        """
        生成 RT 参数
        """
        (
            not_static_rt_ids,
            static_rt_ids,
            source_rt_ids,
        ) = NodeUtils.build_from_nodes_list(from_node_ids)

        outputs = [
            {
                "bk_biz_id": rt_dict["bk_biz_id"],
                "table_name": rt_dict["outputs"][0]["table_name"],
            }
        ]
        window_lateness = rt_dict["window_config"]["window_lateness"] or {
            "allowed_lateness": False,
            "lateness_time": 0,
            "lateness_count_freq": 0,
        }
        # 尽量透传参数
        send_dict = rt_dict["window_config"].copy()
        # 拼接
        send_dict["description"] = rt_dict["outputs"][0]["output_name"]
        send_dict.update(window_lateness)
        # 替换sql
        sql = rt_dict["dedicated_config"]["data_model_sql"]
        rt_params = {
            "project_id": self.flow.project_id,
            "sql": sql,
            "dict": send_dict,
            "input_result_tables": not_static_rt_ids,
            "static_data": static_rt_ids,
            "source_data": source_rt_ids,
            "outputs": outputs,
            "tags": self.geog_area_codes,
        }
        return rt_params

    def build_create_model_indicator_params(self, rt_dict, from_node_ids):
        """
        生成模型实例参数
        """
        rt_params = rt_dict["dedicated_config"]
        rt_params["scheduling_content"] = rt_dict["window_config"]
        # 加上额外的节点信息
        rt_params["bk_biz_id"] = rt_dict["bk_biz_id"]
        rt_params["result_table_id"] = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        rt_params["parent_result_table_id"] = self.from_result_table_ids[0]
        rt_params["flow_node_id"] = self.node_id
        return rt_params

    def build_update_model_indicator_params(self, rt_dict, from_node_ids):
        """
        生成模型实例参数
        """
        rt_params = rt_dict["dedicated_config"]
        rt_params["scheduling_content"] = rt_dict["window_config"]
        rt_params["result_table_id"] = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        rt_params["parent_result_table_id"] = self.from_result_table_ids[0]
        return rt_params

    @property
    def processing_type(self):
        return "stream"

    @property
    def component_type(self):
        """
        节点已创建方可调用
        @return:
        """
        o_job = self.get_job()
        if o_job:
            # 若job_id存在，则获取旧任务的component_type
            component_type = StreamHelper.get_job_component_type(o_job.job_id)
        else:
            component_type = StreamHelper.get_processing_component_type(self.processing_id)
        return component_type

    @property
    def is_belong_to_topology_job(self):
        return True
