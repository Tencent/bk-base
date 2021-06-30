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

from dataflow.flow.api_models import Model
from dataflow.flow.handlers.forms import FlowModelNodeForm
from dataflow.flow.handlers.nodes.base_node.base_processing_node import ProcessingNode
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.model.model_helper import ModelHelper


class ModelNode(ProcessingNode):
    default_storage_type = "hdfs"
    channel_storage_type = "kafka"

    node_type = NodeTypes.MODEL
    node_form = FlowModelNodeForm
    config_attrs = [
        "bk_biz_id",
        "table_name",
        "output_name",
        "name",
        "from_result_table_ids",
        "model_params",
        "model_version_id",
        "model_id",
        "serving_scheduler_params",
        "training_scheduler_params",
        "training_when_serving",
        "auto_upgrade",
        "training_when_serving",
        "training_from_instance",
    ]

    def add_after(self, username, from_node_ids, form_data):
        # 创建 processing
        instance_params = self.build_instance_params(username, form_data)
        response_data = ModelHelper.create_processing(**instance_params)
        # 设置当前节点的 processing_id
        self.processing_id = response_data["processing_id"]
        result_table_ids = [response_data["result_table_id"]]
        rts_heads_tails = {
            "result_table_ids": result_table_ids,
            "heads": result_table_ids,
            "tails": result_table_ids,
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        instance_params = self.build_instance_params(username, form_data)
        instance_params.update({"processing_id": self.processing_id})
        response_data = ModelHelper.update_processing(**instance_params)
        result_table_ids = [response_data["result_table_id"]]

        rts_heads_tails = {
            "result_table_ids": result_table_ids,
            "heads": result_table_ids,
            "tails": result_table_ids,
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(ModelNode, self).remove_before(username)
        self.delete_dp_with_rollback(ModelHelper.delete_processing)

    def build_instance_params(self, username, config):
        o_algo_model = Model(model_id=config["model_id"], model_version_id=config["model_version_id"])

        default_kwargs = {
            "project_id": self.project_id,
            "bk_biz_id": config["bk_biz_id"],
            "table_name": config["table_name"],
        }
        _algo_model_params = o_algo_model.render_config_tempalte(
            o_algo_model.model_config_template, config["model_params"], default_kwargs
        )
        serving_mode = config["serving_scheduler_params"]["serving_mode"]
        instance_params = {
            "submitted_by": username,
            "project_id": self.project_id,
            "bk_biz_id": config["bk_biz_id"],
            "model_version_id": config["model_version_id"],
            "model_id": config["model_id"],
            "model_params": _algo_model_params,
            "auto_upgrade": config["auto_upgrade"],
            "training_when_serving": config["training_when_serving"],
            "training_from_instance": config["training_from_instance"],
            "serving_scheduler_params": self.build_scheduler_params(config["serving_scheduler_params"])
            if serving_mode != "realtime"
            else {},
            "serving_mode": serving_mode,
            "training_scheduler_params": {},
        }
        if config["training_scheduler_params"]:
            instance_params["training_scheduler_params"] = self.build_scheduler_params(
                config["training_scheduler_params"]
            )

        return instance_params

    @staticmethod
    def build_scheduler_params(scheduler_params):
        """
        组装调度参数
        @note: 当 scheduler_params 为 None 时，返回 {}
        @param scheduler_params: dict，举例，
            {
                'value': 1,
                'period': 'd',
                'first_run_time': '2017-02-20 04:30:00'

            }
        @return: dict
            {
                'period': '1d',
                'first_run_time': '2017-02-20 04:30:00'
            }
        """
        if scheduler_params is None:
            return {}

        return {
            "period": "{}{}".format(scheduler_params["value"], scheduler_params["period"])
            if scheduler_params["value"] and scheduler_params["period"]
            else None,
            "first_run_time": scheduler_params["first_run_time"],
        }

    @staticmethod
    def get_models(project_ids):
        response_data = ModelHelper.get_models(
            [
                "model_id",
                "model_version_id",
                "model_name",
                "description",
                "version_description",
            ],
            project_ids,
            True,
            True,
            True,
            "complete",
        )
        return response_data

    @property
    def processing_type(self):
        """
        注意，模型节点可能是实时模型节点，也可能是离线模型节点
        不允许用户修改修改 processing_type
        @return:
        """
        processing_type = DataProcessingHelper.get_data_processing_type(self.processing_id)
        return processing_type

    @property
    def serving_mode(self):
        return self.get_config(loading_latest=False)["serving_scheduler_params"]["serving_mode"]
