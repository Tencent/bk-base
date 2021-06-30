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

from common.exceptions import ApiRequestError
from django.utils.translation import ugettext_lazy as _

from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers.forms import FlowProcessModelNodeForm
from dataflow.flow.handlers.nodes.base_node.base_processing_node import ProcessingNode
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.model.process_model_helper import ProcessModelHelper


class ProcessModelNode(ProcessingNode):
    default_storage_type = "hdfs"
    channel_storage_type = "kafka"

    node_type = NodeTypes.PROCESS_MODEL
    node_form = FlowProcessModelNodeForm
    config_attrs = [
        "bk_biz_id",
        "table_name",
        "output_name",
        "name",
        "from_result_table_ids",
        "model_release_id",
        "input_config",
        "output_config",
        "schedule_config",
        "serving_mode",
        "sample_feedback_config",
        "upgrade_config",
        "model_id",
        "model_extra_config",
        "scene_name",
    ]

    def add_after(self, username, from_node_ids, form_data):
        # 创建 processing
        instance_params = self.build_instance_params(form_data)
        response_data = ProcessModelHelper.create_processing(**instance_params)
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
        self.validate_node_config(form_data)
        instance_params = self.build_instance_params(form_data)
        instance_params.update({"processing_id": self.processing_id})
        response_data = ProcessModelHelper.update_processing(**instance_params)
        result_table_ids = [response_data["result_table_id"]]
        rts_heads_tails = {
            "result_table_ids": result_table_ids,
            "heads": result_table_ids,
            "tails": result_table_ids,
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(ProcessModelNode, self).remove_before(username)
        try:
            # 先获取配置，获取配置异常，说明已经被删除
            ProcessModelHelper.get_processing(self.processing_id)
        except ApiRequestError:
            logger.info("processing_id(%s)已经被删除，无需重复删除" % self.processing_id)
            return

        self.delete_dp_with_rollback(ProcessModelHelper.delete_processing)

    def build_instance_params(self, form_data):
        rt_params = {
            "input_config": form_data["input_config"],
            "output_config": form_data["output_config"],
            "schedule_config": form_data["schedule_config"],
            "upgrade_config": form_data["upgrade_config"],
            "serving_mode": form_data["serving_mode"],
            "project_id": self.project_id,
            "bk_biz_id": form_data["bk_biz_id"],
            "model_release_id": form_data["model_release_id"],
            "sample_feedback_config": form_data["sample_feedback_config"],
            "model_extra_config": form_data["model_extra_config"],
            "node_name": self.name,
        }
        return rt_params

    def validate_node_config(self, form_data):
        """
        serving_mode 不允许切换
        sensitivity 已启动后不允许修改
        @param form_data:
        @return:
        """
        node_config = self.get_config(False)
        current_serving_mode = node_config["serving_mode"]
        node_type_dispay = super(ProcessModelNode, self).get_node_type_display()

        if form_data["serving_mode"] != current_serving_mode:
            raise NodeError(_("不允许修改(%s)节点，当前运行模式为(%s)") % (node_type_dispay, current_serving_mode))

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
        return self.get_config(loading_latest=False)["serving_mode"]

    def max_window_size_by_day(self, form_data, specified_result_table_id=None):
        """
        获取当前节点的计算的数据窗口长度，用于设置当前及下游节点的 HDFS 过期时间，单位为天
        若指定上游特定的rt，则返回上游特定rt相关的窗口长度
            a.单位为小时，除以24 + 1(避免不整除)
            b.单位为周，取7
            c.单位为月，取31
            d.单位为日，取窗口长度
        @param form_data:
        @param specified_result_table_id:
        @return:
        """
        if (
            self.serving_mode == "offline"
            and "schedule_config" in form_data
            and "serving_scheduler_params" in form_data["schedule_config"]
        ):
            data_period = form_data["schedule_config"]["serving_scheduler_params"]["data_period"]
            data_period_unit = form_data["schedule_config"]["serving_scheduler_params"]["data_period_unit"]
            if data_period_unit == "hour":
                return int(data_period / 24) + 1
            elif data_period_unit == "week":
                return 7
            elif data_period_unit == "month":
                return 31
            else:
                return data_period
