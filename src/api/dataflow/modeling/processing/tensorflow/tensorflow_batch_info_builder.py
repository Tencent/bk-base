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

from dataflow.batch.periodic.param_info.builder.periodic_batch_info_builder import PeriodicBatchInfoBuilder
from dataflow.batch.utils import time_util
from dataflow.batch.utils.time_util import BatchTimeTuple
from dataflow.modeling.processing.tensorflow.tensorflow_batch_info_params import (
    InputModelConfigParams,
    OutputModelConfigParam,
    TensorFlowBatchInfoParams,
    TensorflowOutputTableConfigParam,
    TFInputTableConfigParams,
)


class TensorFlowBatchInfoBuilder(PeriodicBatchInfoBuilder):
    def __init__(self):
        super(TensorFlowBatchInfoBuilder, self).__init__()

    def build_sql_from_flow_api(self, params):
        self.periodic_batch_info_obj = TensorFlowBatchInfoParams()
        self.__build_from_flow_api(params)
        return self.periodic_batch_info_obj

    def __build_from_flow_api(self, params):
        bk_biz_id = params["bk_biz_id"]
        processing_name = params["name"]
        self.periodic_batch_info_obj.processing_id = "{}_{}".format(bk_biz_id, processing_name)
        self.periodic_batch_info_obj.description = self.periodic_batch_info_obj.processing_id
        dedicated_config = params["dedicated_config"]
        processor_logic = params["processor_logic"]
        self.periodic_batch_info_obj.batch_type = dedicated_config["batch_type"]
        self.periodic_batch_info_obj.processor_type = self.periodic_batch_info_obj.batch_type
        user_main_class = processor_logic["user_main_class"]
        if not user_main_class.endswith(".py"):
            user_main_class = "{}.py".format(user_main_class)
        self.periodic_batch_info_obj.processor_logic = {
            "programming_language": "Python",
            "user_args": processor_logic["user_args"],
            "script": user_main_class,
            "user_package": processor_logic["user_package"]["path"],
        }
        self.__build_schedule_info(dedicated_config)
        self.__build_recovery_config(dedicated_config)
        self.__build_self_dependency_config(dedicated_config)
        only_from_whole_data = self.__build_tf_input_result_tables(params)
        self.__build_input_models(params)
        self.__build_tf_output_result_tables(params, only_from_whole_data)
        self.__build_output_models(params)
        self.periodic_batch_info_obj.bk_username = get_request_username()
        self.periodic_batch_info_obj.project_id = params["project_id"]
        self.periodic_batch_info_obj.tags = params["tags"]  # todo: need verify

    def __build_schedule_info(self, dedicated_config):
        schedule_config = dedicated_config["schedule_config"]
        self.periodic_batch_info_obj.count_freq = schedule_config["count_freq"]
        self.periodic_batch_info_obj.schedule_period = schedule_config["schedule_period"]
        self.periodic_batch_info_obj.start_time = time_util.to_milliseconds_timestamp_in_hour(
            schedule_config["start_time"], "%Y-%m-%d %H:%M:%S"
        )

    def __build_recovery_config(self, dedicated_config):
        if "recovery_config" in dedicated_config:
            self.periodic_batch_info_obj.recovery_enable = dedicated_config["recovery_config"]["recovery_enable"]
            self.periodic_batch_info_obj.recovery_times = int(dedicated_config["recovery_config"]["recovery_times"])
            self.periodic_batch_info_obj.recovery_interval = dedicated_config["recovery_config"]["recovery_interval"]
        else:
            self.periodic_batch_info_obj.recovery_enable = False
            self.periodic_batch_info_obj.recovery_times = 0
            self.periodic_batch_info_obj.recovery_interval = "1H"

    def __build_self_dependency_config(self, dedicated_config):
        if "self_dependence" in dedicated_config:
            tmp_self_dependency_config = dedicated_config["self_dependence"]
            self.periodic_batch_info_obj.is_self_dependency = tmp_self_dependency_config["self_dependency"]
            if self.periodic_batch_info_obj.is_self_dependency:
                self.periodic_batch_info_obj.self_dependency_fields = tmp_self_dependency_config[
                    "self_dependency_config"
                ]["fields"]
                self.periodic_batch_info_obj.self_dependency_rule = tmp_self_dependency_config[
                    "self_dependency_config"
                ]["dependency_rule"]
        else:
            self.periodic_batch_info_obj.is_self_dependency = False
            self.periodic_batch_info_obj.self_dependency_fields = None
            self.periodic_batch_info_obj.self_dependency_rule = None

    def __build_tf_input_result_tables(self, params):
        self.periodic_batch_info_obj.input_result_tables = []
        input_table_config = params["dedicated_config"]["input_config"]["tables"]
        input_table_map = {table_item["result_table_id"]: table_item for table_item in input_table_config}
        only_from_whole_data = True
        for window_item in params["window_info"]:
            # added count_freq and schedule_period in case window_type is scroll
            window_item["count_freq"] = self.periodic_batch_info_obj.count_freq
            window_item["schedule_period"] = self.periodic_batch_info_obj.schedule_period

            input_table = self.build_input_result_tables(window_item)

            if input_table.window_type != "whole":
                only_from_whole_data = False
            input_table_item = input_table_map[window_item["result_table_id"]]
            tf_input_table = TFInputTableConfigParams(
                input_table,
                extra_info={
                    "feature_shape": input_table_item["feature_shape"] if "feature_shape" in input_table_item else 0,
                    "label_shape": input_table_item["label_shape"] if "label_shape" in input_table_item else 0,
                },
            )
            self.periodic_batch_info_obj.input_result_tables.append(tf_input_table)
        return only_from_whole_data

    def __build_input_models(self, params):
        self.periodic_batch_info_obj.input_models = []
        input_model_list = params["dedicated_config"]["input_config"]["models"]
        for model_item in input_model_list:
            input_model = InputModelConfigParams()
            input_model.model_name = model_item["name"]
            self.periodic_batch_info_obj.input_models.append(input_model)

    def __build_tf_output_result_tables(self, params, only_from_whole_data):
        self.periodic_batch_info_obj.output_result_tables = []
        output_info = params["dedicated_config"]["output_config"]
        output_table_list = output_info["tables"]
        for table_item in output_table_list:
            output_table = TensorflowOutputTableConfigParam()
            output_table.bk_biz_id = table_item["bk_biz_id"]
            output_table.table_name = table_item["table_name"]
            output_table.description = table_item["table_alias"]
            output_table.result_table_id = "{}_{}".format(table_item["bk_biz_id"], table_item["table_name"])
            output_table.enable_customize_output = table_item["enable_customize_output"]
            if output_table.enable_customize_output:
                if table_item["output_baseline_type"].lower() == "schedule_time":
                    output_table.output_baseline = "schedule_time"
                    output_table.output_baseline_location = ""
                else:
                    output_table.output_baseline = table_item["output_baseline"]
                    output_table.output_baseline_location = table_item["output_baseline_location"]
                output_offset_time_tuple = BatchTimeTuple()
                output_offset_time_tuple.set_time_with_unit(
                    table_item["output_offset"], table_item["output_offset_unit"]
                )
                output_table.output_offset = output_offset_time_tuple.to_jobnavi_string()
            output_table.only_from_whole_data = only_from_whole_data
            output_table.fields = table_item["fields"]
            if "need_create_storage" in table_item:
                output_table.need_create_storage = table_item["need_create_storage"]
            self.periodic_batch_info_obj.output_result_tables.append(output_table)

    def __build_output_models(self, params):
        self.periodic_batch_info_obj.output_models = []
        output_info = params["dedicated_config"]["output_config"]
        output_model_list = output_info["models"]
        for model_item in output_model_list:
            output_model = OutputModelConfigParam()
            output_model.model_name = model_item["name"]
            output_model.model_alias = model_item["alias"]
            output_model.bk_biz_id = model_item["bk_biz_id"]
            self.periodic_batch_info_obj.output_models.append(output_model)
