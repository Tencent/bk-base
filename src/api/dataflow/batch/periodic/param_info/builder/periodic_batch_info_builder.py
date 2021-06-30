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

from dataflow.batch.periodic.param_info.periodic_batch_info_params import (
    InputTableConfigParam,
    OutputTableConfigParam,
    PeriodicBatchInfoParams,
)
from dataflow.batch.utils import time_util
from dataflow.batch.utils.time_util import BatchTimeTuple


class PeriodicBatchInfoBuilder(object):
    def __init__(self):
        self.periodic_batch_info_obj = None  # type: PeriodicBatchInfoParams

    def build_sql_from_flow_api(self, params):
        self.periodic_batch_info_obj = PeriodicBatchInfoParams()
        self.build_from_flow_api(params)
        return self.periodic_batch_info_obj

    def build_from_flow_api(self, params):
        single_output = params["outputs"][0]
        self.periodic_batch_info_obj.processing_id = "{}_{}".format(
            single_output["bk_biz_id"], single_output["table_name"]
        )
        self.periodic_batch_info_obj.description = self.periodic_batch_info_obj.processing_id

        self.periodic_batch_info_obj.batch_type = params["dedicated_config"]["batch_type"]

        if self.periodic_batch_info_obj.batch_type.lower() == "batch_sql_v2":
            self.periodic_batch_info_obj.processor_type = self.periodic_batch_info_obj.batch_type
            self.periodic_batch_info_obj.processor_logic = {"sql": params["dedicated_config"]["sql"]}

        dedicated_config = params["dedicated_config"]

        tmp_schedule_config = dedicated_config["schedule_config"]
        self.periodic_batch_info_obj.count_freq = tmp_schedule_config["count_freq"]
        self.periodic_batch_info_obj.schedule_period = tmp_schedule_config["schedule_period"]
        self.periodic_batch_info_obj.start_time = time_util.to_milliseconds_timestamp_in_hour(
            tmp_schedule_config["start_time"], "%Y-%m-%d %H:%M:%S"
        )

        if "recovery_config" in dedicated_config:
            self.periodic_batch_info_obj.recovery_enable = dedicated_config["recovery_config"]["recovery_enable"]
            self.periodic_batch_info_obj.recovery_times = int(dedicated_config["recovery_config"]["recovery_times"])
            self.periodic_batch_info_obj.recovery_interval = dedicated_config["recovery_config"]["recovery_interval"]
        else:
            self.periodic_batch_info_obj.recovery_enable = False
            self.periodic_batch_info_obj.recovery_times = 0
            self.periodic_batch_info_obj.recovery_interval = "1H"

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

        self.periodic_batch_info_obj.input_result_tables = []

        only_from_whole_data = True

        for window_item in params["window_info"]:
            # added count_freq and schedule_period in case window_type is scroll
            window_item["count_freq"] = self.periodic_batch_info_obj.count_freq
            window_item["schedule_period"] = self.periodic_batch_info_obj.schedule_period

            input_table = self.build_input_result_tables(window_item)

            if input_table.window_type != "whole":
                only_from_whole_data = False

            self.periodic_batch_info_obj.input_result_tables.append(input_table)

        self.periodic_batch_info_obj.output_result_tables = []

        output_table = self.build_output_result_tables(params)
        output_table.only_from_whole_data = only_from_whole_data
        self.periodic_batch_info_obj.output_result_tables.append(output_table)

        self.periodic_batch_info_obj.bk_username = get_request_username()
        self.periodic_batch_info_obj.project_id = params["project_id"]
        self.periodic_batch_info_obj.tags = params["tags"]  # todo: need verify

    def build_input_result_tables(self, input_info):
        input_table = InputTableConfigParam()
        input_table.result_table_id = input_info["result_table_id"]
        input_table.window_type = input_info["window_type"]
        if input_table.window_type == "whole":
            input_table.is_static = input_info["is_static"]  # todo: need verify
        else:
            input_table.is_static = False
            input_table.dependency_rule = input_info["dependency_rule"]
            window_offset_time_tuple = BatchTimeTuple()
            window_offset_time_tuple.set_time_with_unit(input_info["window_offset"], input_info["window_offset_unit"])
            input_table.window_offset = window_offset_time_tuple.to_jobnavi_string()
            if input_table.window_type == "scroll":
                window_size_time_tuple = BatchTimeTuple()
                window_size_time_tuple.set_time_with_unit(input_info["count_freq"], input_info["schedule_period"])

                input_table.window_size = window_size_time_tuple.to_jobnavi_string()
            else:
                window_size_time_tuple = BatchTimeTuple()
                window_size_time_tuple.set_time_with_unit(input_info["window_size"], input_info["window_size_unit"])

                input_table.window_size = window_size_time_tuple.to_jobnavi_string()

                if input_table.window_type == "accumulate":
                    window_start_offset_time_tuple = BatchTimeTuple()
                    window_start_offset_time_tuple.set_time_with_unit(
                        input_info["window_start_offset"],
                        input_info["window_start_offset_unit"],
                    )
                    input_table.window_start_offset = window_start_offset_time_tuple.to_jobnavi_string()

                    window_end_offset_time_tuple = BatchTimeTuple()
                    window_end_offset_time_tuple.set_time_with_unit(
                        input_info["window_end_offset"],
                        input_info["window_end_offset_unit"],
                    )
                    input_table.window_end_offset = window_end_offset_time_tuple.to_jobnavi_string()

                    input_table.accumulate_start_time = time_util.to_milliseconds_timestamp_in_hour(
                        input_info["accumulate_start_time"], "%Y-%m-%d %H:%M:%S"
                    )

        return input_table

    def build_output_result_tables(self, params):
        output_info = params["dedicated_config"]["output_config"]
        single_output = params["outputs"][0]

        output_table = OutputTableConfigParam()
        output_table.bk_biz_id = single_output["bk_biz_id"]
        output_table.table_name = single_output["table_name"]

        output_table.result_table_id = self.periodic_batch_info_obj.processing_id

        output_table.enable_customize_output = output_info["enable_customize_output"]

        if output_table.enable_customize_output:
            if output_info["output_baseline_type"].lower() == "schedule_time":
                output_table.output_baseline = "schedule_time"
                output_table.output_baseline_location = ""
            else:
                output_table.output_baseline = output_info["output_baseline"]
                output_table.output_baseline_location = output_info["output_baseline_location"]
            output_offset_time_tuple = BatchTimeTuple()
            output_offset_time_tuple.set_time_with_unit(output_info["output_offset"], output_info["output_offset_unit"])
            output_table.output_offset = output_offset_time_tuple.to_jobnavi_string()
        return output_table
