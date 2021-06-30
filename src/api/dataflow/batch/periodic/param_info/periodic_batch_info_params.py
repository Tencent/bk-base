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

from dataflow.batch import settings
from dataflow.batch.exceptions.comp_execptions import BatchIllegalArgumentError
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo


class InputTableConfigParam(object):
    def __init__(self):
        self.result_table_id = None  # type: str
        self.window_type = None  # type: str
        self.is_static = False  # type: bool
        self.dependency_rule = None  # type: str
        self.window_offset = None  # type: str
        self.window_size = None  # type: str
        self.window_start_offset = None  # type: str
        self.window_end_offset = None  # type: str
        self.accumulate_start_time = None  # type: int

    def from_db_json(self, json_obj):
        self.result_table_id = json_obj["result_table_id"]
        self.window_type = json_obj["window_type"]
        self.is_static = json_obj["is_static"]  # todo: need verify
        self.dependency_rule = json_obj["dependency_rule"]
        self.window_offset = json_obj["window_offset"]
        self.window_size = json_obj["window_size"]
        self.window_start_offset = json_obj["window_start_offset"]
        self.window_end_offset = json_obj["window_end_offset"]
        self.accumulate_start_time = json_obj["accumulate_start_time"]

    def to_db_json(self):
        result_json = {
            "result_table_id": self.result_table_id,
            "window_type": self.window_type,
            "is_static": self.is_static,
            "dependency_rule": self.dependency_rule,
            "window_offset": self.window_offset,
            "window_size": self.window_size,
            "window_start_offset": self.window_start_offset,
            "window_end_offset": self.window_end_offset,
            "accumulate_start_time": self.accumulate_start_time,
        }
        return result_json


class OutputTableConfigParam(object):
    def __init__(self):
        self.result_table_id = None  # type: str

        self.only_from_whole_data = False  # type: bool

        self.enable_customize_output = False  # type: bool
        self.output_baseline = None  # type: str
        self.output_baseline_location = None  # type: str
        self.output_offset = 0  # type: int
        self.output_offset_unit = "hour"  # type: str

        self.bk_biz_id = None  # type: int
        self.table_name = None  # type: str

    def from_db_json(self, json_obj):
        self.result_table_id = json_obj["result_table_id"]
        self.only_from_whole_data = json_obj["only_from_whole_data"]

        self.enable_customize_output = json_obj["enable_customize_output"]
        self.output_baseline = json_obj["output_baseline"]
        self.output_baseline_location = json_obj["output_baseline_location"]
        self.output_offset = json_obj["output_offset"]
        self.bk_biz_id = json_obj["bk_biz_id"]
        self.table_name = json_obj["table_name"]

    def to_db_json(self):
        result_json = {
            "bk_biz_id": self.bk_biz_id,
            "table_name": self.table_name,
            "result_table_id": self.result_table_id,
            "only_from_whole_data": self.only_from_whole_data,
            "enable_customize_output": self.enable_customize_output,
            "output_baseline": self.output_baseline,
            "output_baseline_location": self.output_baseline_location,
            "output_offset": self.output_offset,
        }
        return result_json


class PeriodicBatchInfoParams(object):
    def __init__(self):
        self.count_freq = None  # type: int
        self.schedule_period = None  # type: str
        self.start_time = None  # type: int
        self.batch_type = None  # type: str

        self.processor_logic = {}  # type: dict

        self.input_result_tables = []
        self.output_result_tables = []

        self.recovery_enable = False  # type: bool
        self.recovery_times = None  # type: int
        self.recovery_interval = None  # type: str

        self.is_self_dependency = None  # type: bool
        self.self_dependency_fields = None
        self.self_dependency_rule = None  # type: str

        self.bk_username = None  # type: str
        self.project_id = None  # type: str
        self.tags = None  # type: str
        self.processing_id = None  # type: str
        self.description = None  # type: str

        self.processor_type = ""
        self.component_type = "spark"

    def to_submit_args_json(self):
        result_json = {
            "batch_type": self.batch_type,
            "start_time": self.start_time,
            "input_result_tables": [],
            "output_result_tables": [],
            "recovery_enable": self.recovery_enable,
            "recovery_times": self.recovery_times,
            "recovery_interval": self.recovery_interval,
            "is_self_dependency": self.is_self_dependency,
            "self_dependency_fields": self.self_dependency_fields,
            "self_dependency_rule": self.self_dependency_rule,
            "project_id": self.project_id,
            "tags": self.tags,
            "processing_id": self.processing_id,
        }

        for input_result_table in self.input_result_tables:
            result_json["input_result_tables"].append(input_result_table.to_db_json())

        for output_result_table in self.output_result_tables:
            result_json["output_result_tables"].append(output_result_table.to_db_json())

        return result_json

    def from_processing_batch_info_db_obj(self, batch_info_db_obj):
        submit_args = json.loads(batch_info_db_obj.submit_args)
        self.processing_id = batch_info_db_obj.batch_id
        self.count_freq = batch_info_db_obj.count_freq
        self.schedule_period = batch_info_db_obj.schedule_period

        self.processor_logic = json.loads(batch_info_db_obj.processor_logic)
        self.processor_type = batch_info_db_obj.processor_type

        self.start_time = submit_args["start_time"]
        self.batch_type = submit_args["batch_type"]

        self.recovery_enable = submit_args["recovery_enable"]
        self.recovery_times = submit_args["recovery_times"]
        self.recovery_interval = submit_args["recovery_interval"]

        self.is_self_dependency = submit_args["is_self_dependency"]
        self.self_dependency_fields = submit_args["self_dependency_fields"]
        self.self_dependency_rule = submit_args["self_dependency_rule"]

        self.input_result_tables = []

        for input_table in submit_args["input_result_tables"]:
            input_table_obj = InputTableConfigParam()
            input_table_obj.from_db_json(input_table)
            self.input_result_tables.append(input_table_obj)

        self.output_result_tables = []

        for output_table in submit_args["output_result_tables"]:
            output_table_obj = OutputTableConfigParam()
            output_table_obj.from_db_json(output_table)
            self.output_result_tables.append(output_table_obj)

        self.bk_username = batch_info_db_obj.created_by
        self.project_id = submit_args["project_id"]
        self.tags = submit_args["tags"]  # todo: need verify

    def from_processing_batch_info_db(self, batch_id):
        batch_info_db_obj = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(
            batch_id
        )  # type: ProcessingBatchInfo
        self.from_processing_batch_info_db_obj(batch_info_db_obj)

    def save_to_processing_batch_info_db(self):
        self.__valid_db_text_length()
        ProcessingBatchInfoHandler.save_proc_batch_info_v2(
            processing_id=self.processing_id,
            batch_id=self.processing_id,
            processor_type=self.processor_type,
            processor_logic=json.dumps(self.processor_logic),
            schedule_period=self.schedule_period,
            count_freq=self.count_freq,
            delay=0,
            submit_args=json.dumps(self.to_submit_args_json()),
            component_type=self.component_type,
            created_by=self.bk_username,
            updated_by=self.bk_username,
            description=self.description,
        )

    def update_to_processing_batch_info_db(self):
        self.__valid_db_text_length()
        ProcessingBatchInfoHandler.update_proc_batch_info_v2(
            processing_id=self.processing_id,
            batch_id=self.processing_id,
            processor_type=self.processor_type,
            processor_logic=json.dumps(self.processor_logic),
            schedule_period=self.schedule_period,
            count_freq=self.count_freq,
            delay=0,
            submit_args=json.dumps(self.to_submit_args_json()),
            component_type=self.component_type,
            created_by=self.bk_username,
            updated_by=self.bk_username,
            description=self.description,
        )

    def __valid_db_text_length(self):
        if len(json.dumps(self.processor_logic)) > settings.SPARK_SQL_TEXT_MAX_LENGTH:
            raise BatchIllegalArgumentError("SQL exceeded max length, please check your sql")
