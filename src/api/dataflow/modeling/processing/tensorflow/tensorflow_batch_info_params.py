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

from dataflow.batch.periodic.param_info.periodic_batch_info_params import (
    InputTableConfigParam,
    OutputTableConfigParam,
    PeriodicBatchInfoParams,
)


class TensorFlowBatchInfoParams(PeriodicBatchInfoParams):
    def __init__(self):
        super(TensorFlowBatchInfoParams, self).__init__()
        self.processor_type = "code"
        self.component_type = "tensorflow"
        self.input_models = []
        self.output_models = []

    def from_processing_batch_info_db_obj(self, batch_info_db_obj):
        # 获取父类信息
        super(TensorFlowBatchInfoParams, self).from_processing_batch_info_db_obj(batch_info_db_obj)
        # 补充模型信息
        submit_args = json.loads(batch_info_db_obj.submit_args)
        for input_model in submit_args["input_models"]:
            input_model_obj = InputModelConfigParams()
            input_model_obj.from_db_json(input_model)
            self.input_models.append(input_model_obj)

        for output_model in submit_args["output_models"]:
            output_model_obj = OutputModelConfigParam()
            output_model_obj.from_db_json(output_model)
            self.output_models.append(output_model_obj)

        # 补充输入表的feature与label信息
        new_input_result_tables = []
        input_tables_map = {}
        for result_table in submit_args["input_result_tables"]:
            input_tables_map[result_table["result_table_id"]] = result_table

        for input_table in self.input_result_tables:
            extra_info = {
                "feature_shape": input_tables_map[input_table.result_table_id]["feature_shape"],
                "label_shape": input_tables_map[input_table.result_table_id]["label_shape"],
            }
            new_input_table = TFInputTableConfigParams(input_table, extra_info=extra_info)
            new_input_result_tables.append(new_input_table)
        self.input_result_tables = new_input_result_tables

    def to_submit_args_json(self):
        # 获取父类的相关信息
        result_json = super(TensorFlowBatchInfoParams, self).to_submit_args_json()
        # 追加模型相关信息
        result_json["input_models"] = []
        result_json["output_models"] = []
        for input_model in self.input_models:
            result_json["input_models"].append(input_model.to_db_json())

        for output_model in self.output_models:
            result_json["output_models"].append(output_model.to_db_json())

        return result_json


class InputModelConfigParams(object):
    def __init__(self):
        self.model_name = None

    def to_db_json(self):
        result_json = {"model_name": self.model_name}
        return result_json

    def from_db_json(self, json_obj):
        self.model_name = json_obj["model_name"]


class TFInputTableConfigParams(object):
    # TensorFlow内的表信息，与普通的相比增加了feature与label的信息
    def __init__(self, periodic_table, extra_info={}):
        # 使用普通的周期表信息（即离线的输入表信息）来进行初始化，然后增加新的属性
        self.periodic_table = periodic_table
        self.result_table_id = periodic_table.result_table_id
        self.window_type = periodic_table.window_type
        self.is_static = periodic_table.is_static
        self.dependency_rule = periodic_table.dependency_rule
        self.window_offset = periodic_table.window_offset
        self.window_size = periodic_table.window_size
        self.window_start_offset = periodic_table.window_start_offset
        self.window_end_offset = periodic_table.window_end_offset
        self.accumulate_start_time = periodic_table.accumulate_start_time
        self.feature_shape = extra_info["feature_shape"] if "feature_shape" in extra_info else 0
        self.label_shape = extra_info["label_shape"] if "label_shape" in extra_info else 0

    def to_db_json(self):
        result_json = self.periodic_table.to_db_json()
        result_json["feature_shape"] = self.feature_shape
        result_json["label_shape"] = self.label_shape
        return result_json

    def from_db_json(self, json_obj):
        periodic_table = InputTableConfigParam()
        periodic_table.from_db_json(json_obj)
        self.__init__(periodic_table, json_obj)


class OutputModelConfigParam(object):
    def __init__(self):
        self.model_name = None
        self.model_alias = None

    def to_db_json(self):
        result_json = {"model_name": self.model_name, "model_alias": self.model_alias}
        return result_json

    def from_db_json(self, json_obj):
        self.model_name = json_obj["model_name"]
        self.model_alias = json_obj["model_alias"]


class TensorflowOutputTableConfigParam(OutputTableConfigParam):
    def __init__(self):
        super(TensorflowOutputTableConfigParam, self).__init__()
        self.fields = []
        self.description = None
        self.storages = {}
        self.need_create_storage = True

    def from_db_json(self, json_obj):
        super(TensorflowOutputTableConfigParam, self).from_db_json(json_obj)
        self.fields = json_obj["fields"]
        self.description = json_obj["description"]
        self.storages = json_obj["storages"]

    def to_db_json(self):
        result_json = super(TensorflowOutputTableConfigParam, self).to_db_json()
        result_json["fields"] = self.fields
        result_json["storages"] = self.storages
        return result_json
