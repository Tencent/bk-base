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

import importlib

import tensorflow as tf
from bkbase.dataflow.one_model import STRATEGY
from bkbase.dataflow.one_model.topo.deeplearning_node import Builder, Node
from bkbase.dataflow.one_model.utils import deeplearning_logger, node_utils
from bkbase.dataflow.one_model.utils.deeplearning_constant import TableType
from bkbase.dataflow.one_model.utils.input_window_analyzer import InputWindowAnalyzer
from bkbase.dataflow.one_model.utils.periodic_time_helper import PeriodicTimeHelper
from bkdata_datalake import tables
from tensorflow import keras


class SourceNode(Node):
    input = None
    window_config = None

    def __init__(self, builder):
        super().__init__(builder)
        self.input = builder.input
        self.window_config = builder.window_config

    def map(self, info):
        super().map(info)
        if "window" in info:
            self.window_config = node_utils.map_window_config(info["window"])

    def create_source(self):
        pass


class ModelSourceNode(SourceNode):
    def __init__(self, builder):
        super().__init__(builder)
        self.table_type = builder.table_type

    def create_source(self):
        with STRATEGY.scope():
            model = keras.models.load_model(self.input["path"])
        return model


class ModelIcebergQuerySetSourceNode(SourceNode):
    def __init__(self, builder):
        super().__init__(builder)
        self.iceberg_conf = builder.iceberg_conf
        self.user_main_module = None
        self.feature_shape = None
        self.label_shape = None
        table_info = self.input["iceberg_table_name"].split(".")
        self.table = tables.load_table(table_info[0], table_info[1], self.iceberg_conf)

    def set_user_main_module(self, user_main_module):
        self.user_main_module = user_main_module

    def set_feature_shape(self, feature_shape):
        self.feature_shape = feature_shape

    def create_source(self):
        def rdd_generator():
            deeplearning_logger.info("in rdd generator...")
            # 动态载入用户代码
            module = importlib.import_module(self.user_main_module)
            for record in tables.scan_table(self.table):
                yield module.data_refractor(record)

        shape_list = [int(shape_item) for shape_item in self.feature_shape.split(",")]
        return tf.data.Dataset.from_generator(
            rdd_generator, (tf.float32, tf.float32), (tf.TensorShape(shape_list), tf.TensorShape([1]))
        )


class ModelIcebergResultSetSourceNode(SourceNode):
    def __init__(self, builder):
        super().__init__(builder)
        self.iceberg_conf = builder.iceberg_conf
        self.user_main_module = builder.user_main_module
        self.feature_shape = builder.feature_shape
        self.label_shape = builder.label_shape
        table_info = self.input["iceberg_table_name"].split(".")
        self.table = tables.load_table(table_info[0], table_info[1], self.iceberg_conf)

    def set_user_main_module(self, user_main_module):
        self.user_main_module = user_main_module

    def set_feature_shape(self, feature_shape):
        self.feature_shape = feature_shape

    def create_source(self):
        def rdd_generator():
            # 动态载入用户代码
            module = importlib.import_module(self.user_main_module)
            for record in tables.scan_table(self.table, self.input["filter_expression"]):
                yield module.data_refractor(record)

        if self.label_shape:
            return tf.data.Dataset.from_generator(rdd_generator, (tf.float32, tf.float32), None)
        else:
            return tf.data.Dataset.from_generator(rdd_generator, (tf.float32), None)


class SourceNodeBuilder(Builder):
    input = None
    window_config = None

    def __init__(self, info):
        super().__init__(info)
        if "window" in info:
            self.window_config = node_utils.map_window_config(info["window"])
        self.init_builder(info)

    def init_builder(self, info):
        pass

    def build(self):
        pass


class ModelSourceNodeBuilder(SourceNodeBuilder):
    table_type = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        input_info = info["input"]
        root = input_info["path"]
        self.table_type = input_info.get("table_type", TableType.OTHER.value)
        self.input = {"path": root, "format": input_info["format"]}
        deeplearning_logger.info("read source path:{}".format(root))

    def build(self):
        return ModelSourceNode(self)


class ModelIceBergQuerySetSourceNodeBuilder(SourceNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        self.iceberg_conf = info["iceberg_conf"]
        self.input = {"iceberg_table_name": info["iceberg_table_name"]}

    def build(self):
        return ModelIcebergQuerySetSourceNode(self)


class ModelIcebergDebugSourceNodeBuilder(SourceNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        schedule_time_in_hour = PeriodicTimeHelper.round_schedule_timestamp_to_hour(info["schedule_time"])
        iceberg_table_name = info["iceberg_table_name"]
        latest_hour_range = 24
        start_time_mills = schedule_time_in_hour - latest_hour_range * 3600 * 1000
        end_time_mills = schedule_time_in_hour + 3600 * 1000
        self.iceberg_conf = info["iceberg_conf"]
        self.input = {
            "iceberg_table_name": iceberg_table_name,
            "time_range_list": [{"start_time": start_time_mills, "end_time": end_time_mills}],
            "filter_expression": PeriodicTimeHelper.get_greater_equal_less_expression(start_time_mills, end_time_mills),
            "is_optimized_count_valid": True,
        }

    def build(self):
        return ModelIcebergResultSetSourceNode(self)


class ModelIcebergResultSetSourceNodeBuilder(SourceNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        iceberg_table_name = info["iceberg_table_name"]
        helper = PeriodicTimeHelper(self.node_id, self.window_config, parent_node_url=info["parent_node_url"])
        start_time_mills, end_time_mills = helper.get_start_and_end_time()
        self.input = {
            "iceberg_table_name": iceberg_table_name,
            "time_range_list": [{"start_time": start_time_mills, "end_time": end_time_mills}],
            "filter_expression": PeriodicTimeHelper.get_greater_equal_less_expression(start_time_mills, end_time_mills),
            "is_optimized_count_valid": True,
        }
        self.iceberg_conf = info["iceberg_conf"]

    def build(self):
        return ModelIcebergResultSetSourceNode(self)


class ModelIcebergResultSetSourceNodeBuilderV2(SourceNodeBuilder):
    iceberg_conf = None
    window_analyzer = None
    user_main_module = None
    feature_shape = None
    label_shape = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        self.fields = self.parse_v2_fields(self.fields)
        self.window_analyzer = InputWindowAnalyzer(info)
        iceberg_table_name = info["storage_conf"]["physical_table_name"]
        self.input = {
            "iceberg_table_name": iceberg_table_name,
            "time_range_list": [
                {
                    "start_time": self.window_analyzer.start_time.get_time_in_mills(),
                    "end_time": self.window_analyzer.end_time.get_time_in_mills(),
                }
            ],
            "filter_expression": PeriodicTimeHelper.get_greater_equal_less_expression(
                self.window_analyzer.start_time.get_time_in_mills(), self.window_analyzer.end_time.get_time_in_mills()
            ),
            "is_optimized_count_valid": True,
            "storage_conf": info["storage_conf"],
        }
        self.iceberg_conf = info["storage_conf"]["storekit_hdfs_conf"]
        self.feature_shape = info["feature_shape"]
        self.label_shape = info["label_shape"]
        if self.label_shape and self.feature_shape:
            pass
        elif not self.label_shape and not self.feature_shape:
            self.feature_shape = len(self.fields)
            self.label_shape = 0
        elif self.label_shape and not self.feature_shape:
            self.feature_shape = len(self.fields) - self.label_shape
        else:
            self.label_shape = 0

    def parse_v2_fields(self, fields):
        new_node_fields = []
        for field in fields:
            if field["field"] != "timestamp" or field["field"] != "offset":
                new_field = {
                    "field": field["field"],
                    "type": field["type"],
                    "origin": field["origin"],
                    "description": field["description"],
                }
                new_node_fields.append(new_field)
        return new_node_fields

    def build(self):
        return ModelIcebergResultSetSourceNode(self)
