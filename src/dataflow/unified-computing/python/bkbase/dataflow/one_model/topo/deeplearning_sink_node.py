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
import os
import uuid
from datetime import datetime

from bkbase.dataflow.one_model.api.deeplearning_api import delete_path
from bkbase.dataflow.one_model.topo.deeplearning_node import Builder, Node
from bkbase.dataflow.one_model.utils import deeplearning_logger
from bkbase.dataflow.one_model.utils.deeplearning_constant import (
    IcebergReservedField,
    TableType,
)
from bkbase.dataflow.one_model.utils.input_window_analyzer import (
    BatchTimeDelta,
    BatchTimeStamp,
)
from bkbase.dataflow.one_model.utils.periodic_time_helper import PeriodicTimeHelper
from bkdata_datalake import tables
from jpype.types import JDouble, JInt, JLong, JString


class SinkNode(Node):
    output = None
    dt_event_timestamp = 0

    def __init__(self, builder):
        super().__init__(builder)
        self.output = builder.output
        self.dt_event_timestamp = builder.dt_event_timestamp


class ModelSinkNode(SinkNode):
    table_type = None

    def __init__(self, builder):
        super().__init__(builder)
        self.table_type = builder.table_type

    def create_sink(self, model):
        tf_config = json.loads(os.environ["TF_CONFIG"])
        index = tf_config["task"]["index"]
        path = self.output["path"]
        if path.endswith("/"):
            path = path[0 : len(path) - 1]
        if index > 0:
            path = "{}_{}".format(path, uuid.uuid4().hex[0:8])
        # 这里一定要注意，在各个worker上一定要同时进行save操作，因为分布式模型save的过程中会有
        # 不同worker之间的聚合交互，如果有一个worker上不进行，那就会block整个save的操作
        # 但同时，也只以有chief结点，即index=0的节点才能真正落地到指定的目录，其它的需要保存到临时目录
        # 落地完成之后进行清理
        deeplearning_logger.info("begin to save:{}".format(path))
        model.save(path)
        deeplearning_logger.info("save successfully")
        if index > 0:
            deeplearning_logger.info("delete extra paths...")
            self.delete_extra_path(path)

    def delete_extra_path(self, path):
        hdfs_cluster = self.output["cluster_group"]
        user = self.output["hdfs_user"]
        component_url = self.output["component_url"]
        # 抽取出路径
        index_array = [i for i, ltr in enumerate(path) if ltr == "/"]
        path = path[index_array[2] :]
        if path.startswith("//"):
            path = path[1:]
        deeplearning_logger.info("component url:" + component_url)
        deeplearning_logger.info("hdfs cluster:" + hdfs_cluster)
        deeplearning_logger.info("delete path:" + path)
        delete_path(component_url, hdfs_cluster, [path], True, user)
        # logger.info(clean_data)


class ModelIcebergQuerySetSinkNode(SinkNode):
    def __init__(self, builder):
        super().__init__(builder)
        self.iceberg_conf = builder.iceberg_conf

    def create_sink(self, dataset):
        iceberg_table_name = self.output["iceberg_table_name"]
        table_info = iceberg_table_name.split(".")
        table = tables.load_table(table_info[0], table_info[1], self.iceberg_conf)
        iterator = dataset.as_numpy_iterator()
        column_names = [field_item["field_name"] for field_item in self.fields]
        for item in iterator:
            tables.append_table(table, column_names, [item], {})


class ModelIcebergResultSetSinkNode(SinkNode):
    def __init__(self, builder):
        super().__init__(builder)
        self.iceberg_conf = builder.iceberg_conf

    def create_sink(self, dataset):
        tf_config = json.loads(os.environ["TF_CONFIG"])
        index = tf_config["task"]["index"]
        if index > 0:
            return
        # 仅index=0的才进行落地
        sink_dt_event_time = self.output["dt_event_timestamp"]
        # 生成预留字段
        data_time = datetime.fromtimestamp(sink_dt_event_time / 1000)
        the_date = data_time.strftime("%Y%m%d")
        dt_event_time = data_time.strftime("%Y-%m-%d %H:00:00")
        local_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        iceberg_table_name = self.output["iceberg_table_name"]
        table_info = iceberg_table_name.split(".")
        table = tables.load_table(table_info[0], table_info[1], self.iceberg_conf)
        iterator = dataset.as_numpy_iterator()

        column_names = []
        for field_item in self.fields:
            if field_item["field"] not in ["_startTime_", "_endTime_"]:
                column_names.append(field_item["field"].lower())

        column_names.append(IcebergReservedField.start_time.value.field_name)
        column_names.append(IcebergReservedField.end_time.value.field_name)
        column_names.append(IcebergReservedField.thedate.value.field_name)
        column_names.append(IcebergReservedField.dteventtime.value.field_name)
        column_names.append(IcebergReservedField.dteventtimestamp.value.field_name)
        column_names.append(IcebergReservedField.localtime.value.field_name)

        table_records = []
        for item_array in iterator:
            merge_feature_label_list = []
            for item in item_array:
                merge_feature_label_list.extend(item.tolist())
            final_result_list = []
            final_result_list.extend(
                self.convert_target_type(column_names[0 : len(column_names) - 6], merge_feature_label_list)
            )
            # 增加预留字段
            final_result_list.extend(
                [
                    JString(str(self.output["time_range_list"][0]["start_time"])),
                    JString(str(self.output["time_range_list"][0]["end_time"])),
                ]
            )
            final_result_list.extend([JInt(the_date), dt_event_time, JLong(sink_dt_event_time), local_time])
            table_records.append(final_result_list)
        tables.append_table(table, column_names, table_records, {})

    def convert_target_type(self, column_names, origin_list):
        # 类型转换，dataset中的值全部为float，需要根据实际情况转换为目前类型
        field_map = {field["field"].lower(): field for field in self.fields}
        new_value_list = []
        for column_index in range(0, len(column_names)):
            column_name = column_names[column_index]
            column_value = origin_list[column_index]
            column_type = field_map[column_name]["type"]
            if column_type == "long":
                new_value_list.append(JLong(column_value))
            elif column_type == "int":
                new_value_list.append(JInt(column_value))
            elif column_type == "string":
                new_value_list.append(JString(column_value))
            else:
                new_value_list.append(JDouble(column_value))
        return new_value_list


class SinkNodeBuilder(Builder):
    output = None
    dt_event_timestamp = 0

    def __init__(self, info):
        super().__init__(info)
        self.init_builder(info)

    def init_builder(self, info):
        pass


class ModelSinkNodeBuilder(SinkNodeBuilder):
    table_type = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        output_info = info["output"]
        self.table_type = output_info.get("table_type", TableType.OTHER.value)
        path = output_info["path"]
        self.output = {
            "path": path,
            "format": output_info["format"],
            "mode": info["output_mode"],
            "cluster_group": output_info["cluster_group"],
            "hdfs_user": output_info["hdfs_user"],
            "component_url": output_info["component_url"],
        }

    def build(self):
        return ModelSinkNode(self)


class ModelIcebergQuerySetSinkNodeBuilder(SinkNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        self.iceberg_conf = info["iceberg_conf"]
        iceberg_table_name = info["iceberg_table_name"]
        output_mode = info["output_mode"]
        self.output = {"iceberg_table_name": iceberg_table_name, "mode": output_mode}

    def build(self):
        return ModelIcebergQuerySetSinkNode(self)


class ModelIcebergResultSetSinkNodeBuilder(SinkNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        self.iceberg_conf = info["iceberg_conf"]
        iceberg_table_name = info["iceberg_table_name"]
        self.dt_event_timestamp = info["dt_event_time"]
        end_time = self.dt_event_timestamp + 3600 * 1000
        self.output = {
            "iceberg_table_name": iceberg_table_name,
            "mode": "overwrite",
            "dt_event_timestamp": self.dt_event_timestamp,
            "time_range_list": [{"start_time": self.dt_event_timestamp, "end_time": end_time}],
        }

    def build(self):
        return ModelIcebergResultSetSinkNode(self)


class ModelIcebergResultSetSinkNodeBuilderV2(SinkNodeBuilder):
    iceberg_conf = None

    def __init__(self, info):
        super().__init__(info)

    def init_builder(self, info):
        self.iceberg_conf = info["storage_conf"]["storekit_hdfs_conf"]
        iceberg_table_name = info["storage_conf"]["storekit_hdfs_conf"]["physical_table_name"]
        schedule_time_in_hour = PeriodicTimeHelper.round_schedule_timestamp_to_hour(info["schedule_time"])
        schedule_time_obj = BatchTimeStamp(schedule_time_in_hour)
        data_offset_obj = BatchTimeDelta()
        data_offset_obj.init_delta_from_string(info["data_time_offset"])
        self.dt_event_timestamp = schedule_time_obj.minus(data_offset_obj).get_time_in_mills()
        end_time = self.dt_event_timestamp + 3600 * 1000
        self.output = {
            "iceberg_table_name": iceberg_table_name,
            "mode": "overwrite",
            "dt_event_timestamp": self.dt_event_timestamp,
            "time_range_list": [{"start_time": self.dt_event_timestamp, "end_time": end_time}],
            "storage_conf": info["storage_conf"],
        }

    def build(self):
        return ModelIcebergResultSetSinkNode(self)
