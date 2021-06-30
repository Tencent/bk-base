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

from bkbase.dataflow.one_model.topo.deeplearning_sink_node import (
    ModelIcebergQuerySetSinkNodeBuilder,
    ModelIcebergResultSetSinkNodeBuilder,
    ModelIcebergResultSetSinkNodeBuilderV2,
    ModelSinkNodeBuilder,
)
from bkbase.dataflow.one_model.topo.deeplearning_source_node import (
    ModelIcebergDebugSourceNodeBuilder,
    ModelIceBergQuerySetSourceNodeBuilder,
    ModelIcebergResultSetSourceNodeBuilder,
    ModelIcebergResultSetSourceNodeBuilderV2,
    ModelSourceNodeBuilder,
)
from bkbase.dataflow.one_model.utils.deeplearning_constant import (
    NodeType,
    Role,
    TableType,
)
from bkbase.dataflow.one_model.utils.periodic_time_helper import PeriodicTimeHelper


class ModelBuilderFactory(object):
    def __init__(self, job_type, schedule_time):
        self.job_type = job_type
        self.schedule_time = schedule_time
        self.node_info = None

    def get_builder(self, info):
        pass

    def get_config_map(self, table_type, source_node_type):
        pass


class ModelSourceBuilderFactory(ModelBuilderFactory):
    def __init__(self, schedule_time, job_type, is_debug, version):
        super().__init__(job_type, schedule_time)
        self.debug_label = 1 if is_debug else 0
        self.version = version

    def get_builder(self, info):
        self.node_info = info
        source_node_type = self.node_info.get("type", NodeType.DATA.value)
        if source_node_type == NodeType.MODEL.value:
            source_node_map = self.get_config_map(TableType.OTHER.value, source_node_type)
            source_node_map["type"] = source_node_type
            return ModelSourceNodeBuilder(source_node_map)
        elif source_node_type == NodeType.DATA.value:
            if self.version != "v1":
                # 有标明版本且不是v1,则认为为v2
                source_node_map = self.node_info
                source_node_map["schedule_time"] = self.schedule_time
                return ModelIcebergResultSetSourceNodeBuilderV2(source_node_map)
            else:
                # 旧有的V1版本逻辑
                input_info = self.node_info["input"]
                table_type = input_info.get("table_type", TableType.OTHER.value)
                source_node_map = self.get_config_map(table_type, source_node_type)
                if source_node_type == NodeType.DATA.value:
                    if table_type == TableType.QUERY_SET.value:
                        return ModelIceBergQuerySetSourceNodeBuilder(source_node_map)
                    elif table_type == TableType.RESULT_TABLE.value:
                        if self.debug_label:
                            return ModelIcebergDebugSourceNodeBuilder(source_node_map)
                        else:
                            return ModelIcebergResultSetSourceNodeBuilder(source_node_map)
                    else:
                        raise Exception("unsupported table type:{}".format(table_type))
        else:
            raise Exception("unsupported source node type:{}".format(source_node_type))

    def get_config_map(self, table_type, source_node_type):
        input_info = self.node_info["input"]
        source_node_map = {}
        source_node_map["id"] = self.node_info["id"]
        source_node_map["name"] = self.node_info["name"]
        source_node_map["job_type"] = self.job_type
        source_node_map["schedule_time"] = self.schedule_time
        source_node_map["input"] = input_info
        if source_node_type == NodeType.MODEL.value:
            source_node_map["type"] = source_node_type
        else:
            source_node_map["fields"] = self.node_info["fields"]
            iceberg_conf = self.node_info["iceberg_config"]
            source_node_map["iceberg_table_name"] = iceberg_conf["physical_table_name"]
            source_node_map["iceberg_conf"] = iceberg_conf

            if table_type == TableType.QUERY_SET.value:
                source_node_map["start_time"] = -1
                source_node_map["end_time"] = -1
                source_node_map["type"] = source_node_type
            else:
                source_node_map["parent_node_url"] = self.node_info["parent_node_url"]
                window_info = self.node_info["window"]
                window_info["schedule_time"] = self.schedule_time
                """
                is_accumulate = window_info['accumulate']
                source_node_map['accumulate'] = is_accumulate
                if is_accumulate:
                    source_node_map['data_start'] = window_info['data_start']
                    source_node_map['data_end'] = window_info['data_end']
                else:
                    source_node_map['window_size'] = window_info['window_size']
                    source_node_map['window_delay'] = window_info['window_delay']
                    source_node_map['window_size_period'] = window_info['window_size_period']
                """
                source_node_map["type"] = Role.batch.value
                source_node_map["window"] = window_info

        return source_node_map


class ModelSinkBuilderFactory(ModelBuilderFactory):
    def __init__(self, job_type, schedule_time, source_nddes, version):
        super().__init__(job_type, schedule_time)
        self.souce_nodes = source_nddes
        self.version = version

    def get_builder(self, info):
        self.node_info = info
        sink_node_type = self.node_info.get("type", NodeType.DATA.value)
        if sink_node_type == NodeType.MODEL.value:
            sink_conf_map = self.get_config_map(TableType.OTHER.value, sink_node_type)
            return ModelSinkNodeBuilder(sink_conf_map)
        elif sink_node_type == NodeType.DATA.value:
            if self.version != "v1":
                sink_conf_map = self.node_info
                sink_conf_map["schedule_time"] = self.schedule_time
                return ModelIcebergResultSetSinkNodeBuilderV2(sink_conf_map)
            else:
                output_info = self.node_info["output"]
                table_type = output_info.get("table_type", TableType.OTHER.value)
                sink_conf_map = self.get_config_map(table_type, sink_node_type)
                if table_type == TableType.QUERY_SET.value:
                    return ModelIcebergQuerySetSinkNodeBuilder(sink_conf_map)
                elif table_type == TableType.RESULT_TABLE.value:
                    return ModelIcebergResultSetSinkNodeBuilder(sink_conf_map)
                else:
                    raise Exception("unsupported table type:{}".format(table_type))
        else:
            raise Exception("unsupported sink node type:{}".format(sink_node_type))

    def get_config_map(self, table_type, sink_node_type):
        sink_node_map = {}
        output_info = self.node_info["output"]
        sink_node_id = self.node_info["id"]
        sink_node_map["id"] = sink_node_id
        sink_node_map["name"] = sink_node_id
        sink_node_map["job_type"] = self.job_type
        # sink_node_map['storages'] = self.node_info['storages']
        sink_node_map["schedule_time"] = self.schedule_time
        sink_node_map["output_mode"] = output_info.get("mode", "overwrite")

        sink_node_map["output"] = output_info
        if sink_node_type == NodeType.MODEL.value:
            sink_node_map["type"] = sink_node_type
        else:
            sink_node_map["fields"] = self.node_info["fields"]
            iceberg_conf = output_info["iceberg_config"]
            sink_node_map["iceberg_table_name"] = iceberg_conf["physical_table_name"]
            sink_node_map["iceberg_conf"] = iceberg_conf
            if table_type == TableType.QUERY_SET.value:
                sink_node_map["dt_event_time"] = 0
                sink_node_map["type"] = sink_node_type
            elif table_type == TableType.RESULT_TABLE.value:
                # add window information
                min_window_size, min_window_unit = PeriodicTimeHelper.get_min_window_info(self.souce_nodes)
                sink_node_map["min_window_size"] = min_window_size
                sink_node_map["min_window_unit"] = min_window_unit
                sink_node_map["type"] = Role.batch.value
                schedule_time_in_hour = PeriodicTimeHelper.round_schedule_timestamp_to_hour(self.schedule_time)
                dt_event_time_stamp = PeriodicTimeHelper.get_sink_dt_event_time_in_ms(
                    schedule_time_in_hour, min_window_size, min_window_unit
                )
                sink_node_map["dt_event_time"] = dt_event_time_stamp
        return sink_node_map
