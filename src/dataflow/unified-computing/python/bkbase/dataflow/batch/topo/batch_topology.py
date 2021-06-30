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

from bkbase.dataflow.batch.utils import batch_utils
from bkbase.dataflow.batch.utils.custom_type import TimeUnit, WindowType
from bkbase.dataflow.core.types.node import SinkNode, SourceNode
from bkbase.dataflow.core.types.topology import Topology


class BatchTopology(Topology):
    def __init__(self, batch_topo_builder):
        super().__init__(batch_topo_builder)
        self.batch_topo_builder = batch_topo_builder
        self.job_id = self.batch_topo_builder.schedule_id
        self.schedule_time = batch_utils.time_round_to_hour(int(batch_topo_builder.schedule_info["schedule_time"]))
        self.exec_id = self.batch_topo_builder.schedule_info["exec_id"]
        self.geog_area_code = self.batch_topo_builder.geog_area_code
        self.is_accumulate = self.batch_topo_builder.is_accumulate
        self.user_pkg_path = self.batch_topo_builder.user_pkg_path
        self.map_nodes(batch_topo_builder.type_tables)

    def __str__(self):
        return (
            "job_id: %s, source_nodes: {%s}, sink_nodes: {%s}, "
            "schedule_time: %s, exec_id: %s, geog_area_code: %s, "
            "user_main_class: %s, user_main_module: %s, user_pkg_path: %s, user_args: %s"
            % (
                self.job_id,
                str(self.__generate_source_str()),
                str(self.__generate_sink_str()),
                self.schedule_time,
                self.exec_id,
                self.geog_area_code,
                self.user_main_class,
                self.user_main_module,
                self.user_pkg_path,
                self.user_args,
            )
        )

    def map_nodes(self, type_tables):
        for node_type in type_tables:
            if node_type == "source":
                self.source_nodes = self.map_source_node(type_tables[node_type])
            elif node_type == "sink":
                self.sink_nodes = self.map_sink_node(type_tables[node_type])

    def map_source_node(self, nodes):
        node_dict = {}
        for node in nodes:
            if nodes[node]["input"]["type"] != "ignite":
                batch_source_node = BatchSourceNode()
                batch_source_node.node_id = node
                batch_source_node.map(nodes[node])
                node_dict[node] = batch_source_node
            else:
                raise NotImplementedError("Only HDFS input is supported")

        return node_dict

    def map_sink_node(self, nodes):
        node_dict = {}
        for node in nodes:
            batch_sink_node = BatchSinkNode()
            batch_sink_node.node_id = node
            batch_sink_node.map(nodes[node])
            node_dict[node] = batch_sink_node
        return node_dict

    def __generate_source_str(self):
        source_str = ""
        for source_node in self.source_nodes:
            source_str += str(self.source_nodes[source_node]) + " "
        return source_str

    def __generate_sink_str(self):
        sink_str = ""
        for sink_node in self.sink_nodes:
            sink_str += str(self.sink_nodes[sink_node]) + " "
        return sink_str


class BatchSourceNode(SourceNode):
    def __init__(self):
        super().__init__()
        self.window = None
        self.storage_params = None
        self.self_dependency_mode = ""

    def map(self, node):
        self.window = self.__build_window(node["window"])
        self.input = node["input"]["input_paths"]
        self.self_dependency_mode = node["self_dependency_mode"]
        self.storage_params = node["input"]["conf"]
        self.fields = node["fields"]

    def __build_window(self, window_params):
        is_accumulate = window_params["is_accumulate"]
        batch_window_offset = int(window_params["batch_window_offset"])
        start = int(window_params["segment"]["start"])
        end = int(window_params["segment"]["end"])

        if str(window_params["segment"]["unit"]) == "day":
            unit = TimeUnit.DAY
        else:
            unit = TimeUnit.HOUR

        if is_accumulate:
            window_type = WindowType.ACCUMULATION
        else:
            window_type = WindowType.TUMBLING

        return BatchWindow(batch_window_offset, start, end, unit, window_type)

    def __str__(self):
        return "{}, window: {}, storage_params: {}, self_dependency_mode: {}".format(
            super().__str__(),
            self.window,
            self.storage_params,
            self.self_dependency_mode,
        )


class BatchSinkNode(SinkNode):
    def __init__(self):
        super().__init__()
        self.storages = None
        self.extra_storage = False

    def __str__(self):
        return "{}, storages: {}, extra_storage {}".format(super().__str__(), self.storages, self.extra_storage)

    def map(self, node):
        self.output = node["output"]["output_paths"]
        self.event_time = node["output"]["event_time"]
        self.storages = node["output"]["conf"]
        self.extra_storage = node["output"]["extra_storage"]


class BatchWindow(object):
    def __init__(self, batch_window_offset, start, end, unit, window_type):
        self.batch_window_offset = batch_window_offset
        self.start = start
        self.end = end
        self.unit = unit
        self.window_type = window_type

    def get_start_in_hour(self):
        if self.unit == TimeUnit.DAY:
            return self.start * 24
        return self.start

    def get_end_in_hour(self):
        if self.unit == TimeUnit.DAY:
            return self.end * 24
        return self.end

    def get_batch_window_offset(self):
        return self.batch_window_offset

    def __str__(self):
        return "batch_window_offset: {}, start: {}, end: {}, unit: {}, window_type: {}".format(
            self.batch_window_offset,
            self.start,
            self.end,
            self.unit,
            self.window_type,
        )
