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

from bkbase.dataflow.batch.conf import batch_conf
from bkbase.dataflow.batch.topo.batch_topology import BatchTopology
from bkbase.dataflow.batch.utils import batch_logger, batch_utils
from bkbase.dataflow.core.types.topology import Builder


class BatchTopoBuilder(Builder):
    def __init__(self, params, schedule_info):
        super().__init__(params)
        self.schedule_time = batch_utils.time_round_to_hour(int(schedule_info["schedule_time"]))
        self.schedule_id = schedule_info["schedule_id"]
        self.schedule_info = schedule_info
        # To use schedule_time round to hour
        self.schedule_info["schedule_time"] = self.schedule_time
        self.geog_area_code = self.params["geog_area_code"]
        self.type_tables = {"source": self.params["nodes"]["source"], "sink": self.params["nodes"]["sink"]}

        self.user_pkg_path = self.params["user_package_path"]

        self.is_accumulate = True if self.params["nodes"]["transform"]["window"]["type"] == "accumulate" else False

        self.min_parent_window_size = self.__get_min_window_hour_size()

        self.init_source_node_params()
        self.init_sink_params()

    def build(self):
        topology = BatchTopology(self)
        batch_logger.info("Generated topology: %s" % topology)
        return topology

    def init_source_node_params(self):
        self.__set_source_node_window_type()
        self.__set_source_node_input_path()

    def __set_source_node_window_type(self):
        nodes = self.params["nodes"]["source"]
        for node in nodes:
            nodes[node]["window"]["is_accumulate"] = self.is_accumulate

    def __set_source_node_input_path(self):
        nodes = self.params["nodes"]["source"]
        for node in nodes:
            # build input path
            if nodes[node]["input"]["type"] != "ignite":
                origin_root = self.__build_hdfs_input_root(nodes[node]["input"]["conf"])
            else:
                raise NotImplementedError("Only HDFS input is supported")

            if nodes[node]["self_dependency_mode"] == "":
                input_paths = self.__build_hdfs_input(origin_root, nodes[node])
            else:
                input_paths = {}
                input_paths[batch_conf.SELF_DEPENDENCY_ORIGIN_KEY] = self.__build_hdfs_input(origin_root, nodes[node])
                copy_root = "{}{}/{}/{}".format(
                    nodes[node]["input"]["conf"]["name_service"],
                    batch_conf.SELF_DEPENDENCY_PATH_PREFIX,
                    batch_utils.get_rt_number(node),
                    node,
                )
                input_paths[batch_conf.SELF_DEPENDENCY_COPY_KEY] = self.__build_hdfs_input(copy_root, nodes[node])
            nodes[node]["input"]["input_paths"] = input_paths

    def init_sink_params(self):
        self.__set_sink_node_event_time()
        self.__set_sink_node_output()

    def __set_sink_node_event_time(self):
        nodes = self.params["nodes"]["sink"]
        dt_event_time = self.schedule_time - self.__get_min_window_hour_size() * 3600 * 1000
        for node in nodes:
            nodes[node]["output"]["event_time"] = dt_event_time

    def __set_sink_node_output(self):
        nodes = self.params["nodes"]["sink"]
        for node in nodes:
            nodes[node]["output"]["output_paths"] = self.__build_output(
                nodes[node]["output"]["conf"], nodes[node]["output"]["event_time"]
            )

    def __build_hdfs_input_root(self, storage_params):
        root = "{}{}".format(storage_params["name_service"], storage_params["physical_table_name"])
        return root

    def __build_hdfs_input(self, root, node):
        if self.is_accumulate:
            return self.__build_hdfs_accumulate_input(root, node)
        else:
            return self.__build_hdfs_window_input(root, node)

    def __build_hdfs_accumulate_input(self, root, node):
        window = node["window"]
        hour_millisecond = 3600 * 1000
        segment_params = window["segment"]
        end_in_hour = int(segment_params["end"])
        start_in_hour = int(segment_params["start"])
        schedule_hour = int(batch_utils.timestamp_to_date(self.schedule_time, "%H"))
        if (schedule_hour - 1 + 24) % 24 < start_in_hour or (schedule_hour - 1 + 24) % 24 > end_in_hour:
            msg = "Accumulation window Not In Run Period. Expect Range ({} -> {})".format(
                str((start_in_hour + 1) % 24), str((end_in_hour + 1) % 24)
            )
            batch_logger.info(msg)
            raise Exception(msg)

        date_timestamp = batch_utils.date_to_timestamp(
            batch_utils.timestamp_to_date(self.schedule_time, "/%Y/%m/%d"), "/%Y/%m/%d"
        )

        if schedule_hour == 0:
            date_timestamp = date_timestamp - 24 * hour_millisecond

        start_millisecond = date_timestamp + start_in_hour * hour_millisecond

        return batch_utils.generate_hdfs_path(root, start_millisecond, self.schedule_time)

    def __build_hdfs_window_input(self, root, node):
        window = node["window"]
        role = node["role"]
        hour_millisecond = 3600 * 1000
        segment_params = window["segment"]
        end_in_hour = int(segment_params["end"])
        start_in_hour = int(segment_params["start"])
        batch_window_offset = int(window["batch_window_offset"])
        if segment_params["unit"] == "day":
            end_in_hour = end_in_hour * 24
            start_in_hour = start_in_hour * 24

        if role.lower() == "stream" or role.lower() == "clean":
            start = self.schedule_time - end_in_hour * hour_millisecond
            end = self.schedule_time - start_in_hour * hour_millisecond
            return batch_utils.generate_hdfs_path(root, start, end)
        # role == "batch"
        elif role == "batch":
            parent_schedule_period = node["schedule_period"].lower()
            if parent_schedule_period == "day":
                start = self.schedule_time - (end_in_hour + batch_window_offset - 24 * 1) * hour_millisecond
                end = self.schedule_time - (start_in_hour + batch_window_offset - 24 * 1) * hour_millisecond
                return batch_utils.generate_hdfs_path(root, start, end, 24)
            else:
                start = self.schedule_time - (end_in_hour + batch_window_offset - 1) * hour_millisecond
                end = self.schedule_time - (start_in_hour + batch_window_offset - 1) * hour_millisecond
                return batch_utils.generate_hdfs_path(root, start, end)
        else:
            raise Exception("unsupported role type: {}".format(role))

    def __build_output(self, storage_params, dt_event_time):
        root = "{}{}".format(storage_params["name_service"], storage_params["physical_table_name"])
        path = batch_utils.timestamp_to_path_format(dt_event_time)
        full_path = "{}{}".format(root, path)
        batch_logger.info("Generated output path: %s" % full_path)
        return full_path

    def __get_min_window_hour_size(self):
        nodes = self.params["nodes"]["source"]
        window_size_list = []
        for node in nodes:
            segment_params = nodes[node]["window"]["segment"]
            window_size = segment_params["end"] - segment_params["start"]
            if segment_params["unit"] == "day":
                window_size = window_size * 24
            if window_size != -1:
                window_size_list.append(window_size)

        if len(window_size_list) == 0:
            transform_window_params = self.params["nodes"]["transform"]["window"]
            min_window_size = int(transform_window_params["count_freq"])
            if transform_window_params["period_unit"] == "day":
                min_window_size = min_window_size * 24
        else:
            min_window_size = min(window_size_list)
        batch_logger.info("Get minimum parent window size %s" % min_window_size)
        return min_window_size
