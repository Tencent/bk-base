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

import socket

from bkbase.dataflow.batch.api import batch_api


class MonitorHandler(object):
    def __init__(self, topology, rt_id):
        self.tags = [topology.geog_area_code]
        self.message = {}

        schedule_time = topology.schedule_time

        self.message["version"] = "2.0"
        self.message["time"] = int(schedule_time / 1000)

        # init info
        self.message["info"] = {}
        self.message["info"]["cluster"] = "default"
        self.message["info"]["logical_tag"] = {"tag": rt_id, "desc": {"result_table_id": rt_id}}

        self.message["info"]["component"] = "python_code"
        self.message["info"]["module"] = "batch"

        self.message["info"]["storage"] = {
            "cluster_name": topology.sink_nodes[rt_id].storages["cluster_name"],
            "cluster_type": "hdfs",
        }

        self.message["info"]["custom_tags"] = {"execute_id": topology.exec_id}

        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)

        self.message["info"]["physical_tag"] = {
            "tag": "{}|{}".format(ip_addr, rt_id),
            "desc": {"result_table_id": rt_id, "hostname": hostname},
        }

        # init input/output
        self.input_result = {"total_cnt": 0, "total_cnt_increment": 0}

        self.output_result = {"total_cnt": 0, "total_cnt_increment": 0}

        self.input_dict = {}
        self.output_dict = {}

        hour_to_second = 3600

        source_nodes = topology.source_nodes
        for node in source_nodes:
            start = schedule_time / 1000 - source_nodes[node].window.get_end_in_hour() * hour_to_second
            end = schedule_time / 1000 - source_nodes[node].window.get_start_in_hour() * hour_to_second
            self.input_dict[node] = {"count": 0, "range": "{}_{}".format(int(start), int(end))}

        sink_nodes = topology.sink_nodes
        find_sink_node = False
        for node in sink_nodes:
            if rt_id == node:
                find_sink_node = True
                self.output_dict[node] = {
                    "count": 0,
                    "range": "{}_{}".format(int(sink_nodes[node].event_time), int(schedule_time)),
                }

        if not find_sink_node:
            raise Exception("Can't find {} in sink nodes".format(rt_id))

    def add_input_count(self, count, rt_id):
        self.input_dict[rt_id]["count"] = count

    def add_output_count(self, count, rt_id):
        self.output_dict[rt_id]["count"] = count

    def generate_monitor_params(self):
        self.input_result["tags"] = {}
        for input in self.input_dict:
            tag_name = "{}|{}".format(input, self.input_dict[input]["range"])
            self.input_result["tags"][tag_name] = self.input_dict[input]["count"]
            self.input_result["total_cnt"] += self.input_dict[input]["count"]
            self.input_result["total_cnt_increment"] += self.input_dict[input]["count"]

        self.output_result["tags"] = {}
        for output in self.output_dict:
            tag_name = "{}|{}".format(output, self.output_dict[output]["range"])
            self.output_result["tags"][tag_name] = self.output_dict[output]["count"]
            self.output_result["total_cnt"] += self.output_dict[output]["count"]
            self.output_result["total_cnt_increment"] += self.output_dict[output]["count"]

        self.message["metrics"] = {
            "data_monitor": {"data_loss": {"input": self.input_result, "output": self.output_result}}
        }

        result = {"message": self.message, "tags": self.tags}

        return result

    def send_report(self):
        batch_api.send_monitor_report(self.generate_monitor_params())
