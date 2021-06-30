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
import socket
from datetime import datetime

from bkbase.dataflow.metrics.data_platform_metric import DataPlatformMetric
from bkbase.dataflow.one_model.utils import deeplearning_logger
from bkdata_datalake import tables


class MonitorHandler(object):
    def __init__(self, topology, rt_id):
        self.topology = topology
        self.rt_id = rt_id
        if self.topology.metric_config:
            self.builder = DataPlatformMetric.Builder(
                1, self.topology.metric_config["metric_rest_api_url"], None, "http"
            )
        else:
            self.builder = None

    def report(self, input_info):
        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        output_rt_info = self.get_output_count_info()
        cluster_name = output_rt_info[self.rt_id]["conf"]["cluster_name"]
        storage_hosts = (
            output_rt_info[self.rt_id]["conf"]["hosts"]
            if "hosts" in output_rt_info[self.rt_id]["conf"]
            else "127.0.0.1"
        )
        storage_port = (
            output_rt_info[self.rt_id]["conf"]["port"] if "port" in output_rt_info[self.rt_id]["conf"] else 80
        )

        data_platform_metric = (
            self.builder.module("batch")
            .component("tensorflow")
            .cluster("default")
            .cluster_name(cluster_name)
            .cluster_type("hdfs")
            .storage_host(storage_hosts)
            .storage_port(storage_port)
            .physical_tag("job_id", self.topology.job_id)
            .physical_tag("result_table_id", self.rt_id)
            .logical_tag("result_table_id", self.rt_id)
            .custom_tag("ip", ip_addr)
            .build()
        )
        input_tags_counter = {}
        for rt_id in input_info:
            tags = rt_id
            input_tags_counter[tags] = input_info[rt_id]["count"]
            data_platform_metric.get_input_total_cnt_counter().inc(input_info[rt_id]["count"])
            data_platform_metric.get_input_total_cnt_inc_counter().inc(input_info[rt_id]["count"])
        data_platform_metric.set_input_tags_counter(input_tags_counter)
        output_tags = self.rt_id
        data_platform_metric.set_output_tags_counter({output_tags: output_rt_info[self.rt_id]["count"]})
        data_platform_metric.get_output_total_cnt_counter().inc(output_rt_info[self.rt_id]["count"])
        data_platform_metric.get_output_total_cnt_inc_counter().inc(output_rt_info[self.rt_id]["count"])

    def get_output_count_info(self):
        output_info = {}
        sink_obj = self.topology.sink_nodes[self.rt_id]
        start = sink_obj.output["time_range_list"][0]["start_time"]
        end = sink_obj.output["time_range_list"][0]["end_time"]
        count = MonitorHandler.get_rt_count(start, end, sink_obj.output["storage_conf"]["storekit_hdfs_conf"])
        output_info[self.rt_id] = {"count": count, "start": start, "end": end, "conf": sink_obj.output["storage_conf"]}
        deeplearning_logger.info("output info:{}".format(json.dumps(output_info)))
        return output_info

    @staticmethod
    def get_rt_count(start_time, end_time, iceberg_conf):
        table_info = iceberg_conf["physical_table_name"].split(".")
        table = tables.load_table(table_info[0], table_info[1], iceberg_conf)
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        start = datetime.utcfromtimestamp(start_time / 1000)
        end = datetime.utcfromtimestamp(end_time / 1000)
        return tables.record_count_between(table, start.strftime(date_format), end.strftime(date_format))
