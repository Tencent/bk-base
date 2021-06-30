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
import time

from bkbase.dataflow.metrics.base_metric import BaseMetric
from bkbase.dataflow.metrics.data_platform_metric import DataPlatformMetric

if __name__ == "__main__":
    TOPIC = "xxx_topic"
    HOST = "xxx_host"
    # BaseMetric 上报测试，上报 kafka 时第四个参数改为 kafka
    base_metric1 = BaseMetric(1, HOST, TOPIC, "console")
    # kafka 上报测试
    now = int(time.time())
    base_metric1.literal("time").set_literal(now)
    base_metric1.literal("database").set_literal("monitor_custom_metrics")
    base_metric1.counter("realtime_ipredis_query.xxx_cnt").inc(1)
    base_metric1.counter("realtime_ipredis_query.connection_cnt")
    base_metric1.counter("realtime_ipredis_query.query_cnt_tpCounter")
    base_metric1.literal("realtime_ipredis_query.tags.server").set_literal("127.0.0.1")

    # 上报 kafka 时第四个参数改为 kafka
    builder = DataPlatformMetric.Builder(1, HOST, TOPIC, "console")
    RT_ID = "591_zc_realtime"
    TOPOLOGY_ID = "topology-12345"
    data_platform_metric = (
        builder.module("stream")
        .component("flink")
        .cluster("default")
        .storage_host("127.0.0.1")
        .storage_port(8080)
        .physical_tag("topology_id", TOPOLOGY_ID)
        .physical_tag("task_id", 1)
        .physical_tag("result_table_id", RT_ID)
        .logical_tag("result_table_id", RT_ID)
        .custom_tag("ip", "0.0.0.0")
        .custom_tag("port", "8888")
        .custom_tag("task", 1)
        .build()
    )
    # 可以放里面
    data_platform_metric.literal("version").set_literal("3.0")
    data_platform_metric.set_input_tags_counter({"front_rt1_1_1577808000": 100, "front_rt1_2_1577808000": 200})
    data_platform_metric.get_input_total_cnt_counter().inc(4000)
    data_platform_metric.get_input_total_cnt_inc_counter().inc(300)
    data_platform_metric.set_output_tags_counter({"rt1_1_577808000": 290})
    data_platform_metric.get_output_total_cnt_counter().inc(3000)
    data_platform_metric.get_output_total_cnt_inc_counter().inc(290)
    data_platform_metric.get_output_ckp_drop_counter().inc(5)
    data_platform_metric.get_output_ckp_drop_tags_counter("rt1_1_577808000").inc(5)
    data_platform_metric.set_data_drop_tags_counter("drop_code1", "数据格式解析失败", 5)
    data_platform_metric.get_delay_window_time_counter().inc(60)
    data_platform_metric.get_delay_waiting_time_counter().inc(30)
    # 时间戳
    data_platform_metric.set_delay_min_delay_literal(now, now)
    data_platform_metric.set_delay_max_delay_literal(now, now)
    data_platform_metric.set_data_malformed_counter("field1", "数据结构异常", 10000)

    time.sleep(5)

    data_platform_metric.closed()
    time.sleep(5)
    base_metric1.destroy_reporter()

    # 主函数不能退出
    while True:
        time.sleep(10)
