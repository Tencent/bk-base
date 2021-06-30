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

from bkbase.dataflow.metrics.data_platform_metric import DataPlatformMetric


class MetricOutputProcess:
    def __init__(self, run_mode, job_id, metric, rt):
        interval = 60 if "product" == run_mode else 6
        host = metric["metric_kafka_server"]
        topic = metric["metric_kafka_topic"]
        builder = DataPlatformMetric.Builder(interval, host, topic, "kafka")
        self.rt = rt
        self.data_platform_metric = (
            builder.module("stream")
            .component("spark_structured_streaming")
            .cluster("default")
            .physical_tag("job_id", job_id)
            .physical_tag("result_table_id", rt)
            .logical_tag("result_table_id", rt)
            .build()
        )
        self.data_platform_metric.literal("version").set_literal("2.4.4")
        self.data_platform_metric.set_output_tags_counter({rt: 0})

    def foreach_batch_writer(self, batch_df, batch_id):
        batch_df_total_num = batch_df.count()
        self.data_platform_metric.get_output_total_cnt_counter().inc(batch_df_total_num)
        self.data_platform_metric.get_output_total_cnt_inc_counter().inc(batch_df_total_num)
        self.data_platform_metric.get_output_tags_counter(self.rt).inc(batch_df_total_num)
