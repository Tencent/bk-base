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


class JobMetricsCollector:
    def __init__(self):
        pass

    def collect(self, cluster_type, cluster_domain, cluster_name, job_ids):
        """
        采集指定集群下任务资源指标
        :param cluster_type: 集群类型，目前支持spark, flink-yarn-cluster, flink-yarn-session的计算集群任务资源指标采集
        :param cluster_domain: 集群Master（API server）域名
        :param cluster_name: 集群（队列）名
        :param job_ids: 待采集的任务ID集合
        :return:
        [
            {
                "job_id": "10086_test_job_1",
                "inst_id": "application_10086_10000"
                "allocated_vcores": 1,
                "allocated_memory_mb": 2048 # 单位：MB
            },
            {
                "job_id": "10086_test_job_2",
                "inst_id": "application_10086_10001"
                "allocated_vcores": 2,
                "allocated_memory_mb": 4096 # 单位：MB
            }
        ]
        """
        pass
