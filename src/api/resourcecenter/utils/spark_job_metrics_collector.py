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
from django.conf import settings
from .yarn_job_metrics_collector import YarnJobMetricsCollector
from resourcecenter.log.log_base import get_logger

logger = get_logger(__name__)


class SparkJobMetricsCollector(YarnJobMetricsCollector):
    def __init__(self):
        YarnJobMetricsCollector.__init__(self)

    def collect(self, cluster_type, cluster_domain, cluster_name, job_ids):
        # 从缓存中获取
        application_list = self.get_application_list(cluster_domain, cluster_name)
        metrics = []
        if application_list:
            for application in application_list:
                # 只采集application类型为SPARK且在资源系统有提交记录的任务资源
                if (
                    application["applicationType"] != settings.SPARK_APPLICATION_TYPE
                    and application["name"] not in job_ids
                ):
                    continue
                metric = {
                    "job_id": application["name"],
                    "inst_id": application["id"],
                    "allocated_vcores": application["allocatedVCores"],
                    "allocated_memory_mb": application["allocatedMB"],
                }
                metrics.append(metric)
        return metrics
