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
import requests
from django.conf import settings
from .job_metrics_collector import JobMetricsCollector
from resourcecenter.log.log_base import get_logger

application_url = "http://{cluster_domain}/ws/v1/cluster/apps/?states=running"

logger = get_logger(__name__)


class YarnJobMetricsCollector(JobMetricsCollector):
    def __init__(self):
        JobMetricsCollector.__init__(self)
        self.application_cache = {}

    def collect(self, cluster_type, cluster_domain, cluster_name, job_ids):
        JobMetricsCollector.collect(self, cluster_type, cluster_domain, cluster_name, job_ids)

    def get_application_list(self, cluster_domain, cluster_name):
        if cluster_domain not in self.application_cache or cluster_name not in self.application_cache[cluster_domain]:
            # 从YARN接口拉取
            request_url = application_url.format(cluster_domain=cluster_domain)
            # 过滤可采集的application类型
            yarn_application_types = settings.YARN_APPLICATION_TYPES
            if yarn_application_types and len(yarn_application_types) > 0:
                request_url += "&applicationTypes=" + yarn_application_types[0]
                for application_type in yarn_application_types[1:]:
                    request_url += "," + application_type
            try:
                response = requests.request("GET", request_url)
            except Exception as e:
                logger.error("Failed to get applications from cluster:%s, detail:%s" % (cluster_domain, e.message))
                return None
            applications = json.loads(response.text)
            """
            接口返回格式：
            {
                "apps": {
                    "app": [
                        {
                            "id": "application_XXX_XXX",
                            "user": "root",
                            "name": "10086_job_name",
                            "queue": "root.queue",
                            "state": "RUNNING",
                            "finalStatus": "UNDEFINED",
                            "progress": 10.0,
                            "trackingUI": "ApplicationMaster",
                            "trackingUrl": "http://rm-host/proxy/application_XXX_XXX/",
                            "applicationType": "SPARK",
                            "startedTime": 1614261443125,
                            "finishedTime": 0,
                            "elapsedTime": 1186427410,
                            "allocatedMB": 4096,
                            "allocatedVCores": 2,
                            "reservedMB": 0,
                            "reservedVCores": 0,
                            "runningContainers": 2,
                            "memorySeconds": 4859579344,
                            "vcoreSeconds": 2372840,
                        },
                        ...
                    ]
                }
            }
            """
            if (
                "apps" in applications
                and "app" in applications["apps"]
                and isinstance(applications["apps"]["app"], list)
            ):
                for application in applications["apps"]["app"]:
                    queue = application["queue"]
                    if cluster_domain not in self.application_cache:
                        self.application_cache[cluster_domain] = {}
                    if queue not in self.application_cache[cluster_domain]:
                        self.application_cache[cluster_domain][queue] = []
                    self.application_cache[cluster_domain][queue].append(application)
        if cluster_name in self.application_cache[cluster_domain]:
            return self.application_cache[cluster_domain][cluster_name]
        return None
