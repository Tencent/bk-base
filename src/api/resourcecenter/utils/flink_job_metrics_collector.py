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
from .yarn_job_metrics_collector import YarnJobMetricsCollector
from resourcecenter.log.log_base import get_logger

logger = get_logger(__name__)

overview_url = "{tracking_url}/v1/overview"
job_url = "{tracking_url}/v1/jobs"
job_overview_path = "/overview"


class FlinkJobMetricsCollector(YarnJobMetricsCollector):
    def __init__(self):
        YarnJobMetricsCollector.__init__(self)

    def collect(self, cluster_type, cluster_domain, cluster_name, job_ids):
        # 从缓存中获取
        application_list = self.get_application_list(cluster_domain, cluster_name)
        metrics = []
        if application_list:
            for application in application_list:
                if application["applicationType"] != settings.FLINK_APPLICATION_TYPE:
                    continue
                # Flink集群总分配资源，包含JobManager及Yarn资源分配的额外开销
                total_allocated_vcores = application["allocatedVCores"]
                total_allocated_memory_mb = application["allocatedMB"]
                # 获取Flink集群总slot数
                tracking_url = application["trackingUrl"]
                request_url = overview_url.format(tracking_url=tracking_url)
                try:
                    response = requests.request("GET", request_url)
                except Exception as e:
                    logger.error(
                        "Failed to get overview from flink cluster:%s, detail:%s" % (cluster_domain, e)
                    )
                    continue
                overview = json.loads(response.text)
                """
                flink集群总览接口返回格式：
                {
                  "taskmanagers": 27,
                  "slots-total": 27,
                  "slots-available": 0,
                  "jobs-running": 23,
                  "jobs-finished": 2,
                  "jobs-cancelled": 283,
                  "jobs-failed": 0,
                  "flink-version": "1.7.2",
                }
                """
                if "slots-total" in overview and overview["slots-total"] > 0:
                    slots_total = overview["slots-total"]
                else:
                    continue
                # 计算平均每个slot占用的资源
                vcores_per_slot = float(total_allocated_vcores) / slots_total
                memory_mb_per_slot = float(total_allocated_memory_mb) / slots_total
                flink_job_ids = self._get_flink_job_ids(tracking_url, job_ids)
                # 计算每个任务占用的资源：单个slot占用资源 * 任务slot数
                for flink_job_id in flink_job_ids:
                    flink_job_slots = self._get_flink_job_slots(tracking_url, flink_job_id)
                    if not flink_job_slots:
                        continue
                    metric = {
                        "job_id": flink_job_slots["job_name"],
                        "inst_id": application["id"],
                        "allocated_vcores": vcores_per_slot * flink_job_slots["slots"],
                        "allocated_memory": memory_mb_per_slot * flink_job_slots["slots"],
                    }
                    metrics.append(metric)

        return metrics

    @staticmethod
    def _get_flink_job_ids(tracking_url, job_names):
        """
        获取运行中的flink job ID
        :param tracking_url: job manager tracking URL
        :param job_names: 待采集的任务ID集合
        :return:
        """
        request_url = job_url.format(tracking_url=tracking_url) + job_overview_path
        try:
            response = requests.request("GET", request_url)
        except Exception as e:
            logger.error("Failed to get flink job IDs from cluster:%s, detail:%s" % (tracking_url, e))
            return None
        jobs_overview = json.loads(response.text)
        """
        flink任务总览接口返回格式：
        {
          "jobs": [
            {
              "jid": "5b3c0607ddbcc4e885380e2ec6203999",
              "name": "10000_job_1",
              "state": "RUNNING",
              "start-time": 1611648335001,
              "end-time": -1,
              "duration": 3828472252,
            },
            {
              "jid": "5b3c0607ddbcc4e885380e2ec6203910",
              "name": "10000_job_2",
              "state": "RUNNING",
              "start-time": 1611648335001,
              "end-time": -1,
              "duration": 3828472252,
            }
          ]
        }
        """
        flink_job_ids = []
        if "jobs" in jobs_overview and isinstance(jobs_overview["jobs"], list):
            for job in jobs_overview["jobs"]:
                if job["name"] in job_names and "jid" in job:
                    flink_job_ids.append(job["jid"])

        return flink_job_ids

    @staticmethod
    def _get_flink_job_slots(tracking_url, flink_job_id):
        """
        提交到Yarn的Flink任务占用的slot数与子任务最大并行数一致
        由于目前Flink API无法获取Flink job具体占用了哪些TaskManager的哪些slot，现阶段基于上述原则计算任务的slot数
        :param tracking_url: job manager tracking URL
        :param flink_job_id: flink job ID
        :return:
            example:
            {
                "job_name": "job1",
                "slots": 2
            }
        """
        request_url = job_url.format(tracking_url=tracking_url) + "/" + flink_job_id
        try:
            response = requests.request("GET", request_url)
        except Exception as e:
            logger.error("Failed to get flink job metrics from cluster:%s, detail:%s" % (tracking_url, e))
            return None
        jobs = json.loads(response.text)
        """
        {
          "jid": "5b3c0607ddbcc4e885380e2ec6203999",
          "name": "211_6a82b4f62cd042f195a192f869d3db27",
          "state": "RUNNING",
          "start-time": 1611648335001,
          "end-time": -1,
          "duration": 3819319978,
          "now": 1615467654979,
          "vertices": [
            {
              "id": "cbc357ccb763df2852fee8c4fc7d55f2",
              "name": "v1"
              "parallelism": 2,
              "status": "RUNNING",
              "start-time": 1611648346713,
              "end-time": -1,
              "duration": 3819308266,
            },
            {
              "id": "cbc357ccb763df2852fee8c4fc7d55f3",
              "name": "v2"
              "parallelism": 2,
              "status": "RUNNING",
              "start-time": 1611648346713,
              "end-time": -1,
              "duration": 3819308266,
            }
          ]
        }
        """
        if "name" in jobs and "vertices" in jobs and isinstance(jobs["vertices"], list):
            flink_job_slots = {"job_name": jobs["name"]}
            max_parallelism = 0
            for vertex in jobs["vertices"]:
                if "parallelism" in vertex:
                    max_parallelism = max(max_parallelism, vertex["parallelism"])
            flink_job_slots["slots"] = max_parallelism
            return flink_job_slots

        return None
