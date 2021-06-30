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
import time

from rest_framework.response import Response

from common.decorators import list_route
from common.views import APIViewSet
from resourcecenter.handlers import resource_cluster_config, resource_job_submit_log
from resourcecenter.utils.job_metrics_collector_factory import JobMetricsCollectorFactory


class JobMetricsViewSet(APIViewSet):
    """
    @apiDefine job_metrics
    计算资源指标API
    """

    @list_route(methods=["get"], url_path="collect")
    def collect(self, request):
        """
        @api {get} /resourcecenter/job_metrics/collect/ 任务资源指标采集
        @apiName collect
        @apiGroup job_metrics
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_type] 集群类型
        @apiParam {string} [cluster_name] 集群名称
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test",
              "geog_area_code": "inland",
              "service_type": "stream",
              "cluster_id": "all"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "cluster_name": "root.queue1",
                            "geog_area_code": "inland",
                            "cluster_id": "1",
                            "resource_type": "processing",
                            "component_type": "spark",
                            "service_type": "batch",
                            "cluster_type": "spark",
                            "src_cluster_id": "1",
                            "collect_time": 1587466800
                            "job_id": "10086_test_job_1",
                            "inst_id": "application_10086_10000",
                            "allocated_vcores": 1,
                            "allocated_memory_mb": 2048
                        },
                        {
                            "cluster_name": "root.queue2",
                            "geog_area_code": "inland",
                            "cluster_id": "1",
                            "resource_type": "processing",
                            "component_type": "spark",
                            "service_type": "batch",
                            "cluster_type": "spark",
                            "src_cluster_id": "1",
                            "collect_time": 1587466800
                            "job_id": "10086_test_job_2",
                            "inst_id": "application_10086_10001",
                            "allocated_vcores": 2,
                            "allocated_memory_mb": 4096
                        },
                    ],
                    "result": true
                }
        """
        # 查询正在运行的任务
        running_jobs = resource_job_submit_log.filter_list(status="running")
        running_job_ids = set()
        for running_job in running_jobs:
            running_job_ids.add(running_job.job_id)
        # 时间精确到分钟
        collect_time = request.query_params.get("collect_time", int(time.time()) / 60 * 60)
        params = {"resource_type": "processing"}
        if "cluster_id" in request.query_params:
            params["cluster_id"] = request.query_params.get("cluster_id")
        if "cluster_type" in request.query_params:
            params["cluster_type"] = request.query_params.get("cluster_type")
        if "cluster_name" in request.query_params:
            params["cluster_name"] = request.query_params.get("cluster_name")

        collector_factory = JobMetricsCollectorFactory()
        metrics = []
        cluster_list = resource_cluster_config.filter_list(**params)
        if cluster_list:
            for cluster in cluster_list:
                cluster_type = cluster.cluster_type
                # 目前支持cluster type为spark, flink-yarn-cluster, flink-yarn-session的计算集群任务资源指标采集
                collector = collector_factory.get_collector(cluster_type)
                if collector and cluster.connection_info:
                    connection_info = json.loads(cluster.connection_info)
                    cluster_domain = connection_info["cluster_domain"]
                    """
                    获取各集群下正在运行的任务的资源使用指标, 如：
                    [
                        {
                            "job_id": "10086_test_job_1",
                            "allocated_vcores": 1,
                            "allocated_memory_mb": 2048 # 单位：MB
                        },
                        {
                            "job_id": "10086_test_job_2",
                            "allocated_vcores": 2,
                            "allocated_memory_mb": 4096 # 单位：MB
                        }
                    ]
                    """
                    job_metrics = collector.collect(cluster_type, cluster_domain, cluster.cluster_name, running_job_ids)
                    self.add_job_metric(cluster, cluster_domain, metrics, job_metrics, collect_time)

        return Response(metrics)

    @staticmethod
    def add_job_metric(cluster, cluster_domain, metrics, job_metrics, collect_time):
        """
        添加队列到容量指标
        :param cluster:
        :param cluster_domain:
        :param metrics:
        :param job_metrics:
        :param collect_time:
        :return:
        """
        for metric in job_metrics:
            job_metric = {
                "collect_time": collect_time,
                "cluster_id": cluster.cluster_id,
                "cluster_type": cluster.cluster_type,
                "cluster_name": cluster.cluster_name,
                "component_type": cluster.component_type,
                "geog_area_code": cluster.geog_area_code,
                "resource_type": cluster.resource_type,
                "service_type": cluster.service_type,
                "src_cluster_id": cluster.src_cluster_id,
                "resource_group_id": cluster.resource_group_id,
                "cluster_domain": cluster_domain,
            }
            job_metric.update(metric)
            metrics.append(job_metric)
