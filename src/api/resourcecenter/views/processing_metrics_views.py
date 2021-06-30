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
from django.conf import settings
from common.decorators import params_valid, detail_route, list_route
from common.views import APIViewSet
from resourcecenter.handlers import resource_service_config, resource_cluster_config, resource_geog_area_cluster_group
from resourcecenter.metric.processing_merric_driver import (
    get_resource_group_summary,
    get_resource_group_cpu_history,
    get_resource_group_memory_history,
    get_resource_group_apps_history,
)
from resourcecenter.serializers.processing_metrics_serializers import ProcessingMetricSummarySerializer
from resourcecenter.serializers.serializers import ProcessingMetricsSerializer, GetProcessingServiceClusterSerializer
from resourcecenter.services.cluster_register_svc import none_to_zero
from resourcecenter.utils.format_utils import get_history_last_number_val, covert_number_size, covert_mb_size
from resourcecenter.utils.yarn_rest_utils import get_yarn_scheduler_metric
from resourcecenter.utils.k8s_rest_utils import get_k8s_cluster_metric


class ProcessingMetricsViewSet(APIViewSet):
    """
    @apiDefine processing_metrics
    计算资源指标API
    """

    lookup_field = "resource_group_id"

    @detail_route(methods=["get"], url_path="query_service_type")
    def query_service_type(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/query_service_type 查询服务列表
        @apiName query_service_type
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} [geog_area_code] 地区
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test_00"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "service_type": "stream",
                            "service_name": "实时计算"
                        }
                    ],
                    "result": true
                }
        """

        query_params = {"resource_group_id": resource_group_id}
        if "geog_area_code" in request.query_params and request.query_params["geog_area_code"]:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]

        service_list = resource_service_config.filter_list(resource_type="processing", active="1")
        service_type_dist = {}
        cluster_list = resource_cluster_config.filter_list(**query_params)
        for cluster in cluster_list:
            for svc in service_list:
                if cluster.service_type == svc.service_type:
                    service_type_dist[cluster.service_type] = {
                        "service_name": svc.service_name,
                        "service_type": cluster.service_type,
                    }
        return Response(list(service_type_dist.values()))

    @detail_route(methods=["get"], url_path="query_cluster")
    @params_valid(serializer=GetProcessingServiceClusterSerializer)
    def query_cluster(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/query_cluster 查询集群列表
        @apiName query_cluster
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} service_type 服务类型
        @apiParam {string} [geog_area_code] 地区
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test_00",
              "service_type": "stream"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "cluster_id": "1",
                            "cluster_name": "深圳-yarn-01"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {
            "resource_type": "processing",
            "resource_group_id": resource_group_id,
            "service_type": request.query_params["service_type"],
        }
        if "geog_area_code" in request.query_params and request.query_params["geog_area_code"]:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]
        cluster_dist = []
        cluster_list = resource_cluster_config.filter_list(**query_params)
        for cluster in cluster_list:
            if cluster.cluster_id != "-1":
                cluster_id = cluster.cluster_id
                cluster_name = cluster.cluster_name if cluster.cluster_name is not None else cluster_id
                cluster_dist.append({"cluster_id": cluster_id, "cluster_name": cluster_name})
        return Response(cluster_dist)

    @detail_route(methods=["get"], url_path="summary")
    @params_valid(serializer=ProcessingMetricSummarySerializer)
    def summary(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/summary 计算服务容量摘要
        @apiName summary
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} start_time 开始日期
        @apiParam {string} end_time 结束日期
        @apiParam {string} [geog_area_code] 地区
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test",
              "geog_area_code": "inland",
              "start_time": "2020-04-28 00:00:00",
              "end_time": "2020-04-28 23:59:59",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "geog_area_code": "",
                        "resource_group_id": "default",
                        "cpu": 400225.2170067716,
                        "memory": 1024215038.0
                    },
                    "result": true
                }
        """
        query_params = {
            "start_time": params.get("start_time", ""),
            "end_time": params.get("end_time", ""),
            "resource_group_id": resource_group_id,
            "geog_area_code": params.get("geog_area_code", ""),
        }
        # print query_params
        data = get_resource_group_summary(query_params)
        result = {
            "resource_group_id": resource_group_id,
            "geog_area_code": params.get("geog_area_code", ""),
            "cpu": data.get("cpu", 0),
            "memory": data.get("memory", 0),
        }
        return Response(result)

    @detail_route(methods=["get"], url_path="cpu_history")
    @params_valid(serializer=ProcessingMetricsSerializer)
    def cpu_history(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/cpu_history CPU指标
        @apiName cpu_history
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} geog_area_code 地区
        @apiParam {string} service_type 服务类型
        @apiParam {string} start_time 开始时间
        @apiParam {string} end_time 结束时间
        @apiParam {string} time_unit 时间类型（hour:小时，day:天）
        @apiParam {string} [cluster_id=all] 集群ID
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test",
              "geog_area_code": "inland",
              "service_type": "stream",
              "start_time": "2020-03-16 00:00:00",
              "end_time": "2020-03-16 12:00:00",
              "time_unit": "hour",
              "cluster_id": "all"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "interval": 3600,
                        "history": [
                            {
                                "available": 36.0,
                                "resource_group_id": "default",
                                "total": 60.0,
                                "used": 24.0,
                                "time": "2020-04-28 00:00:00"
                            },
                            {
                                "available": 36.0,
                                "resource_group_id": "default",
                                "total": 60.0,
                                "used": 24.0,
                                "time": "2020-04-28 00:05:00"
                            }
                        ]
                    },
                    "result": true
                }
        """
        params["resource_group_id"] = resource_group_id
        params["cluster_id"] = params.get("cluster_id", "")
        history = get_resource_group_cpu_history(params)
        last_used = get_history_last_number_val(history, "used")
        last_total = get_history_last_number_val(history, "total")
        result = {
            "interval": params.get("interval", 3600),
            "history": history,
            "last": {
                "rate": format(last_used * 100 / last_total if last_total > 0 else 0.00, "0.2f"),
                "used": covert_number_size(last_used),
                "total": covert_number_size(last_total),
            },
        }
        return Response(result)

    @detail_route(methods=["get"], url_path="memory_history")
    @params_valid(serializer=ProcessingMetricsSerializer)
    def memory_history(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/memory_history/ 内存指标
        @apiName memory_history
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} geog_area_code 地区
        @apiParam {string} service_type 服务类型
        @apiParam {string} [cluster_id=all] 集群ID
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
                    "data": {
                        "interval": 3600,
                        "history": [
                            {
                                "available": 36864.0,
                                "resource_group_id": "default",
                                "total": 61440.0,
                                "used": 24576.0,
                                "time": "2020-04-28 00:00:00"
                            },
                            {
                                "available": 36864.0,
                                "resource_group_id": "default",
                                "total": 61440.0,
                                "used": 24576.0,
                                "time": "2020-04-28 00:05:00"
                            },
                        ]
                    },
                    "result": true
                }
        """
        params["resource_group_id"] = resource_group_id
        params["cluster_id"] = params.get("cluster_id", "")
        history = get_resource_group_memory_history(params)
        last_used = get_history_last_number_val(history, "used")
        last_total = get_history_last_number_val(history, "total")
        result = {
            "interval": params.get("interval", 3600),
            "history": history,
            "last": {
                "rate": format(last_used * 100 / last_total if last_total > 0 else 0.00, "0.2f"),
                "used": covert_mb_size(last_used),
                "total": covert_mb_size(last_total),
            },
        }
        return Response(result)

    @detail_route(methods=["get"], url_path="app_history")
    @params_valid(serializer=ProcessingMetricsSerializer)
    def app_history(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/processing_metrics/:resource_group_id/app_history/ app指标
        @apiName app_history
        @apiGroup processing_metrics
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} geog_area_code 地区
        @apiParam {string} service_type 服务类型
        @apiParam {string} [cluster_id=all] 集群ID
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
                    "data": {
                        "interval": 3600,
                        "history": [
                            {
                                "available": 0.0,
                                "resource_group_id": "default",
                                "total": 10.0,
                                "used": 10.0,
                                "time": "2020-04-28 00:00:00"
                            },
                            {
                                "available": 0.0,
                                "resource_group_id": "default",
                                "total": 10.0,
                                "used": 10.0,
                                "time": "2020-04-28 00:05:00"
                            }
                        ]
                    },
                    "result": true
                }
        """
        params["resource_group_id"] = resource_group_id
        params["cluster_id"] = params.get("cluster_id", "")
        history = get_resource_group_apps_history(params)
        last_used = get_history_last_number_val(history, "used")
        last_total = get_history_last_number_val(history, "total")
        result = {
            "interval": params.get("interval", 3600),
            "history": history,
            "last": {
                "rate": format(last_used * 100 / last_total if last_total > 0 else 0.00, "0.2f"),
                "used": covert_number_size(last_used),
                "total": covert_number_size(last_total),
            },
        }
        return Response(result)

    @list_route(methods=["get"], url_path="collect")
    def collect(self, request):
        """
        @api {get} /resourcecenter/processing_metrics/collect/ 计算指标采集
        @apiName collect
        @apiGroup processing_metrics
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
                            "cluster_name": "tspider-test11",
                            "geog_area_code": "inland",
                            "capacity": {
                                "cluster_name": "tspider-test11",
                                "table_cnt": 734,
                                "CapacityUsedMB": 91935,
                                "cluster_type": "tspider",
                                "CapacityTotalMB": 778240,
                                "Priority": 89,
                                "MaxFileHandlers": 204800,
                                "UsedFileHandlers": 5179,
                                "time": 1587398400
                            },
                            "cluster_id": "1",
                            "resource_type": "storage",
                            "component_type": "tspider",
                            "service_type": "tspider",
                            "cluster_type": "tspider",
                            "src_cluster_id": "113",
                            "collect_time": 1587466800
                        },
                        {
                            "cluster_name": "hdfs-testOnline",
                            "geog_area_code": "inland",
                            "capacity": {
                                "CapacityUsedMB": 0.0,
                                "CapacityTotalMB": 1048576000.0
                            },
                            "cluster_id": "bb40d523440eaf683a77757e1b5dc8d5",
                            "resource_type": "storage",
                            "component_type": "hdfs",
                            "service_type": "hdfs",
                            "cluster_type": "hdfs",
                            "src_cluster_id": "452",
                            "collect_time": 1587466800
                        }
                    ],
                    "result": true
                }
        """
        # 时间精确到分钟
        start = request.query_params.get("start", int(time.time()) / 60 * 60)
        params = {"resource_type": "processing"}
        if "cluster_id" in request.query_params:
            params["cluster_id"] = request.query_params.get("cluster_id", "")
        if "cluster_type" in request.query_params:
            params["cluster_type"] = request.query_params.get("cluster_type", "")
        if "cluster_name" in request.query_params:
            params["cluster_name"] = request.query_params.get("cluster_name", "")

        yarn_scheduler_metric_cache = {}
        metrics = []
        cluster_list = resource_cluster_config.filter_list(**params)
        all_cluster_group = resource_geog_area_cluster_group.filter_list()
        if cluster_list:
            for cluster in cluster_list:
                cluster_type = cluster.cluster_type
                cluster_group_id = None
                for cluster_group in all_cluster_group:
                    if (
                        cluster_group.resource_group_id == cluster.resource_group_id
                        and cluster_group.geog_area_code == cluster.geog_area_code
                    ):
                        cluster_group_id = cluster_group.cluster_group
                        break

                is_collected = False
                queue_type = self._get_queue_type(cluster_type)
                if queue_type == settings.YARN_QUEUE_TYPE:
                    queue_name = self._get_queue_name(cluster_group_id, cluster_type)
                    if queue_name is not None:
                        # 不是已知的集群类型，队列规则没有确定，无法采集。
                        yarn_metric = self.get_and_add_cache_yarn_metric(cluster, yarn_scheduler_metric_cache)
                        for cluster_domain, queues in list(yarn_metric.items()):
                            # 可能多个yarn物理集群，分开记录，通过后续计算进行汇总。
                            for queue in queues:
                                if queue_name == queue["queue_name"]:
                                    self.add_queue_capacity(cluster, cluster_domain, metrics, queue, start)
                                    is_collected = True
                                    break
                elif queue_type == settings.K8S_QUEUE_TYPE:
                    # connection_info example: {"cluster_domain": "https://k8s-cluster-domain", "token": "foo"}
                    connection_info = json.loads(cluster.connection_info)
                    cluster_domain = connection_info["cluster_domain"]
                    # k8s api access token
                    token = connection_info["token"]
                    k8s_metric = get_k8s_cluster_metric(cluster_domain, token, cluster.cluster_name)
                    self.add_queue_capacity(cluster, cluster_domain, metrics, k8s_metric, start)
                    is_collected = True
                # 未能采集到添加配置到容量
                if is_collected is False:
                    self.add_default_set_capacity(cluster, metrics, start)

        return Response(metrics)

    def get_and_add_cache_yarn_metric(self, cluster, yarn_scheduler_metric_cache):
        """
        获取yarn的metric，按geog_area_code进行缓存。
        每个地区对应的有一个或者多个yarn的物理集群，cluster_domain会不一样。
        :param cluster:
        :param yarn_scheduler_metric_cache:
        :return:
        """
        yarn_metric = yarn_scheduler_metric_cache.get(cluster.geog_area_code, None)
        if yarn_metric is None:
            yarn_metric = get_yarn_scheduler_metric(cluster.geog_area_code)
            yarn_scheduler_metric_cache[cluster.geog_area_code] = yarn_metric
        return yarn_metric

    def add_queue_capacity(self, cluster, cluster_domain, metrics, queue_metric, start):
        """
        添加队列到容量指标
        :param cluster:
        :param cluster_domain:
        :param metrics:
        :param queue_metric:
        :param start:
        :return:
        """
        metric = {
            "collect_time": start,
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
            "capacity": queue_metric,
        }
        metrics.append(metric)

    def add_default_set_capacity(self, cluster, metrics, start):
        """
        添加设置到容量指标，不能采集到到时候使用
        :param cluster:
        :param metrics:
        :param start:
        :return:
        """
        metric = {
            "collect_time": start,
            "cluster_id": cluster.cluster_id,
            "cluster_type": cluster.cluster_type,
            "cluster_name": cluster.cluster_name,
            "component_type": cluster.component_type,
            "geog_area_code": cluster.geog_area_code,
            "resource_type": cluster.resource_type,
            "service_type": cluster.service_type,
            "src_cluster_id": cluster.src_cluster_id,
            "resource_group_id": cluster.resource_group_id,
            "cluster_domain": "",
            "capacity": {
                "total_memory": none_to_zero(cluster.memory),
                "total_vcores": none_to_zero(cluster.cpu),
            },
        }
        metrics.append(metric)

    def _get_queue_name(self, cluster_group_id, cluster_type):
        """
        获取队列名称
        新增加集群类型，需要在配置文件添加配置模板。
        :param cluster_group_id:
        :param cluster_type:
        :return:
        """
        queue_name = None
        if cluster_group_id:
            # 判断集群类型是否可以采集
            if cluster_type in settings.RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES:
                # 获取队列模板
                queue_template = settings.PROCESSING_CLUSTER_QUEUE_TEMPLATE.get(cluster_type, None)
                if queue_template:
                    queue_name = queue_template.format(cluster_group_id=cluster_group_id)

        return queue_name

    def _get_queue_type(self, cluster_type):
        """
        获取队列集群类型：yarn/k8s
        新增加集群类型，需要在配置文件添加配置模板。
        :param cluster_type:
        :return: yarn/k8s
        """
        queue_type = None
        if cluster_type in settings.RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES:
            # 获取队列模板
            queue_type = settings.PROCESSING_CLUSTER_QUEUE_TYPE.get(cluster_type, None)

        return queue_type
