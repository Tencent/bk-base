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

from rest_framework.response import Response

from django.conf import settings
from common.decorators import params_valid, detail_route, list_route
from common.views import APIViewSet
from resourcecenter.handlers import resource_service_config, resource_cluster_config
from resourcecenter.metric.storage_merric_driver import (
    get_resource_group_storage_summary,
    get_resource_group_disk_history,
)
from resourcecenter.serializers.serializers import StorageMetricsSerializer, GetStorageServiceClusterSerializer
from resourcecenter.serializers.storage_metrics_serializers import StorageMetricSummarySerializer
from resourcecenter.services.cluster_register_svc import none_to_zero
from resourcecenter.storekit.storekit_helper import StorekitHelper
from resourcecenter.utils.format_utils import get_history_last_number_val, covert_mb_size


class StorageMetricsViewSet(APIViewSet):
    """
    @apiDefine storage_metrics
    存储资源指标API
    """

    lookup_field = "resource_group_id"

    @detail_route(methods=["get"], url_path="query_service_type")
    def query_service_type(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/storage_metrics/:resource_group_id/query_service_type 查询服务列表
        @apiName query_service_type
        @apiGroup storage_metrics
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

        service_list = resource_service_config.filter_list(resource_type="storage", active="1")
        service_type_dist = {}
        cluster_list = resource_cluster_config.filter_list(**query_params)
        for cluster in cluster_list:
            for svc in service_list:
                if (
                    cluster.service_type == svc.service_type
                    and cluster.cluster_type in settings.RESOURCE_CENTER_COLLECT_STORAGE_TYPES
                ):
                    service_type_dist[cluster.service_type] = {
                        "service_name": svc.service_name,
                        "service_type": cluster.service_type,
                    }
        return Response(list(service_type_dist.values()))

    @detail_route(methods=["get"], url_path="query_cluster")
    @params_valid(serializer=GetStorageServiceClusterSerializer)
    def query_cluster(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/storage_metrics/:resource_group_id/query_cluster 查询集群列表
        @apiName query_cluster
        @apiGroup storage_metrics
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
            "resource_type": "storage",
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
                cluster_dist.append(
                    {
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                    }
                )
        return Response(cluster_dist)

    @detail_route(methods=["get"], url_path="summary")
    @params_valid(serializer=StorageMetricSummarySerializer)
    def summary(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/storage_metrics/:resource_group_id/summary/ 存储服务容量摘要
        @apiName summary
        @apiGroup storage_metrics
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
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        resource_group_i: "default_test",
                        group_name: "测试资源组",
                        group_type: "private",
                        geog_area_code: "inland",
                        resource_type: "processing",
                        service_type: "stream",
                        cluster_id: "all",
                        cluster_name: "all",
                        cpu: "100",
                        memory: "100",
                        gpu: "100",
                        disk: "100",
                        net: "100"
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
        data = get_resource_group_storage_summary(query_params)
        result = {
            "resource_group_id": resource_group_id,
            "geog_area_code": params.get("geog_area_code", ""),
            "total_mb": data.get("total_mb", 0),
            "used_mb": data.get("use_mb", 0),
        }
        return Response(result)

    @detail_route(methods=["get"], url_path="disk_history")
    @params_valid(serializer=StorageMetricsSerializer)
    def disk_history(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/storage_metrics/:resource_group_id/disk_history 存储容量指标
        @apiName disk_history
        @apiGroup storage_metrics
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
                                "available": 13918032,
                                "total": 50000000,
                                "used": 36081968,
                                "time": "2020-03-16 00:00:00"
                            },
                            {
                                "available": 42199414,
                                "total": 50000000,
                                "used": 7800586,
                                "time": "2020-03-16 01:00:00"
                            }
                        ]
                    },
                    "result": true
                }
        """
        params["resource_group_id"] = resource_group_id
        params["cluster_id"] = params.get("cluster_id", "")

        history = get_resource_group_disk_history(params)
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

    @list_route(methods=["get"], url_path="collect")
    def collect(self, request):
        """
        @api {get} /resourcecenter/storage_metrics/collect/ 存储指标采集
        @apiName collect
        @apiGroup storage_metrics
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
        start = request.query_params.get("start", int(time.time()) / 3600 * 3600)
        params = {"resource_type": "storage"}
        if "cluster_id" in request.query_params:
            params["cluster_id"] = request.query_params.get("cluster_id", "")
        if "cluster_type" in request.query_params:
            params["cluster_type"] = request.query_params.get("cluster_type", "")
        if "cluster_name" in request.query_params:
            params["cluster_name"] = request.query_params.get("cluster_name", "")

        capacities = {}
        # 从配置中获取可以采集到容量到存储类型，然后采集容量信息。
        for _type in settings.RESOURCE_CENTER_COLLECT_STORAGE_TYPES:
            capacities[_type] = StorekitHelper.get_cluster_type_capacity(_type)

        metrics = []
        cluster_list = resource_cluster_config.filter_list(**params)
        if cluster_list:
            for cluster in cluster_list:
                cluster_type = cluster.cluster_type
                capacity = {
                    "CapacityTotalMB": none_to_zero(cluster.disk),
                    "CapacityUsedMB": none_to_zero(cluster.disk) - none_to_zero(cluster.available_disk),
                }
                if cluster_type in capacities and capacities[cluster_type] is not None:
                    caps = capacities[cluster_type]
                    if caps and "series" in caps and caps["series"] and len(caps["series"]) > 0:
                        _time = 0
                        for se in caps["series"]:
                            if se["cluster_name"] == cluster.cluster_name and se["time"] > _time:
                                _time = se["time"]
                                capacity = se

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
                    "capacity": capacity,
                }
                metrics.append(metric)

        return Response(metrics)
