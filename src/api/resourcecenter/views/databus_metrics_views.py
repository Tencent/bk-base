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
from rest_framework.response import Response

from common.decorators import params_valid, detail_route
from common.views import APIViewSet
from resourcecenter.handlers import resource_service_config, resource_cluster_config
from resourcecenter.serializers.serializers import GetDatabusServiceClusterSerializer


class DatabusMetricsViewSet(APIViewSet):
    """
    @apiDefine databus_metrics
    总线指标API
    """

    lookup_field = "resource_group_id"

    @detail_route(methods=["get"], url_path="query_service_type")
    def query_service_type(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/databus_metrics/:resource_group_id/query_service_type/ 查询服务列表
        @apiName query_service_type
        @apiGroup databus_metrics
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

        query_params = {}
        query_params["resource_group_id"] = resource_group_id
        if "geog_area_code" in request.query_params and request.query_params["geog_area_code"]:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]

        service_list = resource_service_config.filter_list(resource_type="databus", active="1")
        service_type_dist = {}
        cluster_list = resource_cluster_config.filter_list(**query_params)
        for cluster in cluster_list:
            for svc in service_list:
                if cluster.service_type == svc.service_type:
                    service_type_dist[cluster.service_type] = {
                        "service_name": svc.service_name,
                        "service_type": cluster.service_type,
                    }

    @detail_route(methods=["get"], url_path="query_cluster")
    @params_valid(serializer=GetDatabusServiceClusterSerializer)
    def query_cluster(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/databus_metrics/:resource_group_id/query_cluster 查询集群列表
        @apiName query_cluster
        @apiGroup databus_metrics
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
        query_params = {"resource_group_id": resource_group_id, "service_type": request.query_params["service_type"]}
        if "geog_area_code" in request.query_params and request.query_params["geog_area_code"]:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]
        cluster_dist = {}
        cluster_list = resource_cluster_config.filter_list(**query_params)
        for cluster in cluster_list:
            if cluster.cluster_id != "-1" and cluster.cluster_id not in list(cluster_dist.keys()):
                cluster_id = cluster.cluster_id
                cluster_name = cluster.cluster_name if cluster.cluster_name is not None else cluster_id
                cluster_dist[cluster.service_type] = {"cluster_id": cluster_id, "cluster_name": cluster_name}
        return Response(list(cluster_dist.values()))
