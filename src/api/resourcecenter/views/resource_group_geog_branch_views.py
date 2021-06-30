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
from django.forms import model_to_dict
from rest_framework.response import Response

from common.decorators import params_valid
from common.local import get_request_username
from common.views import APIViewSet
from resourcecenter.handlers import resource_geog_area_cluster_group
from resourcecenter.serializers.serializers import (
    ResourceGroupGeogBranchSerializer,
    GetResourceGroupGeogBranchSerializer,
)


class ResourceGroupGeogBranchViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_ResourceGroupGeog
    资源组区域集群组接口
    """

    lookup_field = "resource_group_id"

    def list(self, request):
        """
        @api {get} /resourcecenter/resource_geog_area_cluster_group/ 全部资源组区域集群组
        @apiName list_resource_geog_area_cluster_group_branchs
        @apiGroup ResourceCenter_ResourceGroupGeog
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [geog_area_code] 地区
        @apiParam {string} [cluster_group] 集群组
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "geog_area_code": "inland",
                            "resource_group_id": "default_test1",
                            "updated_by": "xx",
                            "created_at": null,
                            "updated_at": "2020-02-28T17:17:51",
                            "created_by": "xx",
                            "cluster_group": "default_test1-inland"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        # TODO：查询参数处理
        if "resource_group_id" in request.query_params:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "geog_area_code" in request.query_params:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]
        if "cluster_group" in request.query_params:
            query_params["cluster_group"] = request.query_params["cluster_group"]

        query_list = resource_geog_area_cluster_group.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=ResourceGroupGeogBranchSerializer)
    def create(self, request, params):
        """
        @api {post} /resourcecenter/resource_geog_area_cluster_group/ 创建资源组区域集群组
        @apiName create_resource_geog_area_cluster_group
        @apiGroup ResourceCenter_ResourceGroupGeog
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_group_id 资源组
        @apiParam {string} geog_area_code 区域
        @apiParam {string} cluster_group 集群组
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_group_id": "default_test1",
                "geog_area_code": "inland",
                "cluster_group": "default_test1-inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "geog_area_code": "inland",
                        "resource_group_id": "default_test1"
                    },
                    "result": true
                }
        """
        resource_geog_area_cluster_group.save(
            resource_group_id=params["resource_group_id"],
            geog_area_code=params["geog_area_code"],
            cluster_group=params["cluster_group"],
            created_by=get_request_username(),
        )
        return Response({"resource_group_id": params["resource_group_id"], "geog_area_code": params["geog_area_code"]})

    @params_valid(serializer=ResourceGroupGeogBranchSerializer)
    def update(self, request, resource_group_id, params):
        """
        @api {put} /resourcecenter/resource_geog_area_cluster_group/:resource_group_id/ 更新资源组区域集群组
        @apiName create_resource_geog_area_cluster_group
        @apiGroup ResourceCenter_ResourceGroupGeog
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_group_id 资源组
        @apiParam {string} geog_area_code 区域
        @apiParam {string} cluster_group 集群组
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_group_id": "default_test1",
                "geog_area_code": "inland",
                "cluster_group": "default_test1-inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "geog_area_code": "inland",
                        "resource_group_id": "default_test1"
                    },
                    "result": true
                }
        """
        resource_geog_area_cluster_group.update(
            resource_group_id,
            params["geog_area_code"],
            cluster_group=params["cluster_group"],
            updated_by=get_request_username(),
        )
        return Response({"resource_group_id": resource_group_id, "geog_area_code": params["geog_area_code"]})

    @params_valid(serializer=GetResourceGroupGeogBranchSerializer)
    def retrieve(self, request, resource_group_id, params):
        """
        @api {get} /resourcecenter/resource_geog_area_cluster_group/:resource_group_id/ 查看资源组区域集群组
        @apiName view_resource_geog_area_cluster_group
        @apiGroup ResourceCenter_ResourceGroupGeog
        @apiParam {string} geog_area_code 区域
        @apiParamExample {json} 参数样例:
            {
              "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "geog_area_code": "inland",
                        "resource_group_id": "default_test1",
                        "updated_by": "xx",
                        "created_at": null,
                        "updated_at": "2020-02-28T17:17:51",
                        "created_by": "xx",
                        "cluster_group": "default_test1-inland"
                    },
                    "result": true
                }
        """
        geog_area_code = params["geog_area_code"]
        obj = resource_geog_area_cluster_group.get(resource_group_id, geog_area_code)
        return Response(model_to_dict(obj))
