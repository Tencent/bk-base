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

from common.decorators import params_valid, list_route
from common.local import get_request_username
from common.views import APIViewSet
from resourcecenter.handlers import resource_unit_config, resource_service_config
from resourcecenter.serializers.serializers import (
    CreateResourceUnitConfigSerializer,
    ResourceServiceConfigSerializer,
    GetResourceServiceConfigSerializer,
)
from resourcecenter.services import resource_service_config_svc
from resourcecenter.error_code.errorcodes import get_codes


class ResourceUnitConfigViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_Unit
    资源套餐配置接口
    """

    lookup_field = "resource_unit_id"

    def list(self, request):
        """
        @api {get} /resourcecenter/resource_units/ 全部资源套餐单元配置
        @apiName list_resource_unit
        @apiGroup ResourceCenter_Unit
        @apiParam {string} [name] 配置单元名称
        @apiParam {string} [resource_type] 资源分类
        @apiParam {string} [service_type] 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParamExample {json} 参数样例:
            {
                "name": "64核128GB内存",
                "resource_type": "processing",
                "service_type": "stream",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "slot": 0.0,
                            "name": "64核128GB内存",
                            "created_at": "2020-02-20T11:32:36",
                            "resource_unit_id": 1,
                            "updated_at": "2020-02-20T11:32:36",
                            "created_by": "xx",
                            "active": 1,
                            "memory": 128.0,
                            "service_type": "stream",
                            "gpu": 0.0,
                            "net": 0.0,
                            "disk": 0.0,
                            "cpu": 64.0,
                            "resource_type": "processing",
                            "updated_by": null
                        },
                        {
                            "slot": 0.0,
                            "name": "64核128GB内存",
                            "created_at": "2020-02-20T11:37:39",
                            "resource_unit_id": 2,
                            "updated_at": "2020-02-20T11:37:39",
                            "created_by": "xx",
                            "active": 1,
                            "memory": 128.0,
                            "service_type": "stream",
                            "gpu": 0.0,
                            "net": 0.0,
                            "disk": 0.0,
                            "cpu": 64.0,
                            "resource_type": "processing",
                            "updated_by": null
                        },
                        {
                            "slot": 0.0,
                            "name": "64核128GB内存",
                            "created_at": "2020-02-20T11:38:32",
                            "resource_unit_id": 3,
                            "updated_at": "2020-02-20T11:38:32",
                            "created_by": "xx",
                            "active": 1,
                            "memory": 128.0,
                            "service_type": "batch",
                            "gpu": 0.0,
                            "net": 0.0,
                            "disk": 0.0,
                            "cpu": 64.0,
                            "resource_type": "processing",
                            "updated_by": "xx"
                        }
                    ],
                    "result": true
                }
        """

        query_params = {}
        # TODO：查询参数处理
        if "name" in request.query_params:
            query_params["name__icontains"] = request.query_params["name"]
        if "resource_type" in request.query_params:
            query_params["resource_type"] = request.query_params["resource_type"]
        if "service_type" in request.query_params:
            query_params["service_type"] = request.query_params["service_type"]
        query_list = resource_unit_config.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=CreateResourceUnitConfigSerializer)
    def create(self, request, params):
        """
        @api {post} /resourcecenter/resource_units/ 创建资源套餐单元配置
        @apiName create_resource_unit
        @apiGroup ResourceCenter_Unit
        @apiParam {string} bk_username 提交人
        @apiParam {string} name 配置单元名称
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} unit_config 一个单元的资源配额，如：{"CPU": 10.0, "memory": 4096.0, "disk": 102400.0, "gpu": 0.0}
        @apiParam {string} active 是否生效
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "name": "64核128GB内存",
                "resource_type": "processing",
                "service_type": "stream",
                "cpu": 64.0,
                "memory": 128.0,
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_unit_id": 10086
                    },
                    "result": true
                }
        """
        ret = resource_unit_config.save(
            name=params["name"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            cpu=params["cpu"] if "cpu" in params else 0.0,
            memory=params["memory"] if "memory" in params else 0.0,
            gpu=params["gpu"] if "gpu" in params else 0.0,
            disk=params["disk"] if "disk" in params else 0.0,
            net=params["net"] if "net" in params else 0.0,
            slot=params["slot"] if "slot" in params else 0.0,
            active=params["active"],
            created_by=get_request_username(),
        )
        return Response({"resource_unit_id": ret.resource_unit_id})

    @params_valid(serializer=CreateResourceUnitConfigSerializer)
    def update(self, request, resource_unit_id, params):
        """
        @api {put} /resourcecenter/resource_units/:resource_unit_id/ 更新资源套餐单元配置
        @apiName update_resource_unit
        @apiGroup ResourceCenter_Unit
        @apiParam {string} unit_id 资源套餐单元ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} name 配置单元名称
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} unit_config 一个单元的资源配额，如：{"CPU": 10, "memory": 4096, "disk": 102400, "gpu": 0}
        @apiParam {string} active 是否生效
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "name": "64核128GB内存",
                "resource_type": "processing",
                "service_type": "batch",
                "cpu": 64.0,
                "memory": 128.0,
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_unit_id": "10086"
                    },
                    "result": true
                }
        """
        resource_unit_config.update(
            resource_unit_id,
            name=params["name"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            cpu=params["cpu"] if "cpu" in params else 0.0,
            memory=params["memory"] if "memory" in params else 0.0,
            gpu=params["gpu"] if "gpu" in params else 0.0,
            disk=params["disk"] if "disk" in params else 0.0,
            net=params["net"] if "net" in params else 0.0,
            slot=params["slot"] if "slot" in params else 0.0,
            active=params["active"],
            updated_by=get_request_username(),
        )
        return Response({"resource_unit_id": resource_unit_id})

    def retrieve(self, request, resource_unit_id):
        """
        @api {get} /resourcecenter/resource_units/:resource_unit_id/ 查看资源套餐单元配置
        @apiName view_resource_unit
        @apiGroup ResourceCenter_Unit
        @apiParam {string} unit_id 资源套餐单元ID
        @apiParamExample {json} 参数样例:
            {
              "resource_unit_id": "10086"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "slot": 0.0,
                        "name": "64核128GB内存",
                        "created_at": "2020-02-20T11:38:32",
                        "resource_unit_id": 3,
                        "updated_at": "2020-02-20T11:38:32",
                        "created_by": "xx",
                        "active": 1,
                        "memory": 128.0,
                        "service_type": "batch",
                        "gpu": 0.0,
                        "net": 0.0,
                        "disk": 0.0,
                        "cpu": 64.0,
                        "resource_type": "processing",
                        "updated_by": "xx"
                    },
                    "result": true
                }
        """
        obj = resource_unit_config.get(resource_unit_id)
        return Response(model_to_dict(obj))


class ResourceServiceConfigViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_ServiceConfig
    资源服务类型配置接口
    """

    lookup_field = "service_type"

    def list(self, request):
        """
        @api {get} /resourcecenter/service_configs/ 全部资源服务类型配置
        @apiName list_resource_service_configs
        @apiGroup ResourceCenter_ServiceConfig
        @apiParam {string} bk_username 提交人
        @apiParam {string} [resource_type] 资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））
        @apiParam {string} [service_type] 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} [service_name] 服务名称
        @apiParam {string} [active] 是否生效
        @apiParamExample {json} 参数样例:
            {
                "resource_type": "processing",
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "updated_by": null,
                            "service_name": "离线计算",
                            "created_at": "2020-02-20T13:33:37",
                            "updated_at": null,
                            "created_by": "xx",
                            "active": 1,
                            "service_type": "batch",
                            "resource_type": "processing"
                        },
                        {
                            "updated_by": "xx",
                            "service_name": "实时计算1",
                            "created_at": "2020-02-20T13:18:44",
                            "updated_at": null,
                            "created_by": "xx",
                            "active": 1,
                            "service_type": "stream",
                            "resource_type": "processing"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        # TODO：查询参数处理
        if "service_name" in request.query_params:
            query_params["service_name__icontains"] = request.query_params["service_name"]
        if "resource_type" in request.query_params:
            query_params["resource_type"] = request.query_params["resource_type"]
        if "service_type" in request.query_params:
            query_params["service_type"] = request.query_params["service_type"]
        if "active" in request.query_params:
            query_params["active"] = request.query_params["active"]
        query_list = resource_service_config.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=ResourceServiceConfigSerializer)
    def create(self, request, params):
        """
        @api {post} /resourcecenter/service_configs/ 创建资源服务类型配置
        @apiName create_resource_service_config
        @apiGroup ResourceCenter_ServiceConfig
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_type 资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} service_name 服务名称
        @apiParam {string} active 是否生效
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_type": "processing",
                "service_type": "stream",
                "service_name": "实时计算",
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_type": "processing",
                            "service_type": "stream"
                    },
                    "result": true
                }
        """
        resource_service_config.save(
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            service_name=params["service_name"],
            active=params["active"],
            created_by=get_request_username(),
        )
        return Response({"resource_type": params["resource_type"], "service_type": params["service_type"]})

    @params_valid(serializer=ResourceServiceConfigSerializer)
    def update(self, request, service_type, params):
        """
        @api {put} /resourcecenter/service_configs/:service_type/ 更新资源服务类型配置
        @apiName update_resource_service_config
        @apiGroup ResourceCenter_ServiceConfig
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_type 资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} service_name 服务名称
        @apiParam {string} active 是否生效
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_type": "processing",
                "service_type": "stream",
                "service_name": "实时计算1",
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_type": "processing",
                            "service_type": "stream"
                    },
                    "result": true
                }
        """
        resource_service_config.update(
            params["resource_type"],
            service_type,
            service_name=params["service_name"],
            active=params["active"],
            updated_by=get_request_username(),
        )
        return Response({"resource_type": params["resource_type"], "service_type": service_type})

    @params_valid(serializer=GetResourceServiceConfigSerializer)
    def retrieve(self, request, service_type, params):
        """
        @api {get} /resourcecenter/service_configs/:service_type/ 查看资源服务类型配置
        @apiName view_resource_service_config
        @apiGroup ResourceCenter_ServiceConfig
        @apiParam {string} resource_type 资源分类
        @apiParamExample {json} 参数样例:
            {
              "resource_type": "processing"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "updated_by": "xx",
                        "service_name": "实时计算1",
                        "created_at": "2020-02-20T13:18:44",
                        "updated_at": null,
                        "created_by": "xx",
                        "active": 1,
                        "service_type": "stream",
                        "resource_type": "processing"
                    },
                    "result": true
                }
        """
        resource_type = params["resource_type"]
        obj = resource_service_config.get(resource_type, service_type)
        return Response(model_to_dict(obj))

    @list_route(methods=["get"], url_path="sync_storage_cluster_type")
    def sync_storage_cluster_type(self, request):
        """
        @api {get} /resourcecenter/service_configs/sync_storage_cluster_type/ 同步存储集群类型
        @apiDescription 手动同步存储类型使用、目的是初始化数据（不使用也没有问题）。
        @apiName sync_cluster_type
        @apiGroup ResourceCenter_ServiceConfig
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        resource_service_config_svc.sync_cluster_type_to_service_config()
        return Response()


class ErrorCodeViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} /resourcecenter/errorcodes/ 列出资源系统api所有error code
        @apiName list_error_codes
        @apiGroup ResourceCenter_ErrorCode
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [[
                        {
                        message: "参数异常",
                        code: "1584000",
                        name: "ILLEGAL_ARGUMENT_ERR",
                        solution: "-",
                        }]
                    "result": true
                }
        """
        return Response(get_codes())
