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
from django.db import transaction
from django.forms import model_to_dict
from rest_framework.response import Response
from django.utils.translation import ugettext as _

from common.decorators import params_valid, list_route
from common.local import get_request_username
from common.views import APIViewSet
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException
from resourcecenter.handlers import resource_cluster_config
from resourcecenter.serializers.serializers import (
    CreateClusterRegisterSerializer,
    UpdateClusterRegisterCapacitySerializer,
    UpdateClusterRegisterSerializer,
    GetClusterRegisterSerializer,
)
from resourcecenter.services import cluster_register_svc, resource_service_config_svc


class ResourceRegisterViewSet(APIViewSet):
    """
    @apiDefine cluster_register
    集群注册到资源系统
    """

    @list_route(methods=["post"], url_path="create_cluster")
    @params_valid(serializer=CreateClusterRegisterSerializer)
    @transaction.atomic(using="bkdata_basic")
    def create_cluster(self, request, params):
        """
        @api {post} /resourcecenter/cluster_register/create_cluster/ 集群注册到资源系统
        @apiDescription 开发或维护人员主动注册集群信息，
                        注意：geog_area_code, resource_type、service_type、cluster_type, src_cluster_id 需保证唯一。
        @apiName create_cluster
        @apiGroup cluster_register
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} src_cluster_id 集群ID（模块的集群ID，非资源系统统一的集群ID）
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} cluster_name 集群名称
        @apiParam {string} component_type 组件类型
        @apiParam {string} geog_area_code 区域
        @apiParam {string} [resource_group_id] 资源组ID（资源组ID和集群组必须填一个）
        @apiParam {string} [cluster_group] 集群组（资源组ID和集群组必须填一个）
        @apiParam {string} [splitable] 是否可以拆分分配
        @apiParam {double} [cpu] CPU数量，单位core
        @apiParam {double} [memory] 内存数据量，单位MB
        @apiParam {double} [disk] 存储容量，单位MB
        @apiParam {double} [net] 网络带宽，单位bit
        @apiParam {double} [slot] 处理槽位，单位个
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_type": "storage",
                "service_type": "hdfs",
                "src_cluster_id": "452",
                "cluster_type": "hdfs",
                "cluster_name": "hdfs-testOnline",
                "component_type"： "hdfs",
                "geog_area_code": "inland",
                "resource_group_id": "default",
                "disk": 104857600.0
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "bb40d523440eaf683a77757e1b5dc8d5"
                    },
                    "result": true
                }
        """
        params["update_type"] = "full"
        # 校验是否存储在
        cluster_config_exists = resource_cluster_config.filter_list(
            src_cluster_id=params["src_cluster_id"],
            cluster_type=params["cluster_type"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            geog_area_code=params["geog_area_code"],
        ).exists()
        if cluster_config_exists:
            raise ResourceCenterException(message=_("该集群信息已经存在。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX)
        cluster_info = cluster_register_svc.cluster_register(params)
        return Response(cluster_info)

    @list_route(methods=["put"], url_path="update_cluster")
    @params_valid(serializer=UpdateClusterRegisterSerializer)
    @transaction.atomic(using="bkdata_basic")
    def update_cluster(self, request, params):
        """
        @api {put} /resourcecenter/cluster_register/update_cluster/ 更新集群在资源系统信息
        @apiDescription 开发或维护人员主动更新集群信息，
                        注意：geog_area_code, resource_type、service_type、cluster_type, src_cluster_id 需保证唯一。
        @apiName update_cluster
        @apiGroup cluster_register
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} src_cluster_id 集群ID（模块的集群ID，非资源系统统一的集群ID）
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} cluster_name 集群名称
        @apiParam {string} component_type 组件类型
        @apiParam {string} geog_area_code 区域
        @apiParam {string} [resource_group_id] 资源组ID（资源组ID和集群组必须填一个）
        @apiParam {string} [cluster_group] 集群组（资源组ID和集群组必须填一个）
        @apiParam {string} update_type 资源更新类型(full:全量更新，incremental：增量）
        @apiParam {string} [splitable] 是否可以拆分分配
        @apiParam {double} [cpu] CPU数量，单位core
        @apiParam {double} [memory] 内存数据量，单位MB
        @apiParam {double} [gpu] GPU数量，单位core
        @apiParam {double} [disk] 存储容量，单位MB
        @apiParam {double} [net] 网络带宽，单位bit
        @apiParam {double} [slot] 处理槽位，单位个
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_type": "storage",
                "service_type": "hdfs",
                "src_cluster_id": "452",
                "cluster_type": "hdfs",
                "cluster_name": "hdfs-testOnline",
                "component_type"： "hdfs",
                "geog_area_code": "inland",
                "resource_group_id": "default",
                "disk": 104857600.0
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "bb40d523440eaf683a77757e1b5dc8d5"
                    },
                    "result": true
                }
        """
        if params["resource_type"] == "storage":
            resource_service_config_svc.add_service_type_for_storage(service_type=params["resource_type"])
        cluster_info = cluster_register_svc.cluster_register(params)
        return Response(cluster_info)

    @list_route(methods=["put"], url_path="update_capacity")
    @params_valid(UpdateClusterRegisterCapacitySerializer)
    @transaction.atomic(using="bkdata_basic")
    def update_capacity(self, request, params):
        """
        @api {put} /resourcecenter/cluster_register/update_capacity/ 更新集群在资源系统容量
        @apiDescription 开发或维护人员主动修改集群容量信息，
                        注意：geog_area_code, resource_type、service_type、cluster_type, src_cluster_id 需保证唯一。
        @apiName update_capacity
        @apiGroup cluster_register
        @apiParam {string} bk_username 提交人
        @apiParam {string} geog_area_code 区域
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} src_cluster_id 集群ID（模块的集群ID，非资源系统统一的集群ID）
        @apiParam {string} update_type 资源更新类型(full:全量更新，incremental：增量）
        @apiParam {double} [cpu] CPU数量，单位core
        @apiParam {double} [memory] 内存数据量，单位MB
        @apiParam {double} [gpu] GPU数量，单位core
        @apiParam {double} [disk] 存储容量，单位MB
        @apiParam {double} [net] 网络带宽，单位bit
        @apiParam {double} [slot] 处理槽位，单位个
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_type": "storage",
                "service_type": "hdfs",
                "src_cluster_id": "452",
                "cluster_type": "hdfs",
                "cluster_name": "hdfs-testOnline",
                "component_type"： "hdfs",
                "geog_area_code": "inland",
                "resource_group_id": "default",
                "disk": 104857600.0
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "bb40d523440eaf683a77757e1b5dc8d5"
                    },
                    "result": true
                }
        """
        if params["resource_type"] == "storage":
            resource_service_config_svc.add_service_type_for_storage(service_type=params["resource_type"])

        cluster_config = resource_cluster_config.filter_list(
            src_cluster_id=params["src_cluster_id"],
            cluster_type=params["cluster_type"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            geog_area_code=params["geog_area_code"],
        ).first()
        if cluster_config is None:
            raise ResourceCenterException(message=_("该集群配置不存在。"), code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX)

        cluster_register_svc.make_default_capacity(params)

        cluster_id = cluster_config.cluster_id
        resource_cluster_config.update(
            cluster_id,
            cpu=params["cpu"],
            memory=params["memory"],
            gpu=params["gpu"],
            disk=params["disk"],
            net=params["net"],
            slot=params["slot"],
            available_cpu=params["cpu"],
            available_memory=params["memory"],
            available_gpu=params["gpu"],
            available_disk=params["disk"],
            available_net=params["net"],
            available_slot=params["slot"],
            updated_by=get_request_username(),
        )

        return Response({"cluster_id": cluster_id})

    @list_route(methods=["get"], url_path="get_cluster")
    @params_valid(GetClusterRegisterSerializer)
    def get_cluster(self, request, params):
        """
        @api {get} /resourcecenter/cluster_register/get_cluster/ 查看集群在资源系统信息
        @apiName view_cluster
        @apiGroup cluster_register
        @apiParam {string} geog_area_code 区域
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} src_cluster_id 集群ID（模块的集群ID，非资源系统统一的集群ID）
        @apiParamExample {json} 参数样例:
            {
              "geog_area_code": "inland"
              "cluster_type": "hdfs"
              "resource_type": "storage"
              "service_type": "hdfs"
              "src_cluster_id": "452"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "updated_at": "2020-03-30T18:54:10",
                        "available_cpu": 0.0,
                        "cluster_id": "bb40d523440eaf683a77757e1b5dc8d5",
                        "created_by": "xx",
                        "disk": 1048576000.0,
                        "updated_by": "xx",
                        "slot": 0.0,
                        "cluster_name": "hdfs-testOnline",
                        "available_memory": 0.0,
                        "cluster_type": "hdfs",
                        "available_gpu": 0.0,
                        "src_cluster_id": "452",
                        "available_slot": 0.0,
                        "available_net": 0.0,
                        "memory": 0.0,
                        "service_type": "hdfs",
                        "gpu": 0.0,
                        "net": 0.0,
                        "description": "",
                        "splitable": "0",
                        "active": "1",
                        "geog_area_code": "inland",
                        "available_disk": 1048576000.0,
                        "component_type": "hdfs",
                        "created_at": "2020-03-30T18:54:10",
                        "cpu": 0.0,
                        "resource_type": "storage"
                    },
                    "result": true
                }
        """
        cluster_config = resource_cluster_config.filter_list(
            src_cluster_id=params["src_cluster_id"],
            cluster_type=params["cluster_type"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            geog_area_code=params["geog_area_code"],
        ).first()
        if cluster_config:
            return Response(model_to_dict(cluster_config))
        else:
            return Response(None)
