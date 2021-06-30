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
import datetime

from django.forms import model_to_dict
from rest_framework.response import Response

from common.decorators import params_valid, list_route
from common.local import get_request_username
from common.views import APIViewSet
from resourcecenter.handlers import resource_cluster_config
from resourcecenter.metric.processing_merric_driver import get_processing_all_cluster_summary
from resourcecenter.serializers.serializers import ResourceClusterConfigSerializer
from resourcecenter.storekit.storekit_helper import StorekitHelper


class ResourceClusterViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_Cluster
    集群管理接口
    """

    lookup_field = "cluster_id"

    def list(self, request):
        """
        @api {get} /resourcecenter/clusters/ 全部集群
        @apiName list_clusters
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [resource_type] 资源类型
        @apiParam {string} [service_type] 资源类型
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_type] 集群类型
        @apiParam {string} [cluster_name] 集群名称
        @apiParam {string} [component_type] 组件类型
        @apiParam {string} [geog_area_code] 区域
        @apiParam {string} [active] 是否生效

        @apiParamExample {json} 参数样例:
            {
                "cluster_type": "yarn",
                "cluster_name": "yarn集群1",
                "component_type": "yarn",
                "geog_area_code": "inland",
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
                            "resource_group_id": "default_test_00",
                            "updated_at": "2020-02-20T14:33:02",
                            "available_cpu": 10000.0,
                            "cluster_id": "yarn-sz-01",
                            "created_by": "xx",
                            "disk": 0.0,
                            "updated_by": null,
                            "slot": 0.0,
                            "cluster_name": "yarn集群1",
                            "available_memory": 100000000.0,
                            "cluster_type": "yarn",
                            "available_gpu": 0.0,
                            "available_slot": 0.0,
                            "available_net": 0.0,
                            "memory": 100000000.0,
                            "gpu": 0.0,
                            "net": 0.0,
                            "description": "深圳yarn1",
                            "splitable": "1",
                            "active": "1",
                            "geog_area_code": "inland",
                            "available_disk": 0.0,
                            "component_type": "yarn",
                            "created_at": "2020-02-20T14:33:02",
                            "cpu": 10000.0
                        },
                        {
                            "resource_group_id": "default_test_00",
                            "updated_at": "2020-02-20T14:33:10",
                            "available_cpu": 10000.0,
                            "cluster_id": "yarn-sz-02",
                            "created_by": "xx",
                            "disk": 0.0,
                            "updated_by": null,
                            "slot": 0.0,
                            "cluster_name": "yarn集群1",
                            "available_memory": 100000000.0,
                            "cluster_type": "yarn",
                            "available_gpu": 0.0,
                            "available_slot": 0.0,
                            "available_net": 0.0,
                            "memory": 100000000.0,
                            "gpu": 0.0,
                            "net": 0.0,
                            "description": "深圳yarn1",
                            "splitable": "1",
                            "active": "1",
                            "geog_area_code": "inland",
                            "available_disk": 0.0,
                            "component_type": "yarn",
                            "created_at": "2020-02-20T14:33:10",
                            "cpu": 10000.0
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        if "resource_group_id" in request.query_params:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "resource_type" in request.query_params:
            query_params["resource_type"] = request.query_params["resource_type"]
        if "service_type" in request.query_params:
            query_params["service_type"] = request.query_params["service_type"]
        if "cluster_name" in request.query_params:
            query_params["cluster_name__icontains"] = request.query_params["cluster_name"]
        if "cluster_type" in request.query_params:
            query_params["cluster_type"] = request.query_params["cluster_type"]
        if "component_type" in request.query_params:
            query_params["component_type"] = request.query_params["component_type"]
        if "geog_area_code" in request.query_params:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]
        if "active" in request.query_params:
            query_params["active"] = request.query_params["active"]
        query_list = resource_cluster_config.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=ResourceClusterConfigSerializer)
    def create(self, request, params):
        """
        @api {post} /resourcecenter/clusters/ 创建集群
        @apiName create_cluster
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} bk_username 提交人
        @apiParam {string} cluster_id 集群ID
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} cluster_name 集群名称
        @apiParam {string} component_type 组件类型
        @apiParam {string} geog_area_code 区域
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型
        @apiParam {string} src_cluster_id 外部系统集群ID
        @apiParam {string} active 是否生效
        @apiParam {string} splitable 是否可以拆分分配
        @apiParam {string} cpu CPU数量，单位core
        @apiParam {string} memory 内存数据量，单位MB
        @apiParam {string} gpu GPU数量，单位core
        @apiParam {string} disk 存储容量，单位MB
        @apiParam {string} net 网络带宽，单位bit
        @apiParam {string} slot 处理槽位，单位个
        @apiParam {string} available_cpu 可用CPU数量，单位core
        @apiParam {string} available_memory 可用内存数据量，单位MB
        @apiParam {string} available_gpu 可用GPU数量，单位core
        @apiParam {string} available_disk 可用存储容量，单位MB
        @apiParam {string} available_net 可用网络带宽，单位bit
        @apiParam {string} available_slot 可用处理槽位，单位个
        @apiParam {string} connection_info 集群连接信息
        @apiParam {int} priority 优先度
        @apiParam {string} belongs_to 所属父集群
        @apiParam {string} description 描述
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "cluster_id": "flink-session-sz-01",
                "cluster_type": "flink-session",
                "cluster_name": "深圳flink-session集群1",
                "component_type": "yarn",
                "geog_area_code": "inland",
                "resource_group_id": "default",
                "resource_type": "processing",
                "service_type": "stream",
                "src_cluster_id": "123456",
                "active": 1,
                "splitable": 1,
                "cpu": 10000,
                "memory": 100000000,
                "gpu": 0.0,
                "disk": 0.0,
                "net": 0.0,
                "slot": 0.0,
                "available_cpu": 10000,
                "available_memory": 100000000,
                "available_gpu": 0.0,
                "available_disk": 0.0,
                "available_net": 0.0,
                "available_slot": 0.0,
                "connection_info": "{"cluster_domain": "k8s-cluster-domain"}"
                "description": "深圳yarn1"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "cluster_id": "yarn-sz-01"
                    },
                    "result": true
                }
        """

        resource_cluster_config.save(
            cluster_id=params["cluster_id"],
            cluster_type=params["cluster_type"],
            cluster_name=params["cluster_name"],
            component_type=params["component_type"],
            geog_area_code=params["geog_area_code"],
            resource_group_id=params["resource_group_id"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            src_cluster_id=params["src_cluster_id"],
            active=params["active"],
            splitable=params["splitable"],
            cpu=params["cpu"],
            memory=params["memory"],
            gpu=params["gpu"],
            disk=params["disk"],
            net=params["net"],
            slot=params["slot"],
            available_cpu=params["available_cpu"],
            available_memory=params["available_memory"],
            available_gpu=params["available_gpu"],
            available_disk=params["available_disk"],
            available_net=params["available_net"],
            available_slot=params["available_slot"],
            connection_info=params["connection_info"] if "connection_info" in params else "",
            priority=params["priority"] if "priority" in params else None,
            belongs_to=params["belongs_to"] if "belongs_to" in params else None,
            description=params["description"],
            created_by=get_request_username(),
        )
        return Response({"cluster_id": params["cluster_id"]})

    @params_valid(serializer=ResourceClusterConfigSerializer)
    def update(self, request, cluster_id, params):
        """
        @api {put} /resourcecenter/clusters/:cluster_id/ 更新集群
        @apiName update_cluster
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} bk_username 提交人
        @apiParam {string} cluster_id 集群ID
        @apiParam {string} cluster_type 集群类型
        @apiParam {string} cluster_name 集群名称
        @apiParam {string} component_type 组件类型
        @apiParam {string} geog_area_code 区域
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型
        @apiParam {string} src_cluster_id 外部系统集群ID
        @apiParam {string} active 是否生效
        @apiParam {string} splitable 是否可以拆分分配
        @apiParam {string} cpu CPU数量，单位core
        @apiParam {string} memory 内存数据量，单位MB
        @apiParam {string} gpu GPU数量，单位core
        @apiParam {string} disk 存储容量，单位MB
        @apiParam {string} net 网络带宽，单位bit
        @apiParam {string} slot 处理槽位，单位个
        @apiParam {string} available_cpu 可用CPU数量，单位core
        @apiParam {string} available_memory 可用内存数据量，单位MB
        @apiParam {string} available_gpu 可用GPU数量，单位core
        @apiParam {string} available_disk 可用存储容量，单位MB
        @apiParam {string} available_net 可用网络带宽，单位bit
        @apiParam {string} available_slot 可用处理槽位，单位个
        @apiParam {string} connection_info 集群连接信息
        @apiParam {int} priority 优先度
        @apiParam {string} belongs_to 所属父集群
        @apiParam {string} description 描述
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "cluster_id": "flink-session-sz-01",
                "cluster_type": "flink-session",
                "cluster_name": "深圳flink-session集群1",
                "component_type": "yarn",
                "geog_area_code": "inland",
                "resource_group_id": "default",
                "resource_type": "processing",
                "service_type": "stream",
                "src_cluster_id": "123456",
                "active": 1,
                "splitable": 1,
                "cpu": 10000,
                "memory": 100000000,
                "gpu": 0.0,
                "disk": 0.0,
                "net": 0.0,
                "slot": 0.0,
                "available_cpu": 5000,
                "available_memory": 50000000,
                "available_gpu": 0.0,
                "available_disk": 0.0,
                "available_net": 0.0,
                "available_slot": 0.0,
                "connection_info": "{"cluster_domain": "k8s-cluster-domain"}"
                "description": "深圳yarn1"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "yarn-sz-01"
                    },
                    "result": true
                }
        """
        resource_cluster_config.update(
            cluster_id,
            cluster_type=params["cluster_type"],
            cluster_name=params["cluster_name"],
            component_type=params["component_type"],
            geog_area_code=params["geog_area_code"],
            resource_group_id=params["resource_group_id"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            src_cluster_id=params["src_cluster_id"],
            active=params["active"],
            splitable=params["splitable"],
            cpu=params["cpu"],
            memory=params["memory"],
            gpu=params["gpu"],
            disk=params["disk"],
            net=params["net"],
            slot=params["slot"],
            available_cpu=params["available_cpu"],
            available_memory=params["available_memory"],
            available_gpu=params["available_gpu"],
            available_disk=params["available_disk"],
            available_net=params["available_net"],
            available_slot=params["available_slot"],
            connection_info=params["connection_info"] if "connection_info" in params else "",
            priority=params["priority"] if "priority" in params else None,
            belongs_to=params["belongs_to"] if "belongs_to" in params else None,
            description=params["description"],
            updated_by=get_request_username(),
        )
        return Response({"cluster_id": params["cluster_id"]})

    def retrieve(self, request, cluster_id):
        """
        @api {get} /resourcecenter/clusters/:cluster_id/ 查看集群
        @apiName view_cluster
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} cluster_id 集群ID
        @apiParamExample {json} 参数样例:
            {
              "cluster_id": "yanr-sz-01"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "updated_at": "2020-02-20T13:55:55",
                        "available_cpu": 5000.0,
                        "cluster_id": "flink-session-sz-01",
                        "created_by": "xx",
                        "disk": 0.0,
                        "updated_by": "xx",
                        "slot": 0.0,
                        "cluster_name": "深圳flink-session集群1",
                        "available_memory": 50000000.0,
                        "cluster_type": "flink-session",
                        "available_gpu": 0.0,
                        "available_slot": 0.0,
                        "available_net": 0.0,
                        "memory": 100000000.0,
                        "gpu": 0.0,
                        "net": 0.0,
                        "description": "深圳yarn1",
                        "splitable": "1",
                        "active": "1",
                        "geog_area_code": "inland",
                        "resource_group_id": "default",
                        "resource_type": "processing",
                        "service_type": "stream",
                        "src_cluster_id": "123456",
                        "available_disk": 0.0,
                        "component_type": "yarn",
                        "created_at": "2020-02-20T13:55:55",
                        "cpu": 10000.0
                    },
                    "result": true
                }
        """
        obj = resource_cluster_config.get(cluster_id)
        return Response(model_to_dict(obj))

    def destroy(self, request, cluster_id):
        """
        @api {delete} /resourcecenter/clusters/:cluster_id/ 删除集群
        @apiName delete_cluster
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} cluster_id 集群ID
        @apiParamExample {json} 参数样例:
            {
              "cluster_id": "yarn-sz-01"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "yarn-sz-01"
                    },
                    "result": true
                }
        """
        # 集群如果有引用，需要进行逻辑删除。
        resource_cluster_config.delete(cluster_id)
        return Response({"cluster_id": cluster_id})

    @list_route(methods=["get"], url_path="list_id")
    def list_id(self, request):
        """
        @api {get} /resourcecenter/clusters/list_id/ 全部集群
        @apiName list_cluster_ids
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [resource_type] 资源类型
        @apiParam {string} [service_type] 资源类型
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_type] 集群类型
        @apiParam {string} [cluster_name] 集群名称
        @apiParam {string} [component_type] 组件类型
        @apiParam {string} [geog_area_code] 区域
        @apiParam {string} [active] 是否生效

        @apiParamExample {json} 参数样例:
            {
                "cluster_type": "yarn",
                "cluster_name": "yarn集群1",
                "component_type": "yarn",
                "geog_area_code": "inland",
                "active": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        "yarn-sz-01",
                        "yarn-sz-02",
                        "yarn-sz-03"
                    ],
                    "result": true
                }
        """
        query_params = {}
        if "resource_group_id" in request.query_params:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "resource_type" in request.query_params:
            query_params["resource_type"] = request.query_params["resource_type"]
        if "service_type" in request.query_params:
            query_params["service_type"] = request.query_params["service_type"]
        if "cluster_name" in request.query_params:
            query_params["cluster_name__icontains"] = request.query_params["cluster_name"]
        if "cluster_type" in request.query_params:
            query_params["cluster_type"] = request.query_params["cluster_type"]
        if "component_type" in request.query_params:
            query_params["component_type"] = request.query_params["component_type"]
        if "geog_area_code" in request.query_params:
            query_params["geog_area_code"] = request.query_params["geog_area_code"]
        if "active" in request.query_params:
            query_params["active"] = request.query_params["active"]
        query_list = resource_cluster_config.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                cluster = model_to_dict(obj)
                if "cluster_id" in cluster:
                    ret.append(cluster["cluster_id"])

        return Response(ret)

    @list_route(methods=["get"], url_path="sync_storage_capacity")
    def sync_storage_capacity(self, request):
        """
        @api {get} /resourcecenter/clusters/sync_storage_capacity/ 同步存储容量
        @apiDescription 管理员可以通过该接口刷新存储集群到容量到资源系统配置。<br/>
        当前存储注册集群时，没有提供容量信息，只能同步能够采集到到存储容量。
        @apiName sync_storage_capacity
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_type] 集群类型
        @apiParamExample {json} 参数样例:
            {
              "cluster_id": "yarn-sz-01"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "yarn-sz-01"
                    },
                    "result": true
                }
        """
        params = {"resource_type": "storage"}
        if "cluster_id" in request.query_params:
            params["cluster_id"] = request.query_params.get("cluster_id", "")
        if "cluster_type" in request.query_params:
            params["cluster_type"] = request.query_params.get("cluster_type", "")
        if "cluster_name" in request.query_params:
            params["cluster_name"] = request.query_params.get("cluster_name", "")

        cluster_list = resource_cluster_config.filter_list(**params)
        if cluster_list:
            for cluster in cluster_list:
                cluster_type = cluster.cluster_type
                cluster_name = cluster.cluster_name
                cluster_id = cluster.cluster_id
                last_series = StorekitHelper.get_cluster_capacity(cluster_type, cluster_name)
                if last_series is not None and "CapacityTotalMB" in last_series:
                    if last_series["CapacityTotalMB"] > 0:
                        disk = last_series["CapacityTotalMB"]
                        available_disk = last_series["CapacityTotalMB"]
                        if "CapacityUsedMB" in last_series and last_series["CapacityUsedMB"] > 0:
                            available_disk = last_series["CapacityTotalMB"] - last_series["CapacityUsedMB"]
                        resource_cluster_config.update(
                            cluster_id, disk=disk, available_disk=available_disk, updated_by=get_request_username()
                        )

        return Response()

    @list_route(methods=["get"], url_path="sync_processing_capacity")
    def sync_processing_capacity(self, request):
        """
        @api {get} /resourcecenter/clusters/sync_processing_capacity/ 同步计算容量
        @apiDescription 管理员可以通过该接口刷新计算集群到容量到资源系统配置。<br/>
        当前计算注册集群时，没有提供容量信息。
        @apiName sync_processing_capacity
        @apiGroup ResourceCenter_Cluster
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_type] 集群类型
        @apiParamExample {json} 参数样例:
            {
              "cluster_id": "yarn-sz-01"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "cluster_id": "yarn-sz-01"
                    },
                    "result": true
                }
        """
        params = {"resource_type": "processing"}
        if "cluster_id" in request.query_params:
            params["cluster_id"] = request.query_params.get("cluster_id", "")
        if "cluster_type" in request.query_params:
            params["cluster_type"] = request.query_params.get("cluster_type", "")
        if "cluster_name" in request.query_params:
            params["cluster_name"] = request.query_params.get("cluster_name", "")

        cluster_list = resource_cluster_config.filter_list(**params)
        now_time = datetime.datetime.now()
        yes_time = now_time + datetime.timedelta(days=-1)
        args = {
            "start_time": request.query_params.get("start_time", yes_time.strftime("%Y-%m-%d %H:%M:%S")),
            "end_time": request.query_params.get("end_time", now_time.strftime("%Y-%m-%d %H:%M:%S")),
        }
        all_cluster_summary = get_processing_all_cluster_summary(args)
        if cluster_list:
            for cluster in cluster_list:
                cluster_id = cluster.cluster_id
                if cluster_id in all_cluster_summary:
                    cpu = all_cluster_summary[cluster_id].get("cpu", 0)
                    memory = all_cluster_summary[cluster_id].get("memory", 0)
                    resource_cluster_config.update(
                        cluster_id, cpu=cpu, memory=memory, updated_by=get_request_username()
                    )

        return Response()
