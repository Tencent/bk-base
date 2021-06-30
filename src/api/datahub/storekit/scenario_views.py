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

from common.decorators import list_route
from common.exceptions import DataAlreadyExistError, DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from datahub.common.const import (
    ACTIVE,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    COLUMN,
    DESCRIPTION,
    SCENARIO_NAME,
    TABLE,
    VALUE,
)
from datahub.storekit import model_manager
from datahub.storekit import storage_settings as s
from datahub.storekit.models import StorageScenarioConfig
from datahub.storekit.serializers import (
    ScenariosSerializer,
    StorageScenarioConfigSerializer,
)
from datahub.storekit.settings import STORAGE_ADMIN, STORAGE_QUERY_ORDER
from django.db import IntegrityError
from rest_framework.response import Response


class AllScenariosSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/storekit/scenarios/ 存储场景列表
        @apiGroup Scenarios
        @apiDescription 获取存储集群场景配置列表
        @apiParam {String} scenario_name 场景名称，可选参数
        @apiParam {String} cluster_type 场景集群类型，可选参数
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "storage_scenario_name": "RDBMS",
                    "storage_scenario_alias": "关系型数据库",
                    "cluster_type": "mysql",
                    "active": true,
                    "created_by": "",
                    "created_at": "2019-02-25 15:53:15",
                    "updated_by": "",
                    "updated_at": "2019-06-21 12:28:07",
                    "description": "建议使用场景：\n\t关系型数据库及关联分析\n\t建议日数据量[单表]：\n\tGB/千万级以内\n\t查询模式：\n\t点查询/关联查询/聚合查询",
                    "formal_name": "MySQL",
                    "storage_scenario_priority": 6,
                    "cluster_type_priority": 0
                },
                {
                    "storage_scenario_name": "olap",
                    "storage_scenario_alias": "分析型数据库",
                    "cluster_type": "druid",
                    "active": true,
                    "created_by": "",
                    "created_at": "2019-02-25 15:53:55",
                    "updated_by": "",
                    "updated_at": "2019-06-24 18:03:19",
                    "description": "<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> GB/千万级以上
                                    <br><b>查询模式：</b><br> 明细查询/聚合查询",
                    "formal_name": "Druid",
                    "storage_scenario_priority": 5,
                    "cluster_type_priority": 0
                }
            ],
            "result": true
        }
        """
        params = self.params_valid(serializer=ScenariosSerializer)
        result = model_manager.get_scenario_configs_by_name_type(params.get(SCENARIO_NAME), params.get(CLUSTER_TYPE))
        return Response(result)

    @list_route(methods=["get"], url_path="common")
    def common(self, request):
        """
        @api {get} v3/storekit/scenarios/common/ 存储使用场景
        @apiGroup Scenarios
        @apiDescription 获取不同存储的使用场景、参数描述等信息
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true,
            "data": {},
            "message": "ok",
            "code": "1500200",
        }
        """
        return Response(
            {
                "formal_name": s.FORMAL_NAME,
                "storage_query": list(STORAGE_QUERY_ORDER.keys()),
                "storage_delete": s.STORAGE_DELETE,
                "storage_scenario": s.STORAGE_SCENARIOS,
                "storage_admin": STORAGE_ADMIN,
            }
        )


class ScenariosSet(APIViewSet):

    """
    storage_scenario_config相关的接口，包含CRUD
    """

    lookup_field = "scenario_name"

    def create(self, request, cluster_type):
        """
        @api {post} v3/storekit/scenarios/:cluster_type/ 新增存储场景
        @apiGroup Scenarios
        @apiDescription 新增存储集群场景配置
        @apiParamExample {json} 参数样例:
        {
            "storage_scenario_name": "OLAP",
            "storage_scenario_alias": "分析型",
            "active": true
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "storage_scenario_name": "OLAP",
                "storage_scenario_alias": "分析型",
                "cluster_type": "Druid",
                "active": true,
                "created_by": "",
                "created_at": "2018-11-01T20:55:23.559450",
                "updated_by": "",
                "updated_at": "2018-11-01T20:55:23.564820",
                "description": ""
            },
            "result": true
        }
        """
        if request.data:
            request.data[CLUSTER_TYPE] = cluster_type
        params = self.params_valid(serializer=StorageScenarioConfigSerializer)
        try:
            obj = StorageScenarioConfig.objects.create(
                cluster_type=cluster_type,
                storage_scenario_name=params.get("storage_scenario_name"),
                storage_scenario_alias=params.get("storage_scenario_alias"),
                active=params.get(ACTIVE, False),
                created_by=get_request_username(),
                updated_by=get_request_username(),
                description=params.get(DESCRIPTION, ""),
                storage_scenario_priority=params.get("storage_scenario_priority", 0),
                cluster_type_priority=params.get("cluster_type_priority", 0),
            )
            return Response(StorageScenarioConfigSerializer(obj).data)
        except IntegrityError:
            logger.warning(f"databus cluster {params[CLUSTER_NAME]} is already exists!")
            raise DataAlreadyExistError(
                message_kv={
                    TABLE: "storage_scenario_config",
                    COLUMN: "storage_scenario_name, cluster_type",
                    VALUE: f'{params["storage_scenario_name"]}, {params[CLUSTER_TYPE]}',
                }
            )

    def retrieve(self, request, cluster_type, scenario_name):
        """
        @api {get} v3/storekit/scenarios/:cluster_type/:scenario_name/ 获取存储场景
        @apiGroup Scenarios
        @apiDescription 获取单个存储场景
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "storage_scenario_name": "scenario_test",
                "storage_scenario_alias": "alias01",
                "cluster_type": "es",
                "active": true,
                "created_by": "",
                "created_at": "2018-11-01T20:55:24",
                "updated_by": "",
                "updated_at": "2018-11-01T20:55:24",
                "description": ""
            },
            "result": true
        }
        """
        result = model_manager.get_scenario_configs_by_name_type(scenario_name, cluster_type)
        if result:
            return Response(result[0])
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_scenario_config",
                    COLUMN: "storage_scenario_name, cluster_type",
                    VALUE: f"{scenario_name}, {cluster_type}",
                }
            )

    def update(self, request, cluster_type, scenario_name):
        """
        @api {put} v3/storekit/scenarios/:cluster_type/:scenario_name/ 更新存储场景
        @apiGroup Scenarios
        @apiDescription 更新单个存储场景
        @apiParamExample {json} 参数样例:
        {
            "storage_scenario_alias": "alias01_new"
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "storage_scenario_name": "scenario_test",
                "storage_scenario_alias": "alias01_new",
                "cluster_type": "es",
                "active": true,
                "created_by": "",
                "created_at": "2018-11-01T20:55:24",
                "updated_by": "",
                "updated_at": "2018-11-01T20:57:52.605256",
                "description": ""
            },
            "result": true
        }
        """
        objs = model_manager.get_scenario_objs_by_name_type(scenario_name, cluster_type)
        if objs:
            obj = objs[0]
            params = request.data
            obj.storage_scenario_alias = params.get("storage_scenario_alias", obj.storage_scenario_alias)
            obj.storage_scenario_priority = params.get("storage_scenario_priority", obj.storage_scenario_priority)
            obj.cluster_type_priority = params.get("cluster_type_priority", obj.cluster_type_priority)
            obj.active = params.get(ACTIVE, obj.active)
            obj.description = params.get(DESCRIPTION, obj.description)
            obj.updated_by = get_request_username()
            obj.save()
            return Response(StorageScenarioConfigSerializer(obj).data)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_scenario_config",
                    COLUMN: "storage_scenario_name, cluster_type",
                    VALUE: f"{scenario_name}, {cluster_type}",
                }
            )

    def destroy(self, request, cluster_type, scenario_name):
        """
        @api {delete} v3/storekit/scenarios/:cluster_type/:scenario_name/ 删除存储场景
        @apiGroup Scenarios
        @apiDescription 删除单个存储场景
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": true,
            "result": true
        }
        """
        objs = model_manager.get_scenario_objs_by_name_type(scenario_name, cluster_type)
        if objs:
            obj = objs[0]
            obj.delete()
            return Response(True)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_scenario_config",
                    COLUMN: "storage_scenario_name, cluster_type",
                    VALUE: f"{scenario_name}, {cluster_type}",
                }
            )

    def list(self, request, cluster_type):
        """
        @api {get} v3/storekit/scenarios/:cluster_type/ 指定类型存储场景列表
        @apiGroup Scenarios
        @apiDescription 获取指定类型的存储集群场景配置列表
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "storage_scenario_name": "olap",
                    "storage_scenario_alias": "分析型数据库2",
                    "cluster_type": "druid",
                    "active": true,
                    "created_by": "",
                    "created_at": "2019-02-25 15:53:55",
                    "updated_by": "",
                    "updated_at": "2019-07-11 14:57:46",
                    "description": "<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> GB/千万级以上
                                    <br><b>查询模式：</b><br> 明细查询/聚合查询",
                    "formal_name": "Druid",
                    "storage_scenario_priority": 6,
                    "cluster_type_priority": 1
                },
                {
                    "storage_scenario_name": "tsdb",
                    "storage_scenario_alias": "时序数据库",
                    "cluster_type": "druid",
                    "active": true,
                    "created_by": "",
                    "created_at": "2019-02-25 15:54:23",
                    "updated_by": "",
                    "updated_at": "2019-06-24 18:05:34",
                    "description": "<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> GB/千万级以上
                                    <br><b>查询模式：</b><br> 明细查询/聚合查询",
                    "formal_name": "Druid",
                    "storage_scenario_priority": 5,
                    "cluster_type_priority": 0
                }
            ],
            "result": true
        }
        """
        result = model_manager.get_scenario_configs_by_name_type(None, cluster_type)
        return Response(result)
