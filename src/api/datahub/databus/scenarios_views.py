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

from common.auth import check_perm
from common.decorators import list_route
from common.exceptions import DataNotFoundError
from common.views import APIViewSet
from datahub.databus.exceptions import GetStorageClustersError
from datahub.databus.serializers import (
    CleanSerializer,
    ScenariosShipperSerializer,
    ScenariosStopShipperSerializer,
    SenariosCleanStopSerializer,
    SenariosCleanUpdateSerializer,
)
from datahub.databus.task import clean as task_clean
from datahub.databus.task.storage_task import create_storage_task
from rest_framework.response import Response

from datahub.databus import clean, rt, scenarios, settings


class ScenariosViewSet(APIViewSet):
    """
    和基础数据场景相关的接口，包含创建清洗任务，创建分发任务，获取存储列表，管理清洗和分发任务等
    """

    # result_table的名称，用于唯一确定一个senario中涉及的清洗result_table_id
    lookup_field = "result_table_id"

    @list_route(methods=["post"], url_path="setup_clean")
    def setup_clean(self, request):
        """
        @api {post} /scenarios/setup_clean/ 创建清洗配置,并启动清洗任务
        @apiGroup Scenario
        @apiDescription 创建一个总线的清洗配置
        @apiParam {int{合法的正整数}} raw_data_id 接入数据的id。
        @apiParam {string{小于65535字符}} json_config 清洗规则的json配置。
        @apiParam {string{小于65535字符}} pe_config 清洗规则的pe配置。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。英文标识在
        业务下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {object[]} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/field_type
        /is_dimension/field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 1500004 ResultTable对象不存在。
        @apiError (错误码) 1500005 清洗配置对象已存在，无法创建。
        @apiError (错误码) 1570010 创建Result Table失败。
        @apiError (错误码) 1570012 创建Result Table的kafka存储失败。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 42,
            "json_config": "{\"extract\": {...}, \"conf\": {...}}",
            "pe_config": "",
            "bk_biz_id": 1,
            "result_table_name": "xxx",
            "result_table_name_alias": "清洗表测试",
            "description": "清洗测试",
            "bk_username": "admin",
            "fields": [{
                "field_name": "ts",
                "field_alias": "时间戳",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 1
            }, {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": false,
                "field_index": 2
            }, {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": false,
                "field_index": 3
            }]
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {
                "result_table_id": "1_xxx"
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=CleanSerializer)
        check_perm("raw_data.etl", params["raw_data_id"])
        result = clean.create_clean_config(params)

        # 调用task接口启动clean任务
        rt_info = rt.get_databus_rt_info(result["result_table_id"])
        task_clean.process_clean_task_in_kafka(rt_info)
        return Response({"result_table_id": result["result_table_id"]})

    @list_route(methods=["post"], url_path="update_clean")
    def update_clean(self, request):
        """
        @api {post} /scenarios/update_clean/ 更新清洗配置
        @apiGroup Scenario
        @apiDescription 更新一个总线清洗的配置
        @apiParam {string{小于255字符}} result_table_id 结果表id
        @apiParam {int{合法的正整数}} raw_data_id 接入数据的id。
        @apiParam {string{小于65535字符}} json_config 清洗规则的json配置。
        @apiParam {string{小于65535字符}} pe_config 清洗规则的pe配置。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。英文标识在
        业务下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {object[]} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/field_type/is_dimension
        /field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 1570002 清洗配置对象不存在，无法更新。
        @apiError (错误码) 1570011 更新Result Table失败。
        @apiParamExample {json} 参数样例:
        {   "result_table_id": "1_xxx"
            "raw_data_id": 5,
            "json_config": "{\"conf\": \"abc\"}",
            "pe_config": "",
            "bk_biz_id": 102,
            "result_table_name": "etl_abc",
            "result_table_name_alias": "清洗表01",
            "description": "清洗测试",
            "fields": [{
                "field_name": "ts",
                "field_alias": "时间戳",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 1
            }, {
                "field_name": "col1",
                "field_alias": "字段1",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 2
            }]
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        # 首先更新result_table，然后更新清洗配置表
        params = self.params_valid(serializer=SenariosCleanUpdateSerializer)
        # 权限校验，需要有清洗的权限
        check_perm("raw_data.etl", params["raw_data_id"])
        result_table_id = params["result_table_id"]
        params["processing_id"] = result_table_id
        result = clean.update_clean_config(params)
        return Response(result)

    @list_route(methods=["post"], url_path="stop_clean")
    def stop_clean(self, request):
        """
        @api {post} /scenarios/stop_clean/ 停止清洗任务
        @apiGroup Scenario
        @apiDescription 停止清洗任务
        @apiParam {int{正整数}} raw_data_id 接入的数据源ID。
        @apiParam {string{小于255字符}} result_table_id 清洗的result table id。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 5,
            "result_table_id": "1_xxx"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": true
        }
        """

        p = self.params_valid(serializer=SenariosCleanStopSerializer)
        # 获取rt的配置信息
        rt_info = rt.get_databus_rt_info(p["result_table_id"])
        if rt_info:
            # 检查清洗rt和raw_data_id对应关系，检查存储是否存在
            scenarios.check_clean_and_storage(p["raw_data_id"], rt_info, "kafka", True)
            scenarios.stop_clean(rt_info)

            return Response(True)
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "result_table",
                    "column": "id",
                    "value": p["result_table_id"],
                }
            )

    @list_route(methods=["post"], url_path="setup_shipper")
    def setup_shipper(self, request):
        """
        @api {post} /scenarios/setup_shipper/ 创建分发存储，并启动对应的分发任务
        @apiGroup Scenario
        @apiDescription 在清洗的result_table_id上创建分发存储，并启动对应的分发任务
        @apiParam {int{正整数}} raw_data_id 接入的数据源ID。
        @apiParam {string{小于255字符}} result_table_id 清洗的result table id。
        @apiParam {boolean{bool值，默认false}} create_storage 是否在rt下创建存储。
        @apiParam {string{小于32字符}} cluster_type 数据分发的存储类型。
        @apiParam {string{小于32字符}} cluster_name 数据分发的存储集群名称，当create_storage为false时无需提供。
        @apiParam {string{小于255字符}} storage_config 数据分发的配置，当create_storage为false时无需提供。
        @apiParam {int{正整数}} expire_days 数据在存储中的过期时间，当create_storage为false时无需提供。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 5,
            "result_table_id": "1_xxx",
            "create_storage": true,
            "cluster_type": "es",
            "cluster_name": "es-default",
            "storage_config": "{\"conf\": \"abc\"}",
            "expire_days": 7
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "result_table_id": "105_etl_test",
                "cluster_type": "es"
            }
        }
        """
        p = self.params_valid(serializer=ScenariosShipperSerializer)
        raw_data_id = p["raw_data_id"]
        rt_id = p["result_table_id"]
        cluster_type = p["cluster_type"]
        create = bool(p["create_storage"])
        # 获取rt的配置信息
        rt_info = rt.get_databus_rt_info(rt_id)

        if rt_info:
            # 检查清洗rt和raw_data_id对应关系，检查存储是否存在
            scenarios.check_clean_and_storage(raw_data_id, rt_info, cluster_type, not create)
            create_storage_task(rt_id, [cluster_type])

            return Response({"result_table_id": rt_id, "cluster_type": cluster_type})
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "result_table",
                    "column": "id",
                    "value": p["result_table_id"],
                }
            )

    @list_route(methods=["post"], url_path="stop_shipper")
    def stop_shipper(self, request):
        """
        @api {post} /scenarios/stop_shipper/ 停止rt对应的分发任务
        @apiGroup Scenario
        @apiDescription 停止清洗result_table_id的某种存储的数据分发任务
        @apiParam {int{正整数}} raw_data_id 接入的数据源ID。
        @apiParam {string{小于255字符}} result_table_id 清洗的result table id。
        @apiParam {string{小于32字符}} cluster_type 数据分发的存储类型。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 5,
            "result_table_id": "1_xxx",
            "cluster_type": "es"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        p = self.params_valid(serializer=ScenariosStopShipperSerializer)
        # 获取rt的配置信息
        rt_info = rt.get_databus_rt_info(p["result_table_id"])
        if rt_info:
            # 检查清洗rt和raw_data_id对应关系，检查存储是否存在
            scenarios.check_clean_and_storage(p["raw_data_id"], rt_info, p["cluster_type"], True)
            scenarios.stop_shipper(rt_info, p["cluster_type"])

            return Response(True)
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "result_table",
                    "column": "id",
                    "value": p["result_table_id"],
                }
            )

    @list_route(methods=["get"], url_path="available_storage")
    def available_storage(self, request):
        """
        @api {get} /scenarios/available_storage/ 获取可用的分发存储类型列表
        @apiGroup Scenario
        @apiDescription 获取可用的分发存储类型列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": ["es", "mysql"]
        }
        """

        return Response(settings.AVAILABLE_STORAGE_LIST)

    @list_route(methods=["get"], url_path="storage_clusters")
    def storage_clusters(self, request):
        """
        @api {get} /scenarios/storage_clusters/ 获取可用的分发存储集群列表
        @apiGroup Scenario
        @apiDescription 获取可用的分发存储集群列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "es": {"cluster": ["default"], "expires": [7, 15, 30]},
                "tspider": {"cluster": ["default"], "expires": [7, 15, 30]},
                "tsdb": {"cluster": ["default"], "expires": [7, 15, 30]}
            }
        }
        """
        clusters = scenarios.get_storage_clusters(settings.AVAILABLE_STORAGE_LIST)

        if clusters:
            result = {}
            for cluster_type in clusters:
                result[cluster_type] = {
                    "clusters": clusters[cluster_type],
                    "expires": settings.STORAGE_EXPIRE_DAYS,
                }
            return Response(result)
        else:
            raise GetStorageClustersError()
