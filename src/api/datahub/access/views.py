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
import os

from common.auth import perm_check
from common.base_utils import model_to_dict
from common.bklanguage import BkLanguage, bktranslates
from common.decorators import detail_route, list_route
from common.exceptions import ValidationError
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from conf import dataapi_settings
from datahub.access.exceptions import (
    AccessError,
    AcessCode,
    CollectorError,
    CollerctorCode,
)
from datahub.access.handlers.raw_data import RawDataHandler
from datahub.access.models import (
    AccessDbTypeConfig,
    AccessHostConfig,
    AccessOperationLog,
    AccessRawData,
    AccessRawDataTask,
    AccessResourceInfo,
    AccessScenarioConfig,
    AccessScenarioSourceConfig,
    AccessScenarioStorageChannel,
    AccessSourceConfig,
    AccessTask,
    DataCategoryConfig,
    EncodingConfig,
    FieldDelimiterConfig,
    FieldTypeConfig,
    TimeFormatConfig,
)
from datahub.access.serializers import (
    AccessTaskLogSerializer,
    CollectorHubDeployPlanSerializer,
    CollectorHubQueryHistorySerializer,
    CollectorHubQueryStatusSerializer,
    CollectorHubSumarySerializer,
    DataScenarioSerializer,
    FileHdfsInfoSerializer,
    FileMergeUploadSerializer,
    FilePartUploadSerializer,
    FileUploadSerializer,
    RawDataPermissionSerializer,
    ServiceDeploySerializer,
    check_perm_by_scenario,
    set_subMenus,
)
from django.db.models import Q
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from ..common.const import (
    BK_BIZ_ID,
    CHUNK_ID,
    CHUNK_SIZE,
    CONNECTION,
    FILE,
    FILE_NAME,
    HOSTS,
    MD5,
    PORT,
    RAW_DATA_ID,
    TASK_NAME,
    UPLOAD_UUID,
)
from .collectors.factory import CollectorFactory
from .collectors.log_collector import bknode
from .utils.upload_hdfs_util import (
    create_and_write_file,
    get_default_storage,
    get_hdfs_upload_path,
    get_hdfs_upload_path_chunk,
    load_and_merge_file,
    remove_dir,
)


class DataSourceConfigViewSet(APIViewSet):
    lookup_field = "access_scenario_id"

    def list(self, request):
        """
        @api {get} v3/access/source/ 获取数据来源列表
        @apiGroup RawData
        @apiDescription  获取数据来源列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/source/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "data_source_name": "system",
                    "updated_by": null,
                    "created_at": "2019-01-02T18:54:52",
                    "pid": 0,
                    "updated_at": null,
                    "created_by": null,
                    "sub_list": [
                        {
                            "data_source_name": "biz_analysis_sys",
                            "updated_by": null,
                            "_subMenus": [],
                            "created_at": "2019-01-02T17:08:48",
                            "pid": 7,
                            "updated_at": null,
                            "created_by": null,
                            "active": 1,
                            "data_source_alias": "xxx",
                            "id": 8,
                            "description": "第三方系统"
                        }
                    ],
                    "active": 1,
                    "data_source_alias": "第三方系统",
                    "id": 7,
                    "description": "第三方系统"
                }
            ],
            "result": true
        }
        """
        result = AccessSourceConfig.objects.all()
        values = list()
        for sourceConfig in result:
            sourceConfig.data_source_alias = bktranslates(sourceConfig.data_source_alias)

        for sourceConfig in result:
            dic_conf = dict()
            dic_conf = model_to_dict(sourceConfig)
            if sourceConfig.pid == 0:
                dic_conf["sub_list"] = set_subMenus(sourceConfig.id, result)
                values.append(dic_conf)

        return Response(values)

    def retrieve(self, request, access_scenario_id):
        """
        @api {get} v3/access/source/:access_scenario_id/ 根据access_scenario_id获取数据来源列表
        @apiGroup RawData
        @apiDescription  根据access_scenario_id获取数据来源列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/source/1/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "data_source_name": "business_server",
                    "updated_by": null,
                    "created_at": "2019-01-02T17:08:48",
                    "pid": 0,
                    "updated_at": null,
                    "created_by": null,
                    "active": 1,
                    "data_source_alias": "业务服务器",
                    "id": 2,
                    "description": "务服务器"
                }
            ],
            "result": true
        }
        """
        res_data = []
        access_source_ids = AccessScenarioSourceConfig.objects.filter(
            access_scenario_id=access_scenario_id
        ).values_list("access_source_id", flat=True)
        if access_source_ids:
            result = AccessSourceConfig.objects.filter(id__in=access_source_ids)
            for sourceConfig in result:
                sourceConfig.data_source_alias = bktranslates(sourceConfig.data_source_alias)
                res_data.append(model_to_dict(sourceConfig))
            return Response(res_data)
        else:
            return Response()


class DataScenarioConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/access/scenario/ 获取数据场景列表
        @apiGroup RawData
        @apiDescription  获取数据场景列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/scenario/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "data_scenario_name": "log",
                    "updated_by": null,
                    "created_at": "2018-10-20T14:50:44",
                    "updated_at": null,
                    "created_by": "1",
                    "data_scenario_alias": "svrlog",
                    "active": 1,
                    "orders": 1,
                    "type": "common",
                    "id": 1,
                    "description": "aaa"
                }
            ],
            "result": true
        }
        """
        res_data = []
        result = AccessScenarioConfig.objects.all().order_by("orders")
        for scenario_config in result:
            scenario_config.data_scenario_alias = bktranslates(scenario_config.data_scenario_alias)
            res_data.append(model_to_dict(scenario_config))
        return Response(res_data)


class EncodingConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/access/encode/ 获取字符编码列表
        @apiGroup RawData
        @apiDescription  获取字符编码列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/encode/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "created_at": "2018-10-22T20:57:00",
                    "updated_at": null,
                    "created_by": "",
                    "encoding_id": "1",
                    "active": 1,
                    "encoding_name": "UTF-8",
                    "description": "UTF-8"
                }
            ],
            "result": true
        }
        """
        result = EncodingConfig.objects.all()
        return Response(result.values())


class FieldTypeConfigViewSet(APIViewSet):
    def list(self, request):
        """
         @api {get} v3/access/field_type/ 获取数据类型列表
         @apiGroup RawData
         @apiDescription   获取数据类型列表
         @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
         @apiError (错误码) 1500500  <code>服务异常</code>
         @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/field_type/
         @apiSuccessExample {json} Success-Response:
         HTTP/1.1 200 OK
         {
             "message": "ok",
             "code": "1500200",
             "data": [
                 {
                     "field_type": "int",
                     "updated_by": null,
                     "created_at": "2018-10-22T20:58:10",
                     "type_name": "整型",
                     "updated_at": null,
                     "created_by": "",
                     "active": 1,
                     "description": "整型"
                 }
             ],
             "result": true
         }
        """
        result = FieldTypeConfig.objects.filter(~Q(field_type="timestamp"))
        return Response(result.values())


class TimeFormatConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/access/time_format/ 获取时间格式列表
        @apiGroup RawData
        @apiDescription   获取时间格式列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/time_format/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "timestamp_len": 0,
                    "time_format_alias": "yyyy-MM-dd HH:mm:ss",
                    "updated_by": null,
                    "created_at": "2018-11-06T12:07:10",
                    "updated_at": null,
                    "created_by": "",
                    "time_format": "yyyy-MM-dd HH:mm:ss",
                    "active": 0,
                    "id": 1,
                    "description": "时间格式"
                }
            ],
            "result": true
        }
        """
        result = TimeFormatConfig.objects.filter(active=1)
        return Response(result.values())


class DelimiterConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/access/delimiter/ 获取分隔符列表
        @apiGroup RawData
        @apiDescription   获取分隔符列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/delimiter/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "created_at": "2019-01-02T19:12:59",
                    "updated_at": null,
                    "created_by": null,
                    "delimiter_alias": "|",
                    "delimiter": "|",
                    "active": 1,
                    "id": 1,
                    "description": "|"
                }
            ],
            "result": true
        }
        """
        response_list = list()
        result = FieldDelimiterConfig.objects.all()
        for delimiter_conf in result:
            delimiter_conf.delimiter_alias = bktranslates(delimiter_conf.delimiter_alias)
            response_list.append(model_to_dict(delimiter_conf))
        return Response(response_list)


class DataCategoryConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/access/category/ 获取数据分类列表
        @apiGroup RawData
        @apiDescription   获取数据分类列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/category/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "data_category_alias": "线上",
                    "updated_by": null,
                    "data_category_name": "",
                    "created_at": "2018-10-22T21:01:46",
                    "updated_at": null,
                    "created_by": "admin",
                    "disabled": 0,
                    "id": 1,
                    "description": "在线"
                }
            ],
            "result": true
        }
        """
        result = DataCategoryConfig.objects.all()
        return Response(result.values())


class DbTypeConfigViewSet(APIViewSet):
    lookup_field = "db_type_id"

    def list(self, request):
        """
        @api {get} v3/access/db_type/ 获取数据库类型列表
        @apiGroup RawData
        @apiDescription  获取数据库类型列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/db_type/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "created_at": "2018-10-25T19:45:20",
                    "updated_at": null,
                    "created_by": null,
                    "db_type_alias": "mysql",
                    "db_type_name": "mysql",
                    "active": 1,
                    "id": 1,
                    "description": "mysql"
                }
            ],
            "result": true
        }
        """
        result = AccessDbTypeConfig.objects.all()
        return Response(result.values())

    def retrieve(self, request, db_type_id):
        """
        @apiGroup RawData
        @api {get} /v3/access/db_type/:db_type_id/ 获取单个实例详情
        @apiDescription  根据数据库id获取数据库类型信息
        @apiParam {int} db_type_id 数据库类型ID。
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/db_type/1/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "created_at": "2018-10-25T19:45:20",
                    "updated_at": null,
                    "created_by": null,
                    "db_type_alias": "mysql",
                    "db_type_name": "mysql",
                    "active": 1,
                    "id": 1,
                    "description": "mysql"
                }
            ],
            "result": true
        }
        """
        try:
            obj = AccessDbTypeConfig.objects.get(id=db_type_id)
        except AccessRawData.DoesNotExist:
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

        return Response(model_to_dict(obj))


class DataScenarioStorageViewSet(APIViewSet):
    lookup_field = "data_scenario"

    def list(self, request):
        """
        @api {get} v3/access/scenario_storage/?data_scenario= 获取接入场景对应存储
        @apiGroup RawData
        @apiDescription  获取接入场景对应存储
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/scenario_storage?data_scenario=log
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "data_scenario": "log",
                    "created_at": "2018-10-30T11:21:09",
                    "storage_channel_id": 1,
                    "updated_at": null,
                    "created_by": null,
                    "priority": 1,
                    "id": 1,
                    "description": "11"
                },
                {
                    "updated_by": null,
                    "data_scenario": "log",
                    "created_at": "2018-10-30T11:21:09",
                    "storage_channel_id": 2,
                    "updated_at": null,
                    "created_by": null,
                    "priority": 2,
                    "id": 2,
                    "description": "22"
                }
            ],
            "result": true
        }
        """
        param = self.params_valid(serializer=DataScenarioSerializer)
        data_scenario = param.get("data_scenario")

        result = AccessScenarioStorageChannel.objects.filter(data_scenario=data_scenario)
        return Response(result.values())


class CollectorViewSet(APIViewSet):
    lookup_field = "data_scenario"

    @list_route(methods=["get"])
    def check(self, request):
        """
        @api {get} v3/access/collector/check/?task_id=123 查询采集器部署任务状态
        @apiName collector_check
        @apiGroup Collector
        @apiParam {int} task_id 任务id
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "deploy_plans": [
                    {
                        "config": [
                            {
                                "raw_data_id": 100096,
                                "deploy_plan_id": [
                                    187
                                ]
                            }
                        ],
                        "host_list": [
                            {
                                "status": "success",
                                "ip": "x.x.x.x",
                                "error_message": "ok",
                                "bk_cloud_id": 1
                            }
                        ]
                    }
                ],
                "bk_username": "",
                "log": "INFO|2018-12-05 14:47:42|开始接入log数据\nINFO|2018-12-05 14:47:42|检查进程是否存在\nINFO|2018-12-05
                 14:47:43|下发配置\nINFO|2018-12-05 14:47:51|启动采集进程\nINFO|2018-12-05 14:47:58|成功接入log数据"
            },
            "result": true
        }
        """
        task_id = request.query_params["task_id"]
        access_task = AccessTask.objects.get(id=task_id)

        task_status = {
            "status": access_task.result,
            "bk_username": access_task.created_by,
            "log": access_task.logs_en,
            "deploy_plans": json.loads(str(access_task.deploy_plans)),
        }
        return Response(task_status)

    @list_route(methods=["get"])
    def task_log(self, request):
        """
        @api {get} v3/access/collector/task_log/?raw_data_id=123 查询当前数据源历史任务
        @apiName collector_task_log
        @apiGroup Collector
        @apiParam {int} raw_data_id 数据源id
        @apiParam {string} ip 服务器ip
        @apiParam {string} bk_cloud_id 云区域ID
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "recent_log": {
                    "status": "running",
                    "created_at": "2018-12-07T20:02:14.034386",
                    "bk_username": "",
                    "log": "INFO|2018-12-07 20:02:14|开始接入script数据\nINFO|2018-12-07 20:02:18|下发配置",
                    "log_en": "INFO|2018-12-07 20:02:14|开始接入script数据\nINFO|2018-12-07 20:02:18|下发配置"
                },
                "task_log": [
                    {
                        "status": "running",
                        "created_at": "2018-12-07T19:37:29.696696",
                        "bk_username": "",
                        "log": "INFO|2018-12-07 19:37:29|开始接入script数据\nINFO|2018-12-07 19:59:40|下发配置",
                        "log_en": "INFO|2018-12-07 19:37:29|开始接入script数据\nINFO|2018-12-07 19:59:40|下发配置"
                    }
                ],
                "ip_log": {
                    "status": "running",
                    "log": "INFO|2018-12-07 19:37:29|开始接入script数据\nINFO|2018-12-07 19:59:40|下发配置",
                    "ip": "x.x.x.x",
                    "created_at": "2018-12-07T19:37:29.696696",
                    "bk_username": "",
                    "bk_cloud_id": 1
                }
            },
            "result": true
        }
        """
        params = self.params_valid(serializer=AccessTaskLogSerializer)
        raw_data_id = params["raw_data_id"]
        task_ids = AccessRawDataTask.objects.filter(raw_data_id=raw_data_id).values_list("task_id", flat=True)

        op_log = AccessOperationLog.objects.filter(raw_data_id=raw_data_id).order_by("-created_at")
        recent_log = {
            "status": op_log[0].status if op_log else "",
            "bk_username": op_log[0].created_by if op_log else "",
            "log": op_log[0].args if op_log else "",
            "log_en": op_log[0].args if op_log else "",
            "created_at": op_log[0].created_at if op_log else "",
        }

        task_logs_status = list()
        task_logs = AccessTask.objects.filter(id__in=task_ids).order_by("-created_at")
        if task_logs:
            for task_log in task_logs:
                task_logs_status.append(
                    {
                        "status": task_log.result,
                        "bk_username": task_log.created_by,
                        "log": task_log.logs_en if BkLanguage.current_language() == "en" else task_log.logs,
                        "log_en": task_log.logs_en,
                        "created_at": task_log.created_at,
                    }
                )
        return Response(
            {
                "recent_log": recent_log,
                "task_log": task_logs_status,
                "ip_log": "",
            }
        )

    @detail_route(methods=["post"])
    def deploy(self, request, data_scenario):
        """
        @api {post} v3/access/collector/:data_scenario/deploy/ 部署采集器
        @apiName collector_deploy
        @apiGroup Collector
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} bk_username 用户
        @apiParam {int} deploy_plans 部署计划
        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id": 2,
            "bk_username": "admin",
            "remian_deploy_plans": [
                {
                    "system":"linux",
                    "config": [
                        {
                            "raw_data_id": 123,
                            "deploy_plan_id": [
                                1
                            ]
                        }
                    ],
                    "host_list": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ],
            "modified_deploy_plans": [
                {
                    "system":"linux",
                    "config": [
                        {
                            "raw_data_id": 123,
                            "deploy_plan_id": [
                                1
                            ]
                        }
                    ],
                    "host_list": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ]

        }
        """
        logger.info(u"deploy: start to get access factory")
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data_scenario)(access_param=request.data)

        logger.info(u"deploy: start to verify callback parameters")
        collector.valid_callback_param()

        return Response(
            {
                "task_id": collector.action(
                    request.data["bk_biz_id"],
                    request.data["bk_username"],
                    request.data["deploy_plans"],
                    AccessTask.ACTION.DEPLOY,
                    request.data.get("version"),
                )
            }
        )

    @detail_route(methods=["post"])
    def remove(self, request, data_scenario):
        """
        @api {post} v3/access/collector/:data_scenario/remove/ 移除采集器配置
        @apiName collector_remove
        @apiGroup Collector
        @apiParam {int} hosts 主机列表
        @apiParamExample {json} 参数样例:
        同部署采集器
        """
        return Response("remove")

    @detail_route(methods=["post"])
    def start(self, request, data_scenario):
        """
        @api {post} v3/access/collector/:data_scenario/start/ 启动采集器进程
        @apiName collector_start
        @apiGroup Collector
        @apiParam {int} hosts 主机列表
        @apiParamExample {json} 参数样例:
        同部署采集器
        """
        return Response("start")

    @detail_route(methods=["post"])
    def stop(self, request, data_scenario):
        """
        @api {post} v3/access/collector/:data_scenario/deploy/ 部署采集器
        @apiName collector_deploy
        @apiGroup Collector
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} bk_username 用户
        @apiParam {int} deploy_plans 部署计划
        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id": 2,
            "bk_username":"admin",
            "deploy_plans":[
                {
                    "config":[
                        {
                            "raw_data_id": 123,
                             "deploy_plan_id":[
                                59
                            ]
                        }
                    ],
                    "ip_list":[
                        {
                            "bk_cloud_id":1,
                            "ip":"x.x.x.x"
                        }
                    ]
                }
            ]
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "task_id": 1850
            },
            "result": true
        }
        """
        data = request.data
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data_scenario)(access_param=request.data)
        collector.valid_callback_param()
        return Response(
            {
                "task_id": collector.action(
                    data["bk_biz_id"],
                    request.data["bk_username"],
                    data["deploy_plans"],
                    AccessTask.ACTION.STOP,
                    request.data.get("vsersion"),
                )
            }
        )


class CollectorHubPlanViewSet(APIViewSet):
    lookup_field = "raw_data_id"

    @detail_route(methods=["post"])
    @perm_check("raw_data.collect_hub")
    def stop(self, request, raw_data_id):
        """
        @api {post} v3/access/collectorhub/:raw_data_id/stop/ 停止单个采集器
        @apiName collectorhub_stop
        @apiGroup CollectorHub
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {string} bk_username 用户
        @apiParam {string} data_scenario 接入场景
        @apiParam {string} raw_data_id 源数据ID
        @apiParam {string{全量:"full"，增量："incr"}} config_mode 配置模式

        @apiParam {array} scope 接入对象
        @apiParamExample {json} 参数样例:
        {
            "data_scenario": "logbeat",
            "bk_username": "admin",
            "bk_biz_id": 123,
            "config_mode": "full",
            "scope": [
                {
                    "deploy_plan_id": 1,
                    "hosts": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ]
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "stop success",
            "result": true
        }
        """
        # 参数校验
        self.params_valid(serializer=RawDataPermissionSerializer, params={"raw_data_id": raw_data_id})

        data_scenario = self.request.data.get("data_scenario")
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data_scenario)(access_param=request.data)

        self.request.data["raw_data_id"] = raw_data_id
        params = self.params_valid(serializer=ServiceDeploySerializer)

        raw_data = AccessRawData.objects.get(id=raw_data_id)
        params["log"] = u"停止采集，当数据源对应机器全部停止采集将停止分发任务"
        params["description"] = raw_data.description
        params["name"] = data_scenario

        if not collector.has_scope:
            try:
                collector.stop(params)
            except Exception as e:
                # 添加操作历史
                self._create_failure_op_log(params)
                raise e
            # 添加操作历史
            self._create_stopped_op_log(params)
            return Response(True)

        return Response(True)

    @detail_route(methods=["post"])
    @perm_check("raw_data.collect_hub")
    def start(self, request, raw_data_id):
        """
        @api {post} v3/access/collectorhub/:raw_data_id/start/ 启动单个采集器
        @apiName collectorhub_start
        @apiGroup CollectorHub
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {string} bk_username 用户
        @apiParam {string} data_scenario 接入场景
        @apiParam {string{全量:"full"，增量："incr"}} config_mode 配置模式

        @apiParam {array} scope 接入对象
        @apiParamExample {json} 参数样例:
        {
            "data_scenario": "log",
            "bk_username": "admin",
            "bk_biz_id": 123,
            "config_mode": "full",
            "scope": [
                {
                    "deploy_plan_id": 1,
                    "hosts": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ]
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "stop success",
            "result": true
        }
        """
        # 参数校验
        self.params_valid(serializer=RawDataPermissionSerializer, params={"raw_data_id": raw_data_id})

        self.request.data["raw_data_id"] = raw_data_id

        params = self.params_valid(serializer=CollectorHubDeployPlanSerializer)
        data_scenario = self.request.data.pop("data_scenario")
        params["name"] = data_scenario

        raw_data = RawDataHandler(raw_data_id=raw_data_id).retrieve()
        params["log"] = u"启动采集和数据分发任务"
        params["description"] = raw_data.get("description")
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(raw_data["data_scenario"])(
            access_param=request.data
        )

        if not collector.has_scope:
            try:
                collector.start(params)
            except Exception as e:
                # 添加操作历史
                self._create_failure_op_log(params)
                raise e
            # 添加操作历史
            self._create_success_op_log(params)
            return Response(True)

        return Response(True)

    @detail_route(methods=["get"], url_path="status/history")
    @perm_check("raw_data.retrieve")
    def status_history(self, request, raw_data_id):
        """
        @api {get} v3/access/collectorhub/:raw_data_id/status/history/ 查询历史部署状态
        @apiName collectorhub_status_history
        @apiGroup CollectorHub
        @apiParam {int} raw_data_id 源数据ID
        @apiParam {string} bk_username 用户
        @apiParam {string} ip 接入场景
        @apiParam {string} bk_cloud_id 云平台ID

        @apiParamExample {json} 参数样例:
        v3/access/collectorhub/100104/status/history/?ip=x.x.x.x8&bk_username=admin&bk_cloud_id=1
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "error_message": "{\"deploy_plans\": [{\"system\": [\"该字段是必填项。\"]}]}",
                    "deploy_status_display": "失败",
                    "bk_username": "xxxxx",
                    "updated_at": "2018-12-19 21:18:32",
                    "deploy_status": "failure",
                    "type_display": "停止",
                    "type": "stop"
                }
            ],
            "result": true
        }
        """
        # 参数校验
        query_params = self.request.query_params.dict()
        query_params["raw_data_id"] = raw_data_id

        serializer = CollectorHubQueryHistorySerializer(data=query_params)
        if not serializer.is_valid():
            raise ValidationError(message=json.dumps(serializer.errors).decode("unicode_escape"))

        # 先尝试读取节点管理结果, 若不存在, 再从collectorhub读取
        res = bknode.history(raw_data_id, query_params)

        return Response(res)

    @detail_route(methods=["get"])
    @perm_check("raw_data.retrieve")
    def status(self, request, raw_data_id):
        """
        @api {get} v3/access/collectorhub/:raw_data_id/status/ 查询部署状态
        @apiName collectorhub_status
        @apiGroup CollectorHub
        @apiParam {int} raw_data_id 源数据ID
        @apiParam {string} bk_username 用户
        @apiParam {string} [ip] 接入场景
        @apiParam {string} [bk_cloud_id] 云平台ID
        @apiParam {string} [ordering] 关键字排序
        @apiParam {int{默认值"0",0:升序排列; 1:降序排列, 当配置ordering时生效}} [descending] 排序
        @apiParam {string{允许值: "pending", "running", "waiting", "success", "failure", "stopped"}} [deploy_status] 部署状态
        @apiParam {int{>=0}} page 页码
        @apiParam {int{>=0}} page_size 页面大小

        @apiParamExample {json} 参数样例:
        v3/access/collectorhub/100104/status/?ip=x.x.x.x8&bk_username=admin&bk_cloud_id=1
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "ip": "x.x.x.x",
                    "deploy_status_display": "失败",
                    "collector_status": "",
                    "scope_type": "module",
                    "deploy_status": "failure",
                    "collector_status_display": "",
                    "bk_cloud_id": 1,
                    "updated_at": "2018-12-19 16:52:19",
                    "scope_object": {
                        "bk_inst_name_list": [
                            "testdev",
                            "testdev-test-collecotr-server"
                        ],
                        "bk_obj_id": "module",
                        "bk_inst_id": 399534
                    },
                    "deploy_plan_id": 195
                }
            ],
            "result": true
        }
        """
        # 参数校验
        query_params = self.request.query_params.dict()
        query_params["raw_data_id"] = raw_data_id

        serializer = CollectorHubQueryStatusSerializer(data=query_params)
        if not serializer.is_valid():
            raise ValidationError(message=json.dumps(serializer.errors).decode("unicode_escape"))

        # 先尝试读取节点管理结果, 若不存在, 再从collectorhub读取
        res = bknode.show_status(raw_data_id, query_params)
        return Response(res)

    @detail_route(methods=["get"])
    @perm_check("raw_data.retrieve")
    def summary(self, request, raw_data_id):
        """
        @api {get} v3/access/collectorhub/:raw_data_id/summary/ 查询部署状态统计
        @apiName collectorhub_summary
        @apiGroup CollectorHub
        @apiParam {int} raw_data_id 源数据ID

        @apiParamExample {json} 参数样例:
        /v3/access/collectorhub/100104/summary/?bk_username=xxxx
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "deploy_plans": [
                    {
                        "deploy_plan_id": 195,
                        "module": [
                            {
                                "bk_object_id": "module",
                                "bk_instance_id": 399534,
                                "summary": {
                                    "failure": 2,
                                    "total": 2
                                }
                            }
                        ],
                        "summary": {
                            "failure": 2,
                            "total": 2
                        }
                    },
                    {
                        "deploy_plan_id": 195,
                        "summary": {
                            "failure": 2,
                            "total": 2
                        }
                    }
                ],
                "summary": {
                    "failure": 4,
                    "total": 4
                }
            },
            "result": true
        }
        """
        raw_data_id = int(raw_data_id)
        query_params = self.params_valid(serializer=CollectorHubSumarySerializer)

        # 先尝试读取节点管理结果, 若不存在, 再从collectorhub读取
        ret = bknode.summary_status(raw_data_id, query_params)
        if ret is not None:
            return Response(ret)

        query_params["raw_data_id"] = raw_data_id
        raw_data = RawDataHandler(raw_data_id=raw_data_id).retrieve()
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(raw_data["data_scenario"])()

        # 若有部署计划, 从collectorhub中查询执行结果
        if collector.has_scope:
            result_status = []
        else:  # and not collector.callback_status
            # 若没有部署计划, 从部署日志中判断结果
            op_log = AccessOperationLog.objects.filter(raw_data_id=raw_data_id).order_by("-created_at")
            task_disable = collector.task_disable

            result_status = {
                "summary": {"failure": 1, "total": 1},
                # 如果不填则启用
                "task_disable": task_disable,
                "deploy_status_display": _(u"异常"),
                "deploy_status": AccessOperationLog.STATUS.FAILURE,
            }
            if op_log:
                del result_status["summary"]["failure"]
                if op_log[0].status in [
                    AccessOperationLog.STATUS.SUCCESS,
                    AccessOperationLog.STATUS.RUNNING,
                ]:
                    if collector.is_disposable:
                        result_status["deploy_status_display"] = _(u"完成")
                        result_status["deploy_status"] = "finish"
                        result_status["summary"]["finish"] = 1
                    else:
                        result_status["deploy_status_display"] = _(u"正常")
                        result_status["deploy_status"] = op_log[0].status
                        result_status["summary"][op_log[0].status] = 1
                elif op_log[0].status == AccessOperationLog.STATUS.STOPPED:
                    result_status["deploy_status_display"] = _(u"停止")
                    result_status["deploy_status"] = op_log[0].status
                    result_status["summary"][op_log[0].status] = 1

        # 追加场景信息
        extra = collector.attribute()
        if extra:
            result_status["attribute"] = extra

        return Response(result_status)

    def _create_success_op_log(self, params):
        self._create_op_log(params, AccessOperationLog.STATUS.SUCCESS)

    def _create_stopped_op_log(self, params):
        self._create_op_log(params, AccessOperationLog.STATUS.STOPPED)

    def _create_failure_op_log(self, params):
        self._create_op_log(params, AccessOperationLog.STATUS.FAILURE)

    def _create_op_log(self, params, status):
        """
        更新操作日志
        :param params:
        :param status: 状态
        :return:
        """
        # 添加操作历史
        operation_log = AccessOperationLog.objects.create(
            raw_data_id=params["raw_data_id"],
            args=json.dumps(params),
            created_by=self.request.data["bk_username"],
            updated_by=self.request.data["bk_username"],
            status=status,
        )
        return operation_log.id


class CheckViewSet(APIViewSet):
    lookup_field = "data_scenario"

    @detail_route(methods=["post"])
    def check(self, request, data_scenario):
        """
        @api {post} /v3/access/collector/:data_scenario/check/ 探测db接口
        @apiName collector_check_db
        @apiGroup collector_check
        @apiDescription 探测db
        @apiParam {string} db_host 数据库主机
        @apiParam {string} db_port 数据库端口
        @apiParam {string} db_user 用户名
        @apiParam {string} db_pass 密码
        @apiParam {string{op_type为"tb"时为必填}} [db_name] 数据库名称
        @apiParam {string} bk_username 操作人
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} bk_biz_id 业务ID
        @apiParam {int} [counts] 条数
        @apiParam {string{"db":尝试获取db列表,"tb":尝试获取表列表,"field":尝试获取字段属性,"rows":尝试获取数据例子}} op_type 操作类型

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} 参数样例:
        {
            "db_host":"x.x.x.x",
            "db_port":"xxxx",
            "db_name":"xxxxx",
            "db_user":"xxxx",
            "db_pass":"xxxxx",
            "bk_username":"xxxxx",
            "bk_biz_id":"xxx",
            "op_type":"xxx"
        }

        @apiSuccessExample {json} Success-Response:
        /v3/access/collector/db/check/
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                "information_schema",
                "bkdata_api_default",
                "bkdata_basic",
                "bkdata_druid",
                "bkdata_flow",
                "bkdata_jobnavi",
                "bkdata_lab",
                "bkdata_log",
                "bkdata_meta",
                "test",
                ""
            ],
            "result": true
        }
        # 请求为field时,返回格式
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "Extra": "auto_increment",
                    "Default": null,
                    "Field": "id",
                    "Key": "PRI",
                    "Null": "NO",
                    "Type": "int(11)"
                }
            ],
            "result": true
        }
        """
        """
        @api {post} v3/access/collector/:data_scenario/check/ 探测http接口
        @apiName collector_check_http
        @apiGroup collector_check
        @apiDescription 探测http

        @apiParam {string} url 请求url
        @apiParam {string} method 请求方法
        @apiParam {string{method为'post'时为必填}} [body] 请求body(默认json)
        @apiParam {string} bk_username 操作人
        @apiParam {string} [time_format] 时间格式

        @apiParam {string} bk_biz_id 业务ID
        @apiParam {string{合法接入场景}} [data_scenario] 接入场景

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} 参数样例:
        v3/access/collector/http/check/
        {
            "url":"http://x.x.x.x.x/v3/x/x/?created_at=<begin>&page=1&page_size=1",
            "method":"get",
            "bk_username":"xxxxx",
            "bk_biz_id":"591"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "bk_biz_id": 10,
                    "topic": "bk_test10",
                    "data_source": "svr",
                    "maintainer": "",
                    "updated_by": null,
                    "raw_data_name": "bk_test",
                    "storage_partitions": 1,
                    "created_at": "2018-10-23T19:48:49",
                    "storage_channel_id": 11,
                    "data_encoding": "UTF-8",
                    "raw_data_alias": "bk_test",
                    "updated_at": "2018-10-31T15:23:59",
                    "bk_app_code": "bk_log_search",
                    "data_scenario": "log",
                    "created_by": "admin",
                    "id": 1,
                    "bk_biz_name": "",
                    "sensitivity": "private",
                    "description": "desc"
                }
            ],
            "result": true
        }
        """
        """
        @api {post} v3/access/collector/:data_scenario/check/ 探测script接口
        @apiName collector_check_script
        @apiGroup collector_check
        @apiDescription 探测script

        @apiParam {string} [raw_data_name] 源数据名称
        @apiParam {string} content 脚本内容
        @apiParam {string} bk_username 操作人

        @apiParam {string} bk_biz_id 业务ID
        @apiParam {array{host_scope,module_scope不能同时为空}} host_scope
        @apiParam {array{host_scope,module_scope不能同时为空}} module_scope

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} 参数样例:
        v3/access/collector/http/check/
        {
            "bk_username": "xxxx",
            "bk_biz_id": 2,
            "raw_data_name": "script_06",
            "content": "INSERT_METRIC test3  789 \nCOMMIT",
            module_scope": [{
                                "bk_obj_id": "module",
                                "bk_inst_id": 399534
                            }],
            "host_scope": [
                            {
                                "bk_cloud_id": 1,
                                 "ip":"x.x.x.x"
                            }
            ]
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "success_hosts": [
                    {
                        "msg": {
                            "_utctime_": "2019-01-06 09:12:25",
                            "_time_": "2019-01-06 17:12:25",
                            "_company_id_": 0,
                            "timestamp": "1546765945",
                            "metric": {
                                "test3": "789",
                                "test4": "789"
                            },
                            "_server_": "x.x.x.x",
                            "_plat_id_": 0
                        },
                        "ip": "x.x.x.x",
                        "status": "success"
                    }
                ],
                "error_hosts": [
                    {
                        "msg": "执行探测脚本失败",
                        "ip": "x.x.x.x",
                        "status": "failure"
                    }
                ]
            },
            "result": true
        }
        """
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data_scenario)(access_param=self.request.data)
        exc_result = collector.check()
        return Response(exc_result)


class AppKeyViewSet(APIViewSet):
    lookup_field = "bk_biz_id"

    def list(self, request):
        """
        @api {post} v3/access/collector/app_key/ 根据bk_biz_id获取appkey列表
        @apiName collector_list_appkey
        @apiGroup collector_list
        @apiDescription 根据bk_biz_id获取appkey列表
        @apiParam {string} bk_biz_id 业务id

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} 参数样例:
        v3/access/collector/app_key/?bk_biz_id=591
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                "112566565"
            ],
            "result": true
        }
        """
        if not self.request.query_params.get("bk_biz_id"):
            raise ValidationError(message=_(u"参数校验不通过:bk_biz_id为必填参数"), errors=u"bk_biz_id为必填参数")
        bk_biz_id = self.request.query_params.get("bk_biz_id")

        check_perm_by_scenario("beacon", bk_biz_id, get_request_username())
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario("beacon")(access_param=self.request.query_params)
        result = collector.app_key()
        return Response(result)


class FileViewSet(APIViewSet):
    lookup_field = "bk_biz_id"

    def create(self, request):
        """
         @api {post} v3/access/collector/upload/ 上传文件
         @apiName collector_file_upload
         @apiGroup CollectorDeployPlan
         @apiDescription 上传文件
         @apiParam {string} bk_biz_id 业务id
         @apiParam {string} file_data 文件

         @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
         @apiError (错误码) 1500500  <code>服务异常</code> .
         @apiError (错误码) 1500405  <code>请求方法错误</code> .

         @apiParamExample {json} 参数样例:
        /v3/access/collector/upload/
         @apiSuccessExample {json} Success-Response:
         HTTP/1.1 200 OK
         {
             "errors": null,
             "message": "ok",
             "code": "1500200",
             "data": {
                 "file_name": "file_591_测试_test01_null.xlsx"
             },
             "result": true
         }
        """
        self.params_valid(serializer=FileUploadSerializer)
        # _file = self.request.FILES['file_data']
        _file = self.request.FILES["file_data"]
        file_name = _file.name
        try:
            path = dataapi_settings.UPLOAD_MEDIA_ROOT.strip()
            path = path.rstrip("\\")
            isExists = os.path.exists(path)
            if not isExists:
                os.makedirs(path)
            file_name = u"file_{}_{}".format(self.request.data[BK_BIZ_ID], file_name)
            path = u"{}/{}".format(dataapi_settings.UPLOAD_MEDIA_ROOT, file_name)
            dest = open(path, "w")
            if _file.multiple_chunks:
                for c in _file.chunks():
                    dest.write(c)
            else:
                dest.write(_file.read())
            dest.close()
            return Response({FILE_NAME: file_name})
        except Exception as e:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_UPLOAD_FILE_FAIL, errors=u"%s" % e)

    @list_route(methods=["post"], url_path="part")
    def part(self, request):
        """
         @api {post} v3/access/collector/upload/part/ 上传文件
         @apiName collector_file_upload
         @apiGroup CollectorDeployPlan
         @apiDescription 上传分片文件
         @apiParam {string} bk_biz_id 业务id
         @apiParam {string} file_data 文件

         @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
         @apiError (错误码) 1500500  <code>服务异常</code> .
         @apiError (错误码) 1500405  <code>请求方法错误</code> .

         @apiParamExample {json} 参数样例:
        /v3/access/collector/upload/part/
         @apiSuccessExample {json} Success-Response:
         HTTP/1.1 200 OK
         {
             "errors": null,
             "message": "ok",
             "code": "1500200",
             "data": {
                 "file_name": "file_591_part-1"
             },
             "result": true
         }
        """
        params = self.params_valid(serializer=FilePartUploadSerializer)
        try:
            _file = self.request.FILES[FILE]
            data = _file.read()
            # 获取探索专用hdfs
            file_name = "{}-{}".format(params[UPLOAD_UUID], params[TASK_NAME])
            hdfs_config = get_default_storage()
            hdfs_upload_path = get_hdfs_upload_path_chunk(params[BK_BIZ_ID], file_name, params[CHUNK_ID])
            hdfs_host = hdfs_config[CONNECTION][HOSTS]
            hdfs_port = hdfs_config[CONNECTION][PORT]
            create_and_write_file(hdfs_host, hdfs_port, hdfs_upload_path, data)
            return Response({FILE_NAME: params[TASK_NAME]})
        except Exception as e:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_UPLOAD_FILE_FAIL, errors=u"%s" % e)

    @list_route(methods=["post"], url_path="merge")
    def merge(self, request):
        """
         @api {post} v3/access/collector/upload/merge/ 上传文件
         @apiName collector_file_upload
         @apiGroup CollectorDeployPlan
         @apiDescription 上传分片文件
         @apiParam {string} bk_biz_id 业务id
         @apiParam {string} file_data 文件

         @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
         @apiError (错误码) 1500500  <code>服务异常</code> .
         @apiError (错误码) 1500405  <code>请求方法错误</code> .

         @apiParamExample {json} 参数样例:
        /v3/access/collector/upload/merge/
         @apiSuccessExample {json} Success-Response:
         HTTP/1.1 200 OK
         {
             "errors": null,
             "message": "ok",
             "code": "1500200",
             "data": {
                 "file_name": "file_591_part-1"
             },
             "result": true
         }
        """
        params = self.params_valid(serializer=FileMergeUploadSerializer)
        try:
            # 获取merge后的路径
            hdfs_config = get_default_storage()
            file_name = "{}-{}".format(params[UPLOAD_UUID], params[TASK_NAME])
            tmp_upload_path = get_hdfs_upload_path_chunk(params[BK_BIZ_ID], file_name)
            hdfs_upload_path = get_hdfs_upload_path(params[BK_BIZ_ID], file_name)
            hdfs_host = hdfs_config[CONNECTION][HOSTS]
            hdfs_port = hdfs_config[CONNECTION][PORT]
            write_md5 = load_and_merge_file(
                hdfs_host,
                hdfs_port,
                hdfs_upload_path,
                tmp_upload_path,
                params[CHUNK_SIZE],
            )
            if params[MD5] != write_md5:
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_UPLOAD_FILE_FAIL,
                    errors=u"md5 check failed! request md5:{}, real md5:{}".format(params[MD5], write_md5),
                )

            # 清理临时文件夹
            remove_dir(hdfs_host, hdfs_port, tmp_upload_path)
            return Response({TASK_NAME: file_name})
        except Exception as e:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_UPLOAD_FILE_FAIL, errors=u"%s" % e)


class FileRawDataViewSet(APIViewSet):
    lookup_field = RAW_DATA_ID

    @detail_route(methods=["get"], url_path="get_hdfs_info")
    def get_hdfs_info(self, request, raw_data_id):
        """
         @api {get} v3/access/collector/{raw_data_id}/upload/get_hdfs_info/获取相关文件的hdfs集群地址信息
         @apiName get_hdfs_info
         @apiGroup CollectorDeployPlan
         @apiDescription 相关文件的hdfs集群地址信息
         @apiParam {string} raw_data_id
         @apiParam {string} file_name 文件名

         @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
         @apiError (错误码) 1500500  <code>服务异常</code> .
         @apiError (错误码) 1500405  <code>请求方法错误</code> .

         @apiParamExample {json} 参数样例:
        /v3/access/collector/10000/upload/get_hdfs_info/
         @apiSuccessExample {json} Success-Response:
         HTTP/1.1 200 OK
         {
             "errors": null,
             "message": "ok",
             "code": "1500200",
             "data": {
                 "file_name": "file_591_part-1"
             },
             "result": true
         }
        """
        try:
            params = self.params_valid(serializer=FileHdfsInfoSerializer)
            resource = AccessResourceInfo.objects.get(raw_data_id=int(raw_data_id))
            if not resource:
                raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)

            raw_data = AccessRawData.objects.get(id=raw_data_id)

            resource_info = json.loads(resource.resource)
            resource_info[RAW_DATA_ID] = resource.raw_data_id
            resource_info[BK_BIZ_ID] = raw_data.bk_biz_id
            raw_data = AccessRawData.objects.get(id=resource.raw_data_id)
            if resource_info[FILE_NAME] == params[FILE_NAME] and raw_data.id == int(raw_data_id):
                return Response(resource_info)

            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)
        except Exception as e:
            logger.error(e)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)


class AccessHostConfigViewSet(APIViewSet):
    lookup_field = "data_scenario"

    def list(self, request):
        """
        @api {get} v3/access/host_config/ 获取采集器服务端配置列表
        @apiGroup RawData
        @apiDescription   获取采集器服务端配置列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParam {string} data_scenario 接入场景
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/host_config/?data_scenario=log
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "source": 1,
                    "data_scenario": "http",
                    "ip": "x.x.x.x",
                    "created_at": "2018-11-13T17:29:36",
                    "updated_at": "2018-11-23T17:13:15",
                    "created_by": null,
                    "action": "deploy",
                    "ext": null,
                    "operator": "admin",
                    "id": 11,
                    "description": "http接入host信息"
                }
            ],
            "result": true
        }
        """
        # 参数校验
        param = self.params_valid(serializer=DataScenarioSerializer)

        host_configs = AccessHostConfig.objects.filter(data_scenario=param.get("data_scenario"))
        if host_configs:
            return Response(host_configs.values())

        return Response([])


class MigrationViewSet(APIViewSet):
    lookup_field = "data_scenario"

    @detail_route(methods=["post"])
    def deploy(self, request, data_scenario):
        """
        @api {post} v3/access/migration/:data_scenario/deploy/ 部署采集器
        @apiName collector_deploy
        @apiGroup Collector
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} bk_username 用户
        @apiParam {int} deploy_plans 部署计划
        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id": 2,
            "bk_username": "admin",
            "remian_deploy_plans": [
                {
                    "system":"linux",
                    "config": [
                        {
                            "raw_data_id": 123,
                            "deploy_plan_id": [
                                1
                            ]
                        }
                    ],
                    "host_list": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ],
            "modified_deploy_plans": [
                {
                    "system":"linux",
                    "config": [
                        {
                            "raw_data_id": 123,
                            "deploy_plan_id": [
                                1
                            ]
                        }
                    ],
                    "host_list": [
                        {
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }
                    ]
                }
            ]

        }
        """
        logger.info("Migration deploy: start migration")

        deploy_plans = request.data["deploy_plans"]
        version = request.data.get("version")
        bk_biz_id = request.data["bk_biz_id"]
        bk_username = request.data["bk_username"]
        # 创建部署任务
        collector_factory = CollectorFactory.get_collector_factory()
        bk_biz_id, bk_username = collector_factory.get_collector_by_data_scenario(data_scenario).scenario_biz_info(
            bk_biz_id, bk_username
        )

        logger.info(u"Migration action: successfully create a deployment task and initialize the ip status")
        for _deploy_plan in deploy_plans:
            _deploy_plan["version"] = version if version else dataapi_settings.LOGC_COLLECTOR_INNER_PACKAGE_VERSION
            for _host in _deploy_plan.get("host_list", []):
                _host["status"] = "success"
                _host["error_message"] = u"完成"

        task_id = AccessTask.objects.create(
            bk_biz_id=bk_biz_id,
            data_scenario=data_scenario,
            action=AccessTask.ACTION.DEPLOY,
            result=AccessTask.STATUS.SUCCESS,
            logs=u"迁移完成",
            created_by=bk_username,
            updated_by=bk_username,
            deploy_plans=json.dumps(deploy_plans),
        ).id

        # 创建数据源任务关系
        raw_data_tasks = list()
        raw_data_ids = dict()
        for _deploy_plan in deploy_plans:
            if _deploy_plan.get("config"):
                for _config in _deploy_plan.get("config"):
                    raw_data_ids[_config["raw_data_id"]] = task_id

        for k, v in raw_data_ids.items():
            raw_data_tasks.append(
                AccessRawDataTask(
                    task_id=task_id,
                    raw_data_id=k,
                    created_by=bk_username,
                    updated_by=bk_username,
                    description=AccessTask.ACTION.DEPLOY,
                )
            )
        AccessRawDataTask.objects.bulk_create(raw_data_tasks)

        return Response({"task_id": task_id})
