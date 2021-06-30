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

import base64
import json
import time
from datetime import datetime

from common.base_utils import model_to_dict
from common.decorators import detail_route, list_route
from common.django_utils import DataResponse
from common.local import get_request_username
from common.views import APIViewSet
from conf.dataapi_settings import (
    AVAILABLE_STORAGE,
    HTTP_SCHEMA,
    QUERYENGINEWEB_FQDN,
    RESULT_LIMIT,
)
from datalab.api.queryengine import QueryEngineApi
from datalab.constants import (
    _STAGES,
    CHART_CONFIG,
    CODE,
    CONNECT_DB,
    ERROR,
    ERROR_MESSAGE,
    EXAMPLE,
    EXAMPLE_RETURN_VALUE,
    EXECUTE_TIME,
    EXPLAIN,
    FAILED,
    FIELD_NAME,
    FINISHED,
    FUNC_ALIAS,
    FUNC_GROUP,
    FUNC_INFO,
    FUNC_NAME,
    FUNCTION_GROUP_INFO,
    INFO,
    LIMIT,
    LIST,
    MESSAGE,
    NAME,
    PAGE,
    PAGE_SIZE,
    PERSONAL,
    PROJECT_ID,
    PROJECT_TYPE,
    QUERY_DB,
    QUERY_ID,
    QUERY_NAME,
    RUNNING,
    SCHEMA_INFO,
    SELECT_FIELDS_ORDER,
    SIZE,
    SQL,
    SQL_EXAMPLE,
    STAGE_COST_TIME,
    STAGE_STATUS,
    STAGE_TYPE,
    STAGES,
    STAGES_DESCRIPTION,
    STATUS,
    SUBMIT_QUERY,
    SUBMIT_QUERY_LIST,
    SUPPORT_STORAGE,
    TOTALRECORDS,
    USAGE,
    WRITE_CACHE,
)
from datalab.exceptions import DownloadError, QueryTaskNotFoundError
from datalab.queries.models import (
    DatalabBKSqlFunctionModel,
    DatalabHistoryTaskModel,
    DatalabQueryTaskModel,
)
from datalab.queries.serializers import QueryTaskModelSerializer
from datalab.queries.utils import check_query_name_exists, create_query_name, sql_query
from datalab.utils import check_project_auth, check_required_params
from django.utils.translation import ugettext as _
from rest_framework.response import Response


class DatalabQueryTaskViewSet(APIViewSet):

    # 查询任务的id，用于唯一确定一个查询任务
    lookup_field = "query_id"
    serializer_class = QueryTaskModelSerializer

    @check_required_params([PROJECT_ID])
    def create(self, request):
        """
        @api {post} /v3/datalab/queries/ 创建查询任务
        @apiName create_query_task
        @apiGroup queries
        @apiDescription 创建查询任务
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal

        @apiParamExample {json} 参数样例:
        {
            "project_id": 11,
            "project_type": "personal"
        }

        @apiSuccess {int} query_id 查询任务id
        @apiSuccess {string} query_name 查询任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} sql_text sql文本
        @apiSuccess {string} query_task_id sql查询作业id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "data": {
                "query_id": 111,
                "query_name": "task_2",
                "project_id": 11,
                "project_type": "personal",
                "sql_text": "",
                "query_task_id": "",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "",
                "updated_by": "",
                "description": ""
            },
            "result": true,
        }
        """
        params = self.request.data
        project_id = params[PROJECT_ID]
        # 如果传参project_type没有指定，默认为个人项目
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        check_project_auth(project_type, project_id)
        # 获取此项目下的所有任务
        objs = list(DatalabQueryTaskModel.objects.filter(project_id=project_id, project_type=project_type).values())
        # 创建查询任务名
        query_name = create_query_name(objs)
        bk_username = get_request_username()
        obj = DatalabQueryTaskModel.objects.create(
            project_id=project_id,
            project_type=project_type,
            query_name=query_name,
            lock_user=bk_username,
            lock_time=datetime.now(),
            created_by=bk_username,
        )
        result = model_to_dict(obj)
        return Response(result)

    def retrieve(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/ 获取查询任务详情
        @apiName get_query_task
        @apiGroup queries
        @apiDescription 获取查询任务详情
        @apiParam {int} query_id 查询任务id

        @apiSuccess {int} query_id 查询任务id
        @apiSuccess {string} query_name 查询任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} sql_text sql文本
        @apiSuccess {string} query_task_id sql查询作业id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "query_id": 111,
                "query_name": "task_1"
                "project_id": 11,
                "project_type": "personal",
                "sql_text": "select xx from table1",
                "query_task_id": "BK_168",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "2019-10-28 00:00:00",
                "updated_by": "yyy",
                "description": ""
            },
            "result": true
        }
        """
        try:
            obj = DatalabQueryTaskModel.objects.get(query_id=query_id)
            result = model_to_dict(obj)
            return Response(result)
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError

    def update(self, request, query_id):
        """
        @api {put} /v3/datalab/queries/:query_id/ 更新查询任务
        @apiName update_query_task
        @apiGroup queries
        @apiDescription 更新查询任务
        @apiParam {int} query_id 查询任务id
        @apiParam {string} [sql] 查询sql，表示修改sql操作
        @apiParam {string} [query_name] 新的查询任务名，表示重命名操作

        @apiParamExample {json} 参数样例:
        {
            "sql": "select * from table2"
        }

        @apiSuccess {int} query_id 查询任务id
        @apiSuccess {string} query_name 查询任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} sql_text sql文本
        @apiSuccess {string} query_task_id sql查询作业id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "result":true,
            "data":{
                "query_id": 112,
                "query_name": "task_2"
                "project_id": 11,
                "project_type": "personal",
                "sql_text": "select yy from table2",
                "query_task_id": "BK_681",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "2019-10-28 00:00:00",
                "updated_by": "yyy",
                "description": ""
            },
            "code":"1500200",
            "message":"ok",
            "errors": null
        }
        """
        params = self.request.data
        bk_username = get_request_username()
        try:
            query_task_obj = DatalabQueryTaskModel.objects.get(query_id=query_id)
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError
        check_project_auth(query_task_obj.project_type, query_task_obj.project_id)
        query_task_obj.updated_by = bk_username
        # 保存并执行sql
        if SQL in params:
            # 先更新sql和query_task_id，更新query_task_id=''的目的在于把上次执行成功的query_task_id先置空，防止queryengine查询报错
            query_task_obj.sql_text, query_task_obj.query_task_id = params[SQL], ""
            query_task_obj.save()

            response = sql_query(params, query_task_obj)
            query_task_id = (
                response.data.get(QUERY_ID)
                if response.is_success()
                else response.errors.get(QUERY_ID)
                if response.errors
                else ""
            )
            if query_task_id:
                query_task_obj.query_task_id = query_task_id
                # 更新查询历史表
                DatalabHistoryTaskModel.objects.create(
                    query_id=query_id, query_task_id=query_task_id, created_by=bk_username
                )
            if not response.is_success():
                return DataResponse(
                    result=False, data=None, message=response.message, code=response.code, errors=response.errors
                )
        # 重命名查询任务
        elif QUERY_NAME in params:
            project_id = query_task_obj.project_id
            check_query_name_exists(project_id, params[QUERY_NAME])
            query_task_obj.query_name = params[QUERY_NAME]
        # 更新查询结果集图表配置
        elif CHART_CONFIG in params:
            query_task_obj.chart_config = params[CHART_CONFIG]
        query_task_obj.save()

        result = model_to_dict(query_task_obj)
        return Response(result)

    def destroy(self, request, query_id):
        """
        @api {delete} /v3/datalab/queries/:query_id/ 删除查询任务
        @apiName delete_query_task
        @apiGroup queries
        @apiDescription 删除查询任务
        @apiParam {int} query_id 查询任务id

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
        try:
            obj = DatalabQueryTaskModel.objects.get(query_id=query_id)
            check_project_auth(obj.project_type, obj.project_id)
            obj.delete()
            return Response(True)
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError

    @detail_route(methods=["get"], url_path="history")
    @check_required_params([PAGE, PAGE_SIZE])
    def history(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/history/ 查看查询历史
        @apiName get_query_history
        @apiGroup queries
        @apiDescription 查看查询历史
        @apiParam {int} query_id 查询任务id
        @apiParam {int} page 分页码
        @apiParam {int} page_size 每页显示的条数

        @apiSuccess {int} total_page 总页码
        @apiSuccess {int} count 总条数
        @apiSuccess {int} query_id sql查询作业id
        @apiSuccess {string} sql_text sql文本
        @apiSuccess {string} query_name 查询任务名称
        @apiSuccess {string} query_method sql查询作业类型
        @apiSuccess {string} prefer_storage sql查询关联的存储
        @apiSuccess {string} query_start_time 查询提交时间，精读：毫秒
        @apiSuccess {string} query_end_time 查询结束时间，精读：毫秒
        @apiSuccess {int} cost_time 查询耗时，单位：毫秒
        @apiSuccess {int} total_records 返回记录数
        @apiSuccess {string} query_state sql任务状态
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "data": {
                "total_page": 1,
                "count": 2,
                "results":
                    [
                        {
                            "cost_time": 595,
                            "description": "query_async",
                            "query_state": "finished",
                            "total_records": 1000,
                            "created_at": "2019-12-20 10:33:22.0",
                            "updated_at": "2019-12-20 10:33:28.0",
                            "created_by": "xx",
                            "properties": null,
                            "query_method": "async",
                            "query_end_time": "2019-12-20 10:33:28.246",
                            "query_id": "XXX",
                            "prefer_storage": "hdfs",
                            "sql_text": "SELECT * FROM xx WHERE thedate>='20191213' AND thedate<='20191213' LIMIT 1000",
                            "query_start_time": "2019-12-20 10:33:22.238",
                            "active": 0,
                            "routing_id": 0,
                            "id": 1,
                            "eval_result": "0",
                            "updated_by": "xx"
                        },
                        {
                            "cost_time": 3453,
                            "description": "query_async",
                            "query_state": "failed",
                            "total_records": 1000,
                            "created_at": "2019-12-20 10:32:56.0",
                            "updated_at": "2019-12-20 10:32:58.0",
                            "created_by": "xx",
                            "properties": null,
                            "query_method": "async",
                            "query_end_time": "2019-12-20 10:33:28.246",
                            "query_id": "XXX",
                            "prefer_storage": "hdfs",
                            "sql_text": "SELECT * FROM xx WHERE thedate>='20191213' AND thedate<='20191213' LIMIT 1000",
                            "query_start_time": "2019-12-20 10:32:55.543",
                            "active": 0,
                            "routing_id": 0,
                            "id": 2,
                            "eval_result": "0",
                            "updated_by": "xx"
                        }
                    ]
            },
            "result": true,
        }
        """
        page = int(request.query_params[PAGE])
        page_size = int(request.query_params[PAGE_SIZE])
        offset = (page - 1) * page_size

        objs = DatalabHistoryTaskModel.objects.filter(query_id=query_id).order_by("-updated_at")
        query_id_list = [obj.query_task_id for obj in objs[offset : offset + page_size]]

        request_data = {"query_id_list": query_id_list}
        res = QueryEngineApi.get_info_list(request_data)

        # 整除向上取整
        total_page = (objs.count() + page_size - 1) / page_size
        results = res.data if res.data else []
        data = {"total_page": total_page, "count": objs.count(), "results": results}
        return Response(data)

    @detail_route(methods=["get"], url_path="result")
    def result(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/result/ 查看查询结果集
        @apiName get_query_result
        @apiGroup queries
        @apiDescription 查看查询结果集
        @apiParam {string} query_id 查询任务id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "select_fields_order":[
                    "thedate",
                    "dtEventTime",
                    "dtEventTimeStamp",
                    "localTime",
                    "dtTime",
                    "online",
                    "data_cnt"
                ],
                "list":[
                    {
                        "thedate": 20191014,
                        "dtEventTime": "2019-10-14 23:57:55",
                        "dtEventTimeStamp": 1571068675000,
                        "localTime": "2019-10-15 00:00:00",
                        "dtTime": "2019-10-14 23:57:55",
                        "online": 255741.2,
                        "data_cnt": 25.0
                    },
                    {
                        "thedate": 20191014,
                        "dtEventTime": "2019-10-14 23:52:50",
                        "dtEventTimeStamp": 1571068370000,
                        "localTime": "2019-10-14 23:55:00",
                        "dtTime": "2019-10-14 23:52:50",
                        "online": 267782.0,
                        "data_cnt": 25.0
                    }
                ],
                "totalRecords": 2,
                "timetaken": {
                    "sqlParse": 408,
                    "checkQuerySemantic": 531,
                    "timetaken": 1659,
                    "getQueryDriver": 6,
                    "pickValidStorage": 363,
                    "getStorageLoad": 6,
                    "executeQueryRule": 6,
                    "queryDb": 9,
                    "connectDb": 26,
                    "checkPermission": 288,
                    "calQueryExecCost": 6,
                    "checkQuerySyntax": 10
                }
            }
        }
        """
        result_limit = request.query_params.get(LIMIT, None)
        try:
            query_task_obj = DatalabQueryTaskModel.objects.get(query_id=query_id)
            query_task_id = query_task_obj.query_task_id
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError
        if not query_task_id:
            return Response()
        res = QueryEngineApi.get_result.retrieve({QUERY_ID: query_task_id, SIZE: RESULT_LIMIT}, raise_exception=True)
        data = res.data
        if data and data.get(STATUS) == FINISHED:
            data[LIST], data[INFO] = (
                (data[LIST][:RESULT_LIMIT], _("页面最多展示%s条数据，请使用SQL进一步检索") % RESULT_LIMIT)
                if result_limit and data[TOTALRECORDS] > RESULT_LIMIT
                else (data[LIST], None)
            )

            # 查询无数据时不存在parquet文件，所以需调用schema接口获取表头信息进行填充
            if data[TOTALRECORDS] == 0:
                schema_res = QueryEngineApi.get_schema.retrieve({QUERY_ID: query_task_id}, raise_exception=True)
                schema_info = json.loads(schema_res.data.get(SCHEMA_INFO))
                if schema_info:
                    data[SELECT_FIELDS_ORDER] = [field[FIELD_NAME] for field in schema_info]
            data[EXECUTE_TIME] = query_task_obj.updated_at
        return Response(data)

    @detail_route(methods=["get"], url_path="stage")
    def stage(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/stage/ 查看SQL作业轨迹
        @apiName get_query_stage
        @apiGroup queries
        @apiDescription 查看SQL作业轨迹
        @apiParam {string} query_id 查询任务id

        @apiSuccess {int} stage_seq 查询阶段序号
        @apiSuccess {string} stage_status 查询阶段状态
        @apiSuccess {int} total 查询阶段耗时
        @apiSuccess {string} detail 查询阶段名称 submitQuery：提交查询，connectDb：连接存储，queryDb：执行查询

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "stages":[
                    {
                        "stage_status": "finished",
                        "total": 1367,
                        "stage_type": "submitQuery",
                        "detail": {
                            "sqlParse": 331,
                            "checkQuerySemantic": 447,
                            "getQueryDriver": 6,
                            "pickValidStorage": 260,
                            "getStorageLoad": 6,
                            "executeQueryRule": 6,
                            "checkPermission": 293,
                            "calQueryExecCost": 6,
                            "checkQuerySyntax": 12
                        },
                        "stage_seq": 1,
                        "description": "提交查询"
                    },
                    {
                        "stage_status": "finished",
                        "total": 26,
                        "stage_type": "connectDb",
                        "detail": {
                            "connectDb": 26
                        },
                        "stage_seq": 2,
                        "description": "连接存储"
                    },
                    {
                        "stage_status": "finished",
                        "total": 9,
                        "stage_type": "queryDb",
                        "detail": {
                            "queryDb": 9
                        },
                        "stage_seq": 3,
                        "description": "执行查询"
                    }
                ]
            }
        }
        """
        try:
            query_task_id = DatalabQueryTaskModel.objects.get(query_id=query_id).query_task_id
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError
        if not query_task_id:
            return Response()
        res = QueryEngineApi.get_stage.retrieve({QUERY_ID: query_task_id}, raise_exception=True)
        stages, submit_query_time, submit_query_detail, final_status = [], 0, {}, RUNNING
        for stage_map in res.data:
            stage_type = stage_map[STAGE_TYPE]
            stage_status = stage_map[STAGE_STATUS]
            # 不需要判断stage_status是否为finished，只有SUBMIT_QUERY这些阶段的状态全为finished，才会得到query_id
            # 此阶段默认running状态
            if stage_type in SUBMIT_QUERY_LIST:
                submit_query_time += stage_map[STAGE_COST_TIME]
                submit_query_detail[stage_type] = stage_map[STAGE_COST_TIME]
                if stage_type == SUBMIT_QUERY_LIST[-1]:
                    stages.append(
                        dict(
                            stage_seq=1,
                            stage_type=SUBMIT_QUERY,
                            stage_status=FINISHED,
                            total=submit_query_time,
                            detail=submit_query_detail,
                            description=STAGES_DESCRIPTION[SUBMIT_QUERY],
                        )
                    )
            # 如果failed，则final_status == failed，否则保持running状态
            elif stage_type in [CONNECT_DB, QUERY_DB, WRITE_CACHE]:
                stages.append(
                    dict(
                        stage_seq=STAGES.get(stage_type),
                        stage_type=stage_type,
                        stage_status=stage_status,
                        total=stage_map[STAGE_COST_TIME],
                        detail={stage_type: stage_map[STAGE_COST_TIME]},
                        description=STAGES_DESCRIPTION.get(stage_type),
                    )
                )
                final_status = (
                    RUNNING if stage_type in [CONNECT_DB, QUERY_DB] and stage_status == FINISHED else stage_status
                )
        data = {STATUS: final_status, _STAGES: stages}
        if final_status == FAILED:
            error_message = json.loads(res.data[-1][ERROR_MESSAGE])
            message = error_message.get(MESSAGE)
            errors = {ERROR: error_message[INFO]} if error_message.get(INFO) else None
            return DataResponse(result=False, data=data, message=message, code=error_message.get(CODE), errors=errors)
        else:
            return Response(data)

    @check_required_params([PROJECT_ID])
    def list(self, request):
        """
        @api {get} /v3/datalab/queries/ 获取项目下的查询任务列表
        @apiName queries
        @apiGroup queries
        @apiDescription 获取项目下的查询任务列表
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal

        @apiSuccess {int} query_id 查询任务id
        @apiSuccess {string} query_name 查询任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} sql_text sql文本
        @apiSuccess {string} query_task_id sql查询作业id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "query_id": 111,
                    "query_name": "task_1"
                    "project_id": 11,
                    "project_type": "personal",
                    "sql_text": "select xx from table1",
                    "query_task_id": "BK_168",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": ""
                },
                {
                    "query_id": 112,
                    "query_name": "task_2"
                    "project_id": 11,
                    "project_type": "personal",
                    "sql_text": "select yy from table2",
                    "query_task_id": "BK_681",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": ""
                }
            ],
            "result": true
        }
        """
        params = request.query_params
        project_id = params[PROJECT_ID]
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        queries = list(DatalabQueryTaskModel.objects.filter(project_id=project_id, project_type=project_type).values())
        if queries:
            return Response(queries)
        else:
            # 项目中创建一个默认的查询任务
            bk_username = get_request_username()
            obj = DatalabQueryTaskModel.objects.create(
                project_id=project_id,
                project_type=project_type,
                sql_text=SQL_EXAMPLE,
                query_name="Demo_最近一周网站访问量",
                lock_user=bk_username,
                lock_time=datetime.now(),
                created_by=bk_username,
            )
            result = model_to_dict(obj)
            return Response([result])

    @list_route(methods=["get"], url_path="available_storage")
    def available_storage(self, request):
        """
        @api {get} /v3/datalab/queries/available_storage/ 查询任务支持的存储
        @apiName available_storage
        @apiGroup queries
        @apiDescription 查询任务支持的存储

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data":[
                "hdfs"
            ]
        }
        """
        return Response(AVAILABLE_STORAGE)

    @detail_route(methods=["get"], url_path="download")
    def download(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/download/ 获取下载秘钥
        @apiName download
        @apiGroup queries
        @apiDescription 获取下载秘钥

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": "xxx"
        }
        """
        try:
            query_task_id = DatalabQueryTaskModel.objects.get(query_id=query_id).query_task_id
        except DatalabQueryTaskModel.DoesNotExist:
            raise DownloadError
        if not query_task_id:
            raise DownloadError

        bk_username = get_request_username()
        download_txt = "{},{},{}".format(query_task_id, bk_username, int(time.time()))
        # 框架的加解密方法使用base64.b64encode，得到的密文会含有特殊字符例如/，不适合作为url的传参，因此此处使用urlsafe_b64encode
        secret_txt = base64.urlsafe_b64encode(download_txt.encode())
        queryengine_download = "{}://{}/v3/queryengine/dataset/download/{}".format(
            HTTP_SCHEMA,
            QUERYENGINEWEB_FQDN,
            secret_txt.decode(),
        )
        return Response(queryengine_download)

    @detail_route(methods=["get"], url_path="lock")
    def lock(self, request, query_id):
        """
        @api {get} /v3/datalab/queries/:query_id/lock/ 判断此时查询任务有没有被用户占用
        @apiName lock
        @apiGroup queries
        @apiDescription 判断此时查询任务有没有被用户占用
        @apiParam {int} query_id 查询任务id

        @apiSuccess {string} lock_status 锁状态，判断此任务是否被锁
        @apiSuccess {string} lock_user 占用此任务的用户名
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "lock_status": true,
                "lock_user": "bb"
            }
        }
        """
        bk_username = get_request_username()
        try:
            obj = DatalabQueryTaskModel.objects.get(query_id=query_id)
            interval = (datetime.now() - obj.lock_time).seconds
            # 如果现在这个时刻相对lock_time的间隔大于1分钟，表示此时任务没有被占用，为可使用状态
            if interval > 60:
                lock_status, lock_user = False, ""
                obj.lock_user = bk_username
                obj.lock_time = datetime.now()
                obj.save()
            else:
                lock_status, lock_user = True, obj.lock_user
                if lock_user == bk_username:
                    lock_status = False
                    obj.lock_time = datetime.now()
                    obj.save()
            return Response({"lock_status": lock_status, "lock_user": lock_user})
        except DatalabQueryTaskModel.DoesNotExist:
            raise QueryTaskNotFoundError

    @list_route(methods=["get"], url_path="function")
    def function(self, request):
        """
        @api {get} /v3/datalab/queries/function/ 查看支持的查询函数库
        @apiName get_query_function
        @apiGroup queries
        @apiDescription 查看支持的查询函数库

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [
                {
                    "func_group": "string-function",
                    "func_alias": "字符串",
                    "func_info": [
                        {
                            "name": "substr",
                            "usage": "SUBSTRING_INDEX(STRING str, STRING pattern, STRING len)",
                            "example": "SELECT SUBSTR('k1=v1;k2=v2', 2, 2);",
                            "example_return_value": "1=(类型：STRING)",
                            "support_storage": "hdfs",
                            "explain": "获取字符串子串。截取从位置 start 开始，长度为 len 的子串"
                        },
                        {
                            "name": "concat_ws",
                            "usage": "CONCAT_WS(STRING separator, STRING var1, STRING var2, ...)",
                            "example": "SELECT CONCAT_WS('#', 'a', 'b' );",
                            "example_return_value": "a#b(类型：STRING)",
                            "support_storage": "hdfs",
                            "explain": "将每个参数值和第一个参数 separator 指定的分隔符依次连接到一起组成新的字符串"
                        }
                    ]
                },
                {
                    "func_group": "math-function",
                    "func_alias": "数学",
                    "func_info": [
                        {
                            "name": "sqrt",
                            "usage": "sqrt(DOUBLE num)",
                            "example": "SELECT sqrt(4);",
                            "example_return_value": "2.0(类型：DOUBLE)",
                            "support_storage": "hdfs druid",
                            "explain": "开方"
                        }
                    ]
                }
            ]
        """
        functions = [
            {FUNC_GROUP: function_group_name, FUNC_ALIAS: function_group_alias, FUNC_INFO: []}
            for function_group_name, function_group_alias in list(FUNCTION_GROUP_INFO.items())
        ]

        function_objs = DatalabBKSqlFunctionModel.objects.all().values(
            FUNC_GROUP, FUNC_NAME, USAGE, EXAMPLE, EXAMPLE_RETURN_VALUE, SUPPORT_STORAGE, EXPLAIN
        )
        for function in function_objs:
            function_group_index = list(FUNCTION_GROUP_INFO.keys()).index(function[FUNC_GROUP])
            functions[function_group_index].get(FUNC_INFO).append(
                {
                    NAME: function[FUNC_NAME],
                    USAGE: function[USAGE],
                    EXAMPLE: function[EXAMPLE],
                    EXAMPLE_RETURN_VALUE: function[EXAMPLE_RETURN_VALUE],
                    SUPPORT_STORAGE: function[SUPPORT_STORAGE],
                    EXPLAIN: function[EXPLAIN],
                }
            )
        return Response(functions)
