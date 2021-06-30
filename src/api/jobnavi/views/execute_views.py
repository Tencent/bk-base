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

from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.decorators import params_valid, list_route
from common.django_utils import DataResponse
from common.views import APIViewSet
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import ArgumentError, InterfaceError
from jobnavi.views.serializers import (
    CommonSerializer,
    ListExecuteSerializer,
    ExecuteSerializer,
    QuerySubmittedExecuteByTimeSerializer,
    RunSerializer,
    RerunSerializer,
    RedoSerializer,
)


class ExecuteViewSet(APIViewSet):
    """
    @apiDefine execute
    任务执行API
    """

    lookup_field = "exec_id"
    lookup_value_regex = "\\d+"

    @params_valid(serializer=ListExecuteSerializer)
    def list(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/execute/?schedule_id=xxx&limit=xxx 获取执行任务结果
        @api {get} /jobnavi/cluster/:cluster_id/execute/?schedule_id=xxx&status=xxx 获取执行任务结果
        @apiName get_execute_status
        @apiGroup execute
        @apiParam {string} schedule_id 调度任务名称
        @apiParam {int} limit 返回条数限制,可选
        @apiParam {string} status 任务状态,可选
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "limit":10,
                "status":"running"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "execute_info": {
                            "id": 123,
                            "host": "xxx.xxx.xx.xx"
                        },
                        "schedule_id": "xxx",
                        "schedule_time": 1539778425000,
                        "status": "running",
                        "info": "xxx",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    },
                    {
                        "execute_info": {
                            "id": 124,
                            "host": "xxx.xxx.xx.xx"
                        },
                        "schedule_id": "xxx",
                        "schedule_time": 1539778425000,
                        "status": "finished",
                        "info": "xxx",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    }
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        schedule_id = request.GET.get("schedule_id", None)
        if not schedule_id:
            raise ArgumentError(message=_("过滤条件必须包含[schedule_id]."))
        limit = request.GET.get("limit", None)
        status = request.GET.get("status", None)
        if limit:
            result = jobnavi.query_execute(schedule_id, limit)
        elif status:
            schedule_time = request.GET.get("schedule_time", None)
            result = jobnavi.query_execute_by_status(schedule_id, schedule_time, status)
        else:
            raise ArgumentError(message=_("过滤条件只支持[limit] 和 [status]"))
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @params_valid(serializer=CommonSerializer)
    def retrieve(self, request, cluster_id, exec_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/execute/:exec_id/?field=status 获取执行任务结果
        @apiName get_execute_status
        @apiGroup execute
        @apiParam {string} exec_id 执行任务id
        @apiParamExample {json} 参数样例:
            {
                "exec_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "execute_info": {
                        "id": 124,
                        "host": "xxx.xxx.xx.xx"
                    },
                    "schedule_id": "xxx",
                    "schedule_time": 1539778425000,
                    "status": "finished",
                    "info": "xxx",
                    "created_at": 1539778425000,
                    "updated_at": 1539778425000
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        field = request.GET.get("field", None)
        if not field or field != "status":
            raise ArgumentError(message=_("获取执行结果目前仅支持field=status"))
        result = jobnavi.get_execute_status(exec_id)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response(json.loads(result.data))

    @params_valid(serializer=ExecuteSerializer)
    def create(self, request, params, cluster_id):
        """
        @api {post} /jobnavi/cluster/:cluster_id/execute/ 创建执行任务
        @apiName create_execute
        @apiGroup execute
        @apiParam {string} schedule_id 调度名称
        @apiParam {long} schedule_time 调度时间(可选)
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "schedule_time": 1539778425000
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": 12,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        schedule_id = request.data["schedule_id"]
        schedule_time = request.data.get("schedule_time", None)
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.execute_schedule(schedule_id, schedule_time)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(result.data)

    @list_route(methods=["get"], url_path="query_submitted_by_time")
    @params_valid(serializer=QuerySubmittedExecuteByTimeSerializer)
    def query_submitted_by_time(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/execute/query_submitted_by_time/ 获取指定任务时间范围内已提交的执行记录
        @apiName query_submiited_execute_by_time
        @apiGroup execute
        @apiParam {string} schedule_id 调度任务名称
        @apiParam {long} start_time 查询开始时间
        @apiParam {long} end_time 查询结束时间
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "start_time": 1537152075939,
                "end_time": 1537152075939,
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "execute_id": 12544,
                        "schedule_time": 1539778425000,
                        "status": "running",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    },
                    {
                        "execute_id": 14425,
                        "schedule_time": 1539778425000,
                        "status": "finished",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    },
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        schedule_id = request.GET.get("schedule_id", None)
        start_time = request.GET.get("start_time", None)
        end_time = request.GET.get("end_time", None)
        result = jobnavi.query_execute_by_time_range(schedule_id, start_time, end_time)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        data = []
        for record in json.loads(result.data):
            status = record["status"]
            if status == "running" or status == "finished" or status == "failed" or status == "failed_succeeded":
                data.append(record)
        return Response(data)

    @list_route(methods=["get"], url_path="fetch_today_job_status")
    @params_valid(serializer=CommonSerializer)
    def fetch_today_job_status(self, request, cluster_id, params):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/fetch_today_job_status/ 获取今日执行任务结果
        @apiName fetch_today_job_status
        @apiGroup execute
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "2018102210":{
                        "running":xxx
                        "preparing":xxx
                        "finished":xxx
                    },
                    "2018102211":{
                        "running":xxx
                        "preparing":xxx
                        "finished":xxx
                    }
                    ...
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.fetch_today_job_status()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="fetch_last_hour_job_status")
    @params_valid(serializer=CommonSerializer)
    def fetch_last_hour_job_status(self, request, cluster_id, params):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/fetch_last_hour_job_status/ 获取上一个小时执行任务结果
        @apiName fetch_last_hour_job_status
        @apiGroup execute
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "finished":xxx,
                    "preparing":xxx,
                    "running":xxx,
                    "failed":xxx,
                    "skip":xxxx
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.fetch_last_hour_job_status()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="fetch_job_status_by_create_time")
    @params_valid(serializer=CommonSerializer)
    def fetch_job_status_by_create_time(self, request, cluster_id, params):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/fetch_job_status_by_create_time/ 获取近24小时生成任务结果
        @apiName fetch_job_status_by_create_time
        @apiGroup execute
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "2018102210":{
                        "running":xxx
                        "preparing":xxx
                        "finished":xxx
                    },
                    "2018102211":{
                        "running":xxx
                        "preparing":xxx
                        "finished":xxx
                    }
                    ...
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.fetch_job_status_by_create_time()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["post"], url_path="run")
    @params_valid(serializer=RunSerializer)
    def run(self, request, params, cluster_id):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/run/ 执行任务
        @apiName run
        @apiGroup execute
        @apiParam {string} rerun_processings 待执行任务列表，逗号分隔
        @apiParam {string} rerun_model 执行模式：current_canvas, all_child
        @apiParam {long} start_time 任务时间起点
        @apiParam {long} start_time 任务时间终点
        @apiParam {string} priority 任务优先度（可选）: low, normal, high
        @apiParamExample {json} 参数样例:
            {
              "rerun_processings": "123_clean,456_import,789_calculate",
              "rerun_model": "current_canvas",
              "start_time": 1537152075939,
              "end_time": 1537152075939,
              "priority": "normal"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data":
                [
                    {
                        "executeInfo":{
                            "id":1,
                            "host":xxx
                        },
                        "eventInfo":"preparing",
                        "taskInfo":{
                            "scheduleId":xxx,
                            "scheduleTime":xxx,
                            "type":{
                                "name":xxx,
                                "main":xxx,
                                "description":xxx,
                                "env":xxx,
                                "sysEnv":xxx,
                                "language":xxx,
                                "taskMode":xxx
                            },
                            "extraInfo":xxx,
                            "recoveryInfo":{
                                "recoveryEnable":False,
                                "recoveryTimes":123,
                                "intervalTime":xxxx,
                                "maxRecoveryTimes":1
                            }
                            "decommissionTimeout":xxx,
                            "nodeLabel":xxx
                        }
                    },
                    {
                        "executeInfo":{
                            "id":1,
                            "host":xxx
                        },
                        "eventInfo":"preparing",
                        "taskInfo":{
                            "scheduleId":xxx,
                            "scheduleTime":xxx,
                            "type":{
                                "name":xxx,
                                "main":xxx,
                                "description":xxx,
                                "env":xxx,
                                "sysEnv":xxx,
                                "language":xxx,
                                "taskMode":xxx
                            },
                            "extraInfo":xxx,
                            "recoveryInfo":{
                                "recoveryEnable":False,
                                "recoveryTimes":123,
                                "intervalTime":xxxx,
                                "maxRecoveryTimes":1
                            }
                            "decommissionTimeout":xxx,
                            "nodeLabel":xxx
                        }
                    }
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        rerun_processings = request.data["rerun_processings"]
        rerun_model = request.data["rerun_model"]
        start_time = request.data["start_time"]
        end_time = request.data["end_time"]
        priority = request.data.get("priority", "normal")
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.rerun(rerun_processings, rerun_model, start_time, end_time, priority)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["post"], url_path="rerun")
    @params_valid(serializer=RerunSerializer)
    def rerun(self, request, params, cluster_id):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/rerun/ 重运行执行
        @apiName rerun
        @apiGroup execute
        @apiParam {string} rerun_processings 待执行任务列表，逗号分隔
        @apiParam {string} rerun_model 执行模式：current_canvas, all_child
        @apiParam {long} start_time 任务时间起点
        @apiParam {long} start_time 任务时间终点
        @apiParam {string} [exclude_statuses] 不补算已执行且为指定状态的任务
        @apiParamExample {json} 参数样例:
            {
              "rerun_processings": "123_clean,456_import,789_calculate",
              "rerun_model": "current_canvas",
              "start_time": 1537152075939
              "end_time": 1537152075939
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data":
                [
                    {
                        "executeInfo":{
                            "id":1,
                            "host":xxx
                        },
                        "eventInfo":"preparing",
                        "taskInfo":{
                            "scheduleId":xxx,
                            "scheduleTime":xxx,
                            "type":{
                                "name":xxx,
                                "main":xxx,
                                "description":xxx,
                                "env":xxx,
                                "sysEnv":xxx,
                                "language":xxx,
                                "taskMode":xxx
                            },
                            "extraInfo":xxx,
                            "recoveryInfo":{
                                "recoveryEnable":False,
                                "recoveryTimes":123,
                                "intervalTime":xxxx,
                                "maxRecoveryTimes":1
                            }
                            "decommissionTimeout":xxx,
                            "nodeLabel":xxx
                        }
                    },
                    {
                        "executeInfo":{
                            "id":1,
                            "host":xxx
                        },
                        "eventInfo":"preparing",
                        "taskInfo":{
                            "scheduleId":xxx,
                            "scheduleTime":xxx,
                            "type":{
                                "name":xxx,
                                "main":xxx,
                                "description":xxx,
                                "env":xxx,
                                "sysEnv":xxx,
                                "language":xxx,
                                "taskMode":xxx
                            },
                            "extraInfo":xxx,
                            "recoveryInfo":{
                                "recoveryEnable":False,
                                "recoveryTimes":123,
                                "intervalTime":xxxx,
                                "maxRecoveryTimes":1
                            }
                            "decommissionTimeout":xxx,
                            "nodeLabel":xxx
                        }
                    }
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        rerun_processings = request.data["rerun_processings"]
        rerun_model = request.data["rerun_model"]
        start_time = request.data["start_time"]
        end_time = request.data["end_time"]
        exclude_statuses = params["exclude_statuses"] if "exclude_statuses" in params else None
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.rerun(rerun_processings, rerun_model, start_time, end_time, exclude_statuses)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="redo")
    @params_valid(serializer=RedoSerializer)
    def redo(self, request, params, cluster_id):
        """
        @api {GET} /jobnavi/cluster/:cluster_id/execute/redo/ 重运行执行
        @apiName jobnavi_redo
        @apiGroup execute
        @apiParam {string} schedule_id 调度任务标识
        @apiParam {string} schedule_time 调度时间
        @apiParam {boolean} is_run_depend 是否重运行子节点
        @apiParam {string} [exclude_statuses] 不重算指定状态的任务
        @apiParamExample {json} 参数样例:
            {
              "schedule_id": "123_clean",
              "schedule_time": 1539778425000,
              "is_run_depend": false
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "rerun [123_clean] success.",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        schedule_id = params["schedule_id"]
        schedule_time = params["schedule_time"]
        is_run_depend = params["is_run_depend"]
        exclude_statuses = params["exclude_statuses"] if "exclude_statuses" in params else None

        jobnavi = JobNaviApi(geog_area_code, cluster_id)

        # api 不提供强制下发
        result = jobnavi.redo(schedule_id, schedule_time, is_run_depend, False, exclude_statuses)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(result.message)
