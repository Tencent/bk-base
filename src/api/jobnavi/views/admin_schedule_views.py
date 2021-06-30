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

from rest_framework.response import Response

from common.decorators import params_valid, list_route
from common.django_utils import DataResponse
from common.views import APIViewSet
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import (
    CalculateScheduleTaskTimeSerializer,
    QueryFailedExecutesSerializer,
)


class AdminScheduleViewSet(APIViewSet):
    """
    @apiDefine admin_schedule
    schedule管理API
    """

    @list_route(methods=["post"], url_path="calculate_schedule_task_time")
    @params_valid(serializer=CalculateScheduleTaskTimeSerializer)
    def calculate_schedule_task_time(self, request, params, cluster_id):
        """
        @api {post} /jobnavi/cluster/:cluster_id/admin/schedule/calculate_schedule_task_time/ 计算任务的调度时间
        @apiName admin_schedule_calculate_schedule_task_time
        @apiGroup admin_schedule
        @apiParamExample {json} 参数样例:
           {
              "rerun_processings": "123_clean,456_import,789_calculate",
              "rerun_model": "current_canvas",
              "start_time": 1537152075939,
              "end_time": 1537152075939
            }
        @apiSuccessExample {json} 成功返回，返回事件ID
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "scheduleId":xxx,
                        "scheduleTime":xxx
                    },
                    {
                        "scheduleId":xxx,
                        "scheduleTime":xxx
                    }
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        rerun_processings = params["rerun_processings"]
        rerun_model = params["rerun_model"]
        start_time = params["start_time"]
        end_time = params["end_time"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.admin_schedule_calculate_task_time(
            rerun_processings, rerun_model, start_time, end_time
        )
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="query_failed_executes")
    @params_valid(serializer=QueryFailedExecutesSerializer)
    def query_failed_executes(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/admin/schedule/query_failed_executes/ 查询失败任务
        @apiName query_failed_executes
        @apiGroup admin_schedule
        @apiParam {long} begin_time 调度起始时间
        @apiParam {long} end_time 调度结束时间
        @apiParam {string} type_id 任务类型
        @apiParamExample {json} 参数样例:
            {
                "begin_time": 1539778425000,
                "end_time": 1539778425000,
                "type_id": "spark_sql"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [{
                    "execute_info": {
                        "id": 12345,
                        "host": "host1",
                        "rank": 0.0
                    },
                    "schedule_time": 1539778425000,
                    "updated_at": 1539778425000,
                    "data_time": 1539778425000,
                    "created_at": 1539778425000,
                    "started_at": 0,
                    "schedule_id": "test_job",
                    "status": "failed",
                    "info": null
                }],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        begin_time = params["begin_time"]
        end_time = params["end_time"]
        type_id = params["type_id"]
        result = jobnavi.query_failed_executes(begin_time, end_time, type_id)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response(json.loads(result.data))
