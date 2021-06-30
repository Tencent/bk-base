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
from jobnavi.exception.exceptions import JobNaviException, InterfaceError
from jobnavi.views.serializers import (
    CommonSerializer,
    EventSerializer,
)


class EventViewSet(APIViewSet):
    """
    @apiDefine event
    任务事件API
    """

    lookup_field = "event_id"
    lookup_value_regex = "\\d+"

    @params_valid(serializer=EventSerializer)
    def create(self, request, params, cluster_id):
        """
        @api {post} /jobnavi/cluster/:cluster_id/event/ 创建事件
        @apiName create_event
        @apiGroup event
        @apiParam {string} execute_id 任务执行ID
        @apiParam {string} event_name 事件名称
        @apiParam {string} change_status 修改状态，系统事件必填，自定义事件可选
        @apiParam {string} event_info 事件信息，可选
        @apiParamExample {json} 参数样例:
            {
                "execute_id": xxx,
                "event_name": "running"
                "change_status": "running"
                "event_info": "cancel_job"
            }
        @apiSuccessExample {json} 成功返回，返回事件ID
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": 12,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        execute_id = request.data["execute_id"]
        event_name = request.data["event_name"]
        change_status = request.data.get("change_status", None)
        event_info = request.data.get("event_info", None)
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.send_event(execute_id, event_name, change_status, event_info)
        if not result.is_success():
            try:
                info = json.loads(json.loads(result.data)["info"])
                if info["code"] and info["message"]:
                    raise InterfaceError(message=info["message"])
            except Exception as e:
                if isinstance(e, JobNaviException):
                    raise e
                else:
                    raise JobNaviException(message=result.message)
        else:
            return Response(result.data)

    @params_valid(serializer=CommonSerializer)
    def retrieve(self, request, cluster_id, event_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/event/:event_id/ 获取事件信息
        @apiName get_event
        @apiGroup event
        @apiParam {string} event_id 事件ID
        @apiParamExample {json} 参数样例:
            {
                "event_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "is_processed" : true,
                    "process_success": true,
                    "process_info": "xxx"
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.get_event_result(event_id)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="query_processing_event_amount")
    @params_valid(serializer=CommonSerializer)
    def query_processing_event_amount(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/event/query_processing_event_amount/ 获取未处理的事件任务数
        @apiName query_processing_event_amount
        @apiGroup event
        @apiParam {string} schedule_id 调度名称
        @apiParam {long} schedule_time 调度时间(可选)
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": 22,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.query_processing_event_amount()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(result.data)
