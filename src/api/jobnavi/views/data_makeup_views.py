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
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import (
    ListDataMakeupSerializer,
    CheckDataMakeupAllowedSerializer,
    RunDataMakeupSerializer,
)


class DataMakeupViewSet(APIViewSet):
    """
    @apiDefine data_makeup
    数据补齐API
    """

    @params_valid(serializer=ListDataMakeupSerializer)
    def list(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/data_makeup/?schedule_id=xxx&start_time=xxx&end_time=xxx 获取执行记录
        @apiName list_data_makeup_execute
        @apiGroup data_makeup
        @apiParam {string} schedule_id 调度任务名称
        @apiParam {long} start_time 查询开始时间
        @apiParam {long} end_time 查询结束时间
        @apiParam {string} status 任务状态,可选
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "start_time": 1537152075939
                "end_time": 1537152075939
                "status":"running"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "schedule_time": 1539778425000,
                        "status": "running",
                        "status_str": "执行中",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    },
                    {
                        "schedule_time": 1539778425000,
                        "status": "finished",
                        "status_str": "成功",
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
        status = request.GET.get("status", None)
        result = jobnavi.list_data_makeup_execute(schedule_id, start_time, end_time, status)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        data = []
        for record in json.loads(result.data):
            status = record["status"]
            record["status_str"] = status
            if status == "none" or status == "preparing" or status == "recovering":
                record["status_str"] = _("等待中")
            elif status == "running":
                record["status_str"] = _("运行中")
            elif status == "failed" or status == "killed" or status == "disabled" or status == "skipped":
                record["status_str"] = _("失败")
                if status == "disabled":
                    record["err_msg"] = _("前序节点执行失败")
                elif status == "skipped":
                    record["err_msg"] = _("前序节点未执行，请等待下个周期")
            elif status == "failed_succeeded":
                record["status_str"] = _("警告")
            elif status == "finished":
                record["status_str"] = _("成功")
            data.append(record)
        return Response(data)

    @list_route(methods=["get"], url_path="check_allowed")
    @params_valid(serializer=CheckDataMakeupAllowedSerializer)
    def check_allowed(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/data_makeup/check_allowed/?schedule_id=xxx&schedule_time=xxx 检查是否可执行补齐
        @apiName check_data_makeup_allowed
        @apiGroup data_makeup
        @apiParam {string} schedule_id 调度任务名称
        @apiParam {long} schedule_time 调度时间
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "schedule_time": 1539778425000,
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "status": "failed",
                    "is_allowed": true
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        schedule_id = request.GET.get("schedule_id", None)
        schedule_time = request.GET.get("schedule_time", None)
        result = jobnavi.check_data_makeup_allowed(schedule_id, schedule_time)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response(json.loads(result.data))

    @params_valid(serializer=RunDataMakeupSerializer)
    def create(self, request, params, cluster_id):
        """
        @api {post} /jobnavi/cluster/:cluster_id/data_makeup/ 提交补齐任务
        @apiName run_data_makeup
        @apiGroup data_makeup
        @apiParam {string} processing_id 当前补齐的节点的调度ID
        @apiParam {string} rerun_processings 下游需要补算的任务列表，逗号分隔
        @apiParam {string} rerun_model 补算模式：current_canvas：只补算rerun_processings里指定的任务；all_child：补算processing_id下游所有任务
        @apiParam {long} target_schedule_time 目标调度时间
        @apiParam {long} source_schedule_time 源调度时间
        @apiParam {boolean} dispatch_to_storage 是否下发到数据存储
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "123_clean",
                "rerun_processings": "123_filter,123_done",
                "rerun_model": "current_canvas",
                "target_schedule_time": 1539778425000,
                "source_schedule_time": 1539774425000,
                "dispatch_to_storage": true
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
        processing_id = request.data.get("processing_id", None)
        rerun_processings = request.data.get("rerun_processings", None)
        rerun_model = request.data.get("rerun_model", None)
        target_schedule_time = request.data.get("target_schedule_time", None)
        source_schedule_time = request.data.get("source_schedule_time", None)
        dispatch_to_storage = request.data.get("dispatch_to_storage", False)
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.run_data_makeup(
            processing_id,
            rerun_processings,
            rerun_model,
            target_schedule_time,
            source_schedule_time,
            dispatch_to_storage,
        )
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))
