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
import time

from rest_framework.response import Response

from django.conf import settings
from common.decorators import params_valid, list_route
from common.views import APIViewSet
from jobnavi.api_helpers.datamanage.datamanage_helper import DatamanageHelper
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import CommonSerializer


class RunnerViewSet(APIViewSet):
    """
    @apiDefine runner
    Runner管理API
    """

    @params_valid(serializer=CommonSerializer)
    def list(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/runner/ 获取Runner信息
        @apiName list_runner_digests
        @apiGroup runner
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "host-0": {
                        "cpuUsage": 0.0,
                        "cpuAvgLoad": 0.07,
                        "threadTaskNum": 0,
                        "memory": 4140,
                        "memoryUsage": 74.61582523220986,
                        "maxTaskNum": 200,
                        "host": "host-0",
                        "maxThreadTaskNum": 200,
                        "taskNum": 0,
                        "status": "running",
                        "decommissioningTaskNumb": 0
                    },
                    "host-1": {
                        "cpuUsage": 0.0,
                        "cpuAvgLoad": 0.07,
                        "threadTaskNum": 0,
                        "memory": 4140,
                        "memoryUsage": 75.61582523220986,
                        "maxTaskNum": 200,
                        "host": "host-1",
                        "maxThreadTaskNum": 200,
                        "taskNum": 0,
                        "status": "running",
                        "decommissioningTaskNumb": 0
                    }
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.list_runner_digests()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @list_route(methods=["get"], url_path="report")
    @params_valid(serializer=CommonSerializer)
    def report(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/runner/report/ 上报Runner信息
        @apiName report_runner_digest
        @apiGroup runner
        @apiDescription 用于上报Runner监控信息
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.list_runner_digests()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        dataapi = DatamanageHelper()
        runners = json.loads(result.data)
        for hostname, info in list(runners.items()):
            info["tags"] = {"runner_id": hostname}
            message = {"time": int(time.time()), "database": "monitor_custom_metrics", "jobnavi_runner_info": info}
            kafaka_topic = settings.BK_MONITOR_METRIC_TOPIC
            tags = [settings.DEFAULT_GEOG_AREA_CODE]
            dataapi.op_metric_report(message, kafaka_topic, tags)
        return Response()
