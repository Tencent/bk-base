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

from common.decorators import detail_route, list_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.batch.custom_calculates.custom_calculate_driver import (
    analyze_custom_calculate,
    create_custom_calculate,
    delete_expire_data,
    get_basic_info,
    get_job_execute_list,
    stop_custom_calculate,
    sync_custom_calculate,
)
from dataflow.batch.serializer.serializers import (
    CustomCalculateAnalyzeSerializer,
    CustomCalculateListExecuteSerializer,
    StartCustomCalculateSerializer,
    StopCustomCalculateSerializer,
)


class CustomCalculatesViewSet(APIViewSet):
    """
    重算api
    """

    lookup_field = "custom_calculate_id"

    @params_valid(serializer=StartCustomCalculateSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/custom_calculates 新增离线重算
        @apiName add_custom_calculate
        @apiGroup Batch
        @apiParam {string} bk_username 提交人
        @apiParam {string} rerun_processings 用逗号分割result_table
        @apiParam {int} data_start 数据起始时间
        @apiParam {int} data_end 数据截止时间
        @apiParam {string} jobserver_config 作业服务配置
        @apiParam {string} rerun_model rerun_model
        @apiParamExample {json} 参数样例:
            {
              "custom_calculate_id": custom_calculate_id,
              "data_start": 1537152075939
              "data_end": 1537152075939
              "type": "makup",
              "geog_area_code": "inland"
              "rerun_processings": "123_clean",
              "rerun_model": "default"
            }
        @apiSuccessExample {json} custom_calculate_id
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "custom_calculate_id": xxx,
                        "status": running,
                        "jobs": [{
                            "job_id": xxx,
                            "schedule_time": xxx,
                            "status": running,
                            "info": xxx
                        },{
                            "job_id": xxx,
                            "schedule_time": xxx,
                            "status": finished,
                            "info": xxx
                        }]
                    },
                    "result": true
                }
        """
        return Response(create_custom_calculate(params))

    @detail_route(methods=["post"], url_path="stop")
    @params_valid(serializer=StopCustomCalculateSerializer)
    def stop(self, request, custom_calculate_id, params):
        """
        @api {post} /dataflow/batch/custom_calculates/:custom_calculate_id/stop 停止重算任务
        @apiName sync_custom_calculate
        @apiGroup Batch
        @apiParam {string} bk_username 提交人
        @apiParam {string} jobserver_config 作业服务配置
        @apiParamExample {json} 参数样例:
            {
              "jobserver_config": "default"
            }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        geog_area_code = params["geog_area_code"]
        stop_custom_calculate(custom_calculate_id, geog_area_code)
        return Response()

    @detail_route(methods=["get"], url_path="basic_info")
    def basic_info(self, request, custom_calculate_id):
        """
        @api {post} /dataflow/batch/custom_calculates/:custom_calculate_id/basic_info 重算任务信息
        @apiName basic_info_custom_calculate
        @apiGroup Batch
        @apiParam {string} bk_username 提交人
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "custom_calculate_id": xxx,
                        "status": running,
                        "jobs": [{
                            "job_id": xxx,
                            "schedule_time": xxx,
                            "status": running,
                            "info": xxx
                        },{
                            "job_id": xxx,
                            "schedule_time": xxx,
                            "status": finished,
                            "info": xxx
                        }]
                    },
                    "result": true
                }
        """
        return Response(get_basic_info(custom_calculate_id))

    @list_route(methods=["get"], url_path="execute_list")
    @params_valid(serializer=CustomCalculateListExecuteSerializer)
    def execute_list(self, request, params):
        """
        @api {get} /dataflow/batch/custom_calculates/:custom_calculate_id/execute_list 任务执行列表信息
        @apiName execute_list_custom_calculate
        @apiGroup Batch
        @apiParamExample {json} 参数样例:
            {
              "job_id":"xxx"
            }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [{
                            "job_id": xxx,
                            "execute_id":xxx,
                            "schedule_time": xxx,
                            "status": running,
                            "info": xxx
                        },{
                            "job_id": xxx,
                            "execute_id":xxx,
                            "schedule_time": xxx,
                            "status": finished,
                            "info": xxx
                        }],
                    "result": true
                }
        """
        job_id = request.GET.get("job_id")
        return Response(get_job_execute_list(job_id))

    @list_route(methods=["get"], url_path="sync")
    def sync(self, request):
        """
        @api {post} /dataflow/batch/custom_calculates/sync 同步重算状态
        @apiName sync_custom_calculate
        @apiGroup Batch
        @apiParam {string} bk_username 提交人
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        sync_custom_calculate()
        return Response()

    @list_route(methods=["get"], url_path="expire_data")
    def expire_data(self, request):
        """
        @api {post} /dataflow/batch/custom_calculates/expire_data 清理过期数据
        @apiName expire_data_custom_calculate
        @apiGroup Batch
        @apiParam {string} bk_username 提交人
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        delete_expire_data()
        return Response()

    @list_route(methods=["post"], url_path="analyze")
    @params_valid(serializer=CustomCalculateAnalyzeSerializer)
    def analyze(self, request, params):
        """
        @api {post} /dataflow/batch/custom_calculates/analyze 分析待补算的任务
        @apiName analyze_custom_calculate
        @apiGroup Batch
        @apiParam {string} heads result_table的heads,多个head用逗号分割
        @apiParam {string} tails result_table的tails,多个tail用逗号分割
        @apiParam {int} data_start 数据起始时间
        @apiParam {int} data_end 数据截止时间
        @apiParam {string} rerun_model rerun_model
        @apiParamExample {json} 参数样例:
            {
              "rerun_processings": "123_clean",
              "data_start": 1537152075939
              "data_end": 1537152075939
              "type": "makup"
              "rerun_model": "default"
            }
        @apiSuccessExample {json} custom_calculate_id
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [
                        {
                            "schedule_time":1560997320000,
                            "schedule_id":"591_task2_b_1"
                        },
                        {
                            "schedule_time":1560997740000,
                            "schedule_id":"591_task2_b_2"
                        },
                        {
                            "schedule_time":1560997020000,
                            "schedule_id":"591_task2_b_3"
                        }
                    ],
                    "result": true
                }
        """
        return Response(analyze_custom_calculate(params))
