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

from common.decorators import list_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.batch.data_makeup.data_makeup_driver import *
from dataflow.batch.serializer.serializers import (
    CreateDataMakeupSerializer,
    DataMakeupCheckExecSerializer,
    DataMakeupSqlColumnSerializer,
    DataMakeupStatusListSerializer,
)


class DataMakeupViewSet(APIViewSet):
    @params_valid(serializer=CreateDataMakeupSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/data_makeup 提交补齐任务
        @apiName data_makeup
        @apiGroup Batch
        @apiParam {string} heads result_table的heads,多个head用逗号分割
        @apiParam {string} tails result_table的tails,多个tail用逗号分割
        @apiParam {int} target_schedule_time 数据起始时间
        @apiParam {int} source_schedule_time 数据截止时间
        @apiParam {boolean} with_storage 下发至存储
        @apiParam {string} geog_area_code
        @apiParamExample {json} 参数样例:
            {
              "processing_id": "123_clean"
              "rerun_processings": "123_filter,123_done",
              "rerun_model": "current_canvas",
              "target_schedule_time": 1537152075939
              "source_schedule_time": 1537152075939
              "with_storage": true,
              "geog_area_code": "inland"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        return Response(submit_jobnavi_data_makeup(params))

    @list_route(methods=["get"], url_path="status_list")
    @params_valid(serializer=DataMakeupStatusListSerializer)
    def status_list(self, request, params):
        """
        @api {get} /dataflow/batch/data_makeup/status_list 获取执行记录
        @apiName status_list
        @apiGroup JobNavi
        @apiParam {string} schedule_id 调度任务名称
        @apiParam {long} start_time 查询开始时间
        @apiParam {long} end_time 查询结束时间
        @apiParam {string} geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "xxx",
                "data_start": 1537152075939
                "data_end": 1537152075939
                "geog_area_code": "inland"
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
        return Response(get_jobnavi_data_makeup_status(params))

    @list_route(methods=["get"], url_path="check_execution")
    @params_valid(serializer=DataMakeupCheckExecSerializer)
    def check_execution(self, request, params):
        """
        @api {get} /dataflow/batch/data_makeup/check_execution 检查是否可执行补齐
        @apiName check_makeup_status
        @apiGroup JobNavi
        @apiParam {string} processing_id 调度任务名称
        @apiParam {long} schedule_time 调度时间
        @apiParam {string} geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "xxx",
                "schedule_time": 1539778425000,
                "geog_area_code": "inland"
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
        return Response(is_jobnavi_data_makeup_allowed(params))

    @list_route(methods=["post"], url_path="sql_column")
    @params_valid(serializer=DataMakeupSqlColumnSerializer)
    def sql_column(self, request, params):
        """
        @api {post} /dataflow/batch/data_makeup/sql_column 获取sql字段名
        @apiName sql_column
        @apiGroup BKSQL
        @apiParam {string} sql sql语句
        @apiParamExample {json} 参数样例:
            {
                "sql": "select ip, report_time, gseindex, path, log from 591_durant1115"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    "path",
                    "gseindex",
                    "report_time",
                    "log",
                    "ip"
                ],
                "result": true
            }
        """
        return Response(submit_sql_column(params))
