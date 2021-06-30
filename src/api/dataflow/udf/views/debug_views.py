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

from common.decorators import detail_route, list_route

from dataflow.udf.debug.debug_driver import *
from dataflow.udf.views.base_views import BaseViewSet


class DebugViewSet(BaseViewSet):
    """
    function debug api
    """

    lookup_field = "debug_id"

    def create(self, request):
        """
        @api {post} /dataflow/udf/debugs/ 创建调试任务
        @apiName debugs/
        @apiGroup udf
        @apiParam {String} func_name 函数名称
        @apiParam {String} calculation_types 调试任务类型
        @apiParam {String} sql 调试SQL
        @apiParam {String} result_table_id 调试数据源ID
        @apiParamExample {json} 参数样例:
            {
                "func_name": "abc",
                "calculation_types": ['batch'],
                "sql": "select abc from table",
                "result_table_id": "123_acc"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "debug_id": xxxx
                    },
                    "result": true
                }
        """
        args = request.data
        func_name = args["func_name"]
        calculation_types = args["calculation_types"]
        sql = args["sql"]
        result_table_id = args["result_table_id"]
        debug_id = create_debug(func_name, calculation_types, sql, result_table_id)
        return self.return_ok({"debug_id": debug_id})

    @list_route(methods=["get"], url_path="auto_generate_data")
    def auto_generate_data(self, request):
        """
        @api {get} /dataflow/udf/debugs/auto_generate_data 获取调试数据
        @apiName debugs/auto_generate_data
        @apiGroup udf
        @apiParam {String} result_table_id 用户选择的调试数据源
        @apiParamExample {json} 参数样例:
            {
                "result_table_id": "591_efg"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        [['field_a', 'field_b'],['a_value1', 'b_value1'], ['a_value2', 'b_value2']]
                    },
                    "result": true
                }
        """
        args = request.query_params
        result_table_id = args["result_table_id"]
        data = auto_generate_data(result_table_id)
        return self.return_ok(data)

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, debug_id):
        """
        @api {post} /dataflow/udf/debugs/:debug_id/start 启动调试任务
        @apiName debugs/:debug_id/start
        @apiGroup udf
        @apiParam {String} calculation_type 调试计算类型 batch/stream
        @apiParamExample {json} 参数样例:
            {
                "calculation_type": "stream",
                "result_table_id": "591_clean",
                "debug_data": {
                                'schema': [{
                                        field_name: a,
                                        field_type: string
                                    }, {
                                        field_name: b,
                                        field_type: string
                                    }
                                ],
                                'value': [
                                    [xx, 2]
                                ]
                            },
                "sql": "select * ...."
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        debug_data = args["debug_data"]
        result_table_id = args["result_table_id"]
        calculation_type = args["calculation_type"]
        sql = args["sql"]
        start_debug(calculation_type, debug_id, debug_data, result_table_id, sql)
        return self.return_ok()

    @detail_route(methods=["get"], url_path="result_data")
    def result_data(self, request, debug_id):
        """
        @api {get} /dataflow/udf/debugs/:debug_id/result_data 获取调试结果数据
        @apiName debugs/:debug_id/result_data
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "stream": [],
                        "batch": []
                    },
                    "result": true
                }
        """
        result = get_result_data(debug_id)
        return self.return_ok(result)
