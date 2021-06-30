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

from dataflow.stream.processing.processing_driver import (
    change_component_type,
    check_stream_param,
    create_processing,
    delete_processing_funcs,
    get_processing_udf_info,
    update_processings,
)
from dataflow.stream.processing.processing_serializers import *
from dataflow.stream.views.base_views import BaseViewSet


class ProcessingViewSet(BaseViewSet):
    """
    node reset api
    """

    lookup_field = "processing_id"

    @params_valid(serializer=ProcessingSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/stream/processings/ 创建stream processing
        @apiName processings
        @apiGroup Stream
        @apiParam {list} static_data
        @apiParam {list} source_data
        @apiParam {list} result_tables
        @apiParam {string} sql
        @apiParam {string} processing_id
        @apiParam {list} outputs
        @apiParam {dict}  dict
        @apiParam {int} project_id
        @apiParam {string} component_type
        @apiParamExample {json} 参数样例:
            {
                "tags": "xxx",
                "static_data": [],
                "source_data": [],
                "input_result_tables": [
                    "xxx_xxx"
                ],
                "sql": "select _path_, platid from xxx",
                "processing_id": "xxx_xxx",
                "outputs": [{bk_biz_id: 123,table_name: abc},{bk_biz_id:456,table_name: def}],
                "dict": {
                    "window_type": "none",
                    "description": "xxx",
                    "count_freq": 0,
                    "waiting_time": 0,
                    "window_time": 0,
                    "session_gap": 0
                },
                "project_id": 99999,
                "component_type": "flink"
            }
            code 参数样例
            {
                "tags": "xxx",
                "project_id": 99999,
                "component_type": "spark_structured_streaming",
                "implement_type": "code",
                "static_data": [],
                "input_result_tables": [rt_id1, rt_id2],
                "processing_id": "id1",
                "processing_alias": "xxxxx",
                "dict": {
                    "programming_language": "python",
                    "user_args": "args1 args2 args3",
                    "user_main_class": "com.abc.MyMain",
                    "package": {
                        "path": "/app/xxx",
                        "id": "xxxxx",
                        "name": "xxx.jar"
                    },
                    "advanced": {
                        "use_savepoint": True,
                    }
                },
                "outputs": [
                    {
                        "bk_biz_id": 591,
                        "result_table_name": "abc",
                        "result_table_name_alias": "ABC",
                        "fields": [
                            {
                                "field_name": "a",
                                "field_type": "int",
                                "field_alias": "A",
                                "event_time": False
                            }
                        ]
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "tails": [
                            "5_test_0620"
                        ],
                        "result_table_ids": [
                            "5_test_0620"
                        ],
                        "heads": [
                            "5_test_0620"
                        ],
                        "processing_id": "5_test_0620"
                    },
                    "result": true
                }

            code 返回
            {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "result_table_ids": [
                            "5_test_0620"
                        ],
                        "processing_id": "5_test_0620"
                    },
                    "result": true
                }
        """
        stream_result = create_processing(params)
        return self.return_ok(stream_result)

    @params_valid(serializer=UpdateProcessingSerializer)
    def update(self, request, processing_id, params):
        """
        @api {put} /dataflow/stream/processings/ 更新stream processing
        @apiName processings
        @apiGroup Stream
        @apiParam {list} static_data
        @apiParam {list} source_data
        @apiParam {list} result_tables
        @apiParam {string} sql
        @apiParam {list} outputs
        @apiParam {dict}  dict
        @apiParam {int} project_id
        @apiParamExample {json} 参数样例:
            {
                "static_data": [],
                "source_data": [],
                "input_result_tables": [
                    "xxx_xxx"
                ],
                "sql": "select _path_, platid from xxx",
                "outputs": [{bk_biz_id: 123,table_name: abc},{bk_biz_id:456,table_name: def}],
                "dict": {
                    "window_type": "none",
                    "description": "xxx",
                    "count_freq": 0,
                    "waiting_time": 0,
                    "window_time": 0,
                    "session_gap": 0
                },
                "project_id": 99999
            }
            code 参数样例
            {
                "tags": "xxx",
                "project_id": 99999,
                "implement_type": "code",
                "processing_alias": "xxxxx",
                "static_data": [],
                "input_result_tables": [rt_id1, rt_id2],
                "delete_result_tables": [],
                "dict": {
                    "programming_language": "python",
                    "user_args": "args1 args2 args3",
                    "user_main_class": "com.abc.MyMain",
                    "package": {
                        "path": "/app/xxx",
                        "id": "xxxxx",
                        "name": "xxx.jar"
                    },
                    "advanced": {
                        "use_savepoint": True,
                    }
                },
                "outputs": [
                    {
                        "bk_biz_id": 591,
                        "result_table_name": "abc",
                        "result_table_name_alias": "ABC",
                        "fields": [
                            {
                                "field_name": "a",
                                "field_type": "int",
                                "field_alias": "A",
                                "event_time": False
                            }
                        ]
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "tails": [
                            "5_test_0620"
                        ],
                        "result_table_ids": [
                            "5_test_0620"
                        ],
                        "heads": [
                            "5_test_0620"
                        ],
                        "processing_id": "5_test_0620"
                    },
                    "result": true
                }

                code 返回
            {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "result_table_ids": [
                            "5_test_0620"
                        ],
                        "processing_id": "5_test_0620"
                    },
                    "result": true
                }
        """
        stream_result = update_processings(params, processing_id)
        return self.return_ok(stream_result)

    def destroy(self, request, processing_id):
        """
        @api {delete} /dataflow/stream/processings/ 删除stream processing
        @apiName processings
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "with_data": False  可选
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
        delete_processing_funcs(args, processing_id)
        return self.return_ok()

    @list_route(methods=["post"], url_path="change_component_type")
    @params_valid(serializer=ChangeComponentType)
    def change_component_type(self, request, params):
        """
        @api {post} /dataflow/stream/processings/change_component_type/ 更改计算的类型
        @apiName processings
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "processings": [rt1,rt2],
                "component_type": 'flink/storm'
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
        change_component_type(params)
        return self.return_ok()

    @detail_route(methods=["get"], url_path="udf_info")
    def udf_info(self, request, processing_id):
        """
        @api {get} /dataflow/stream/processings/:processing_id/udf_info 获取processing相关udf信息
        @apiName processings
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": ['udf1', 'udf2'],
                    "result": true
                }
        """
        data = get_processing_udf_info(processing_id)
        return self.return_ok(data)

    @list_route(methods=["post"], url_path="check_stream_param")
    @params_valid(serializer=WindowConfigSerializer)
    def check_window_config(self, request, params):
        """
        @api {post} /dataflow/stream/processings/check_stream_param/ 校验实时计算窗口配置
        @apiName processings
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "scheduling_type":"stream",
                "scheduling_content":{
                    "window_type":"scroll",
                    "count_freq":30,
                    "window_time":30,
                    "waiting_time":30,
                    "session_gap": 0,
                    "expired_time": 0,
                    "window_lateness":{
                        "allowed_lateness":true,
                        "lateness_count_freq":60,
                        "lateness_time":1
                    }
                }
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
        check_stream_param(params)
        return self.return_ok()
