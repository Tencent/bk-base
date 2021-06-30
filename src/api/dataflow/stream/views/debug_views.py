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

from common.decorators import detail_route, params_valid

from dataflow.stream.debug.debug_driver import (
    create_debug,
    get_basic_info,
    get_node_info,
    set_error_data,
    set_result_data,
    stop_debug,
    update_metric_info,
)
from dataflow.stream.debug.debug_serializers import *
from dataflow.stream.views.base_views import BaseViewSet


class DebugViewSet(BaseViewSet):
    """
    调试api
    """

    lookup_field = "debug_id"
    # lookup_value_regex = '\d+'

    @detail_route(methods=["get"], url_path="basic_info")
    def basic_info(self, request, debug_id):
        """
        @api {get} /dataflow/stream/debugs/:debug_id/basic_info 获取debug basic info
        @apiName debugs/:debug_id/basic_info
        @apiGroup Stream
        @apiParam {string} job_id
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "result_tables":{
                            "123_parser":{
                                "output_total_count":2190,
                                "warning_count": 0
                            },
                            "123_filter":{
                                "output_total_count":200,
                                "warning_count": 1
                            }
                        },
                        "debug_error":{
                            "error_result_table_id":"123_filter"
                        }
                    },
                    "result": true
                }

        """
        basic_info = get_basic_info(debug_id)
        return self.return_ok(basic_info)

    @detail_route(methods=["get"], url_path="node_info")
    @params_valid(serializer=GetNodeInfoSerializer)
    def node_info(self, request, debug_id, params):
        """
        @api {get} /dataflow/stream/debugs/:debug_id/node_info 获取调试node info
        @apiName debugs/:debug_id/node_info
        @apiGroup Stream
        @apiParam {string} job_id
        @apiParam {string} result_table_id
        @apiParamExample {json} 参数样例:
            {
                "job_id": "123abc",
                "result_table_id": "xxxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "debug_errcode":{
                            "error_code":101,
                            "error_message":"在【123_test】中的字段【aaa】转换失败"
                        },
                        "debug_metric":{
                            "input_total_count":45210,
                            "output_total_count":0,
                            "filter_discard_count":0,
                            "filter_discard_rate": 0,
                            "transformer_discard_count":45210,
                            "transformer_discard_rate": 1,
                            "aggregator_discard_count":0,
                            "aggregator_discard_rate": 0
                            "metric_info":[u"数据过滤丢弃率0%",u"数据转换丢弃率100%",u"数据聚合丢弃率0%"],
                            "warning_info": [u"【警告】数据转换丢失率为100%"]
                        },
                        "debug_data":{
                            "result_data":[
                                {"ip":"x.x.x.x","cc_set":"test","cc_module":"test"},
                                {"ip":"x.x.x.x","cc_set":"test","cc_module":"test"}
                            ],
                            "discard_data":{
                                "filter:[
                                    {"ip":"1x.x.x.x","cc_set":"test","cc_module":"test"},
                                    {"ip":"x.x.x.x","cc_set":"test","cc_module":"test"}
                                ],
                                "transformer":[{"ip":"x.x.x.x","cc_set":"test","cc_module":"test"}],
                                "aggregator":[{"ip":"x.x.x.x","cc_set":"test","cc_module":"test"}]
                            }
                        }
                    },
                    "result": true
                }

        """
        node_info = get_node_info(params, debug_id)
        return self.return_ok(node_info)

    @params_valid(serializer=CreateSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/stream/debugs/ 创建调试任务
        @apiName debugs/
        @apiGroup Stream
        @apiParam {String} job_id
        @apiParam {String} heads rt的heads,多个head用逗号分割
        @apiParam {String} tails rt的tails,多个tail用逗号分割
        @apiParamExample {json} 参数样例:
            {
                "job_id": "123xx",
                "heads": "123_clean",
                "tails": "123_filter"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "debug_id": 1200
                    },
                    "result": true
                }
        """
        debug_id = create_debug(params)
        return self.return_ok({"debug_id": debug_id})

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, debug_id):
        """
        @api {post} /dataflow/stream/debugs/:debug_id/stop 停止调试任务
        @apiName debugs/:debug_id/stop
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
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
        stop_debug(debug_id)
        return self.return_ok()

    @detail_route(methods=["post"], url_path="error_data")
    @params_valid(serializer=SaveErrorDataSerializer)
    def error_data(self, request, debug_id, params):
        """
        @api {post} /dataflow/stream/debugs/:debug_id/error_data 设置错误信息
        @apiName debugs/:debug_id/error_data
        @apiGroup Stream
        @apiParam {string} job_id
        @apiParam {string} result_table_id
        @apiParam {string} error_code
        @apiParam {string} error_message
        @apiParam {string} debug_date
        @apiParamExample {json} 参数样例:
            {
                "job_id": "1234",
                "result_table_id": "1_abc",
                "error_code": "xxx",
                "error_message": "xxxxx",
                "debug_date": "2018-09-19 17:40:58"
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
        set_error_data(params, debug_id)
        return self.return_ok()

    @detail_route(methods=["post"], url_path="metric_info")
    @params_valid(serializer=MetricInfoSerializer)
    def metric_info(self, request, debug_id, params):
        """
        @api {post} /dataflow/stream/debugs/:debug_id/metric_info 设置节点metric信息
        @apiName debugs/:debug_id/metric_info
        @apiGroup Stream
        @apiParam {string} job_id
        @apiParam {long} input_total_count
        @apiParam {long} output_total_count
        @apiParam {long} filter_discard_count
        @apiParam {long} transformer_discard_count
        @apiParam {long} aggregator_discard_count
        @apiParam {string} result_table_id
        @apiParamExample {json} 参数样例:
            {
                "job_id": "1234",
                "input_total_count": 100,
                "output_total_count": 100,
                "filter_discard_count": 12,
                "transformer_discard_count": 0,
                "aggregator_discard_count": 0,
                "result_table_id": "2_abc"
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
        update_metric_info(params, debug_id)
        return self.return_ok()

    @detail_route(methods=["post"], url_path="result_data")
    @params_valid(serializer=SaveResultDataSerializer)
    def result_data(self, request, debug_id, params):
        """
        @api {post} /dataflow/stream/debugs/:debug_id/result_data 设置节点的结果数据
        @apiName debugs/:debug_id/result_data
        @apiGroup Stream
        @apiParam {string} job_id
        @apiParam {string} result_table_id
        @apiParam {string} result_data
        @apiParam {long} debug_date
        @apiParam {int} thedate
        @apiParamExample {json} 参数样例:
            {
                "job_id": "1234",
                "result_table_id": "1_abc",
                "result_data": "[{"gsid": 130011101},{"gsid": "xxxxxx"}]",
                "debug_date": 1537152075939,
                "thedate": 20180917
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
        set_result_data(params, debug_id)
        return self.return_ok()
