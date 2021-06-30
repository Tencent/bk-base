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

from dataflow.shared.handlers import dataflow_job
from dataflow.stream.handlers.job_handler import JobHandler
from dataflow.stream.result_table.result_table_driver import *
from dataflow.stream.result_table.result_table_serializers import *
from dataflow.stream.views.base_views import BaseViewSet


class ResultTableViewSet(BaseViewSet):
    """
    result table api
    """

    lookup_field = "result_table_id"

    @detail_route(methods=["post"], url_path="reset_checkpoint")
    def reset_checkpoint(self, request, result_table_id):
        """
        @api {post} /dataflow/stream/result_tables/:result_table_id/reset_checkpoint/ 迁移kafka集群时重置RT对应的checkpoint
        @apiName result_tables/:result_table_id/reset_checkpoint
        @apiParam {string} flow_id 需要重置checkpoint任务所在的flow_id
        @apiParamExample {json} 参数样例:
            {
              "flow_id": None
            }
        @apiGroup Stream
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "result": true
                }
        """
        args = request.data
        flow_id = args.get("flow_id", None)
        job_id = dataflow_job.find_job_id_by_flow_id(flow_id)
        JobHandler(job_id).reset_checkpoint_on_migrate_kafka(result_table_id)
        return self.return_ok()

    @list_route(methods=["get"], url_path="get_chain")
    @params_valid(serializer=GetChainSerializer)
    def get_chain(self, request, params):
        """
        @api {get} /dataflow/stream/result_tables/get_chain/ 获取heads和tails之间的result table信息
        @apiName result_tables/get_chain/
        @apiGroup Stream
        @apiParam {string} heads chain的head，逗号分割
        @apiParam {string} tails chain的tail，逗号分割
        @apiParamExample {json} 参数样例:
            {
              "heads": "2_clean",
              "tails": "2_test",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "2_test": {
                      "description": "xxx",
                      "fields": [
                        {
                          "origin": "",
                          "field": "dtEventTime",
                          "type": "string",
                          "description": "event time"
                        },
                        {
                          "origin": "",
                          "field": "par_offset",
                          "type": "string",
                          "description": "offset"
                        },
                        {
                          "origin": "",
                          "field": "path",
                          "type": "string",
                          "description": "日志路径"
                        }
                      ],
                      "storages": [
                        "tspider","kafka"
                      ],
                      "id": "2_test",
                      "window": {
                        "count_freq": 0,
                        "length": 0,
                        "type": null,
                        "waiting_time": 0
                      },
                      "parents": [
                        "2_clean"
                      ],
                      "output": {
                        "kafka_info": "host:port"
                      },
                      "processor": {
                        "processor_type": "common_transform",
                        "processor_args": "SELECT * FROM 2_test42_clean"
                      },
                      "name": "test"
                    },
                    "2_clean": {
                      "input": {
                        "kafka_info": "host:port"
                      },
                      "description": "xxx",
                      "id": "2_clean",
                      "fields": [
                        {
                          "origin": "",
                          "field": "dtEventTime",
                          "type": "string",
                          "description": "event time"
                        },
                        {
                          "origin": "",
                          "field": "par_offset",
                          "type": "string",
                          "description": "offset"
                        },
                        {
                          "origin": "",
                          "field": "path",
                          "type": "string",
                          "description": "日志路径"
                        }
                      ],
                      "name": "clean"
                    }
                }
        """

        result_tables = load_chain(params["heads"], params["tails"])
        return self.return_ok(result_tables)

    @detail_route(methods=["get"], url_path="associate_data_source_info")
    def associate_data_source_info(self, request, result_table_id):
        """
        @api {get} /dataflow/stream/result_tables/:result_table_id/associate_data_source_info 获取关联数据源信息
        @apiName result_tables/:result_table_id/associate_data_source_info
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "",
                    "code": "00",
                    "data": {
                        "fields": [{
                                "is_key": 0,
                                "type": "string",
                                "name": "city1",
                                "description": "2"
                            }, {
                                "is_key": 0,
                                "type": "string",
                                "name": "city2",
                                "description": "3"
                            }, {
                                "is_key": 1,
                                "type": "string",
                                "name": "cityid",
                                "description": "1"
                            }
                        ],
                        "storage_info": {
                            "query_mode": "single",
                            "redis_type": "private",
                            "data_id": 101078,
                            "host": "127.0.0.1",
                            "password": "xxxxxx",
                            "port": "30000"
                        },
                        "table_name": "test_static_csv",
                        "bk_biz_id": 591
                    },
                    "result": true
                }
        """

        data = get_associate_data_source_info(result_table_id)
        return self.return_ok(data)
