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

import random

from .base import BaseTest


class TestStream(BaseTest):
    def jobs(self):
        data = {
            "job_id": "default_job_id",
            "default_job_id": random.choice(["ACTIVE", None]),
            "jar_name": "master-0.0.1.jar",
        }
        return self.success_response(data)

    def jobs_submit(self):
        data = {"operate_info": {"operate": "start"}}
        return self.success_response(data)

    def jobs_cancel(self):
        data = {"operate_info": {"operate": "stop"}}
        return self.success_response(data)

    def processings(self):
        data = {
            "processing_id": "591_stream_test_abc",
            "result_table_ids": ["591_stream_test_abc"],
            "heads": ["591_stream_test_abc"],
            "tails": ["591_stream_test_abc"],
        }
        return self.success_response(data)

    def debugs(self):
        data = {"debug_id": 1200}
        return self.success_response(data)

    def get_debug_basic_info(self):
        data = {
            "debug_id": 1200,
            "result_tables": {
                "123_parser": {"output_total_count": 2190, "warning_count": 0},
                "123_filter": {"output_total_count": 200, "warning_count": 1},
            },
            "debug_error": {
                # "error_result_table_id": "123_filter"
            },
        }
        return self.success_response(data)

    def get_debug_node_info(self):
        data = {
            "debug_id": 1200,
            "debug_errcode": {
                "error_code": 101,
                "error_message": "在【123_test】中的字段【aaa】转换失败",
            },
            "debug_metric": {
                "input_total_count": 45210,
                "output_total_count": 2,
                "filter_discard_count": 0,
                "filter_discard_rate": 0,
                "transformer_discard_count": 45210,
                "transformer_discard_rate": 1,
                "aggregator_discard_count": 0,
                "aggregator_discard_rate": 0,
                "metric_info": ["数据过滤丢弃率0%", "数据转换丢弃率100%", "数据聚合丢弃率0%"],
                "warning_info": ["【警告】数据转换丢失率为100%"],
            },
            "debug_data": {
                "result_data": [
                    {
                        "dtEventTime": "2017-10-18 20:36:04",
                        "ip": "x.x.x.x",
                        "cc_set": "test",
                        "cc_module": "test",
                    },
                    {
                        "dtEventTime": "2017-10-18 20:36:04",
                        "ip": "x.x.x.x",
                        "cc_set": "test",
                        "cc_module": "test",
                    },
                ],
                "discard_data": {
                    "filter": [
                        {
                            "dtEventTime": "2017-10-18 20:36:04",
                            "ip": "1x.x.x.x",
                            "cc_set": "test",
                            "cc_module": "test",
                        },
                        {
                            "dtEventTime": "2017-10-18 20:36:04",
                            "ip": "x.x.x.x",
                            "cc_set": "test",
                            "cc_module": "test",
                        },
                    ],
                    "transformer": [
                        {
                            "dtEventTime": "2017-10-18 20:36:04",
                            "ip": "x.x.x.x",
                            "cc_set": "test",
                            "cc_module": "test",
                        }
                    ],
                    "aggregator": [
                        {
                            "dtEventTime": "2017-10-18 20:36:04",
                            "ip": "x.x.x.x",
                            "cc_set": "test",
                            "cc_module": "test",
                        }
                    ],
                },
            },
        }
        return self.success_response(data)
