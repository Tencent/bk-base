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

from .base import BaseTest


class TestBatch(BaseTest):
    def jobs(self):
        data = {"processing_id": "2_f1_batch_01", "job_id": "default_job_id"}
        return self.success_response(data)

    def save_job(self):
        data = {"job_id": "default"}
        return self.success_response(data)

    def processings(self):
        data = {"result_table_ids": ["2_f1_batch_01"], "processing_id": "2_f1_batch_01"}
        return self.success_response(data)

    def get_result_table_data_tail(self):
        data = (
            '{"thedate":20190102,"dtEventTime":"2019-01-02 13:00:00","dtEventTimeStamp":1546405200000,'
            '"localTime":"2019-01-02 14:30:10","ip":"127.0.0.1","report_time":"2019-01-02 13:59:02",'
            '"gseindex":1159,"path":"/tmp/mayi/2.log","log":"Wed Jan 2 13:59:01 CST 2019|60"}'
        )
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
                # "error_result_table_id": "123_filter"     # 值为空表示调试失败
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

    def recalculates(self):
        data = {"recalculate_id": "recal_xxxx"}
        return self.success_response(data)

    def get_recalculate_basic_info(self):
        data = {
            "recalculate_id": "recal_xxxx",
            "status": "none",
        }
        return self.success_response(data)

    def stop_recalculate(self):
        data = {"recalculate_id": "recal_xxxx"}
        return self.success_response(data)

    def data_makeup_status_list(self):
        data = [
            {
                "schedule_time": "xxx",
                "status": "running",
                "status_str": "成功",
                "created_at": "",
                "updated_at": "",
            }
        ]
        return self.success_response(data)

    def data_makeup_check_execution(self):
        data = {"allowed_data_makeup": True}
        return self.success_response(data)
