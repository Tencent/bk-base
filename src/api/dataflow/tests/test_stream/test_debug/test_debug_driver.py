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

from rest_framework.test import APITestCase

from dataflow.stream.debug.debug_driver import get_basic_info, get_node_info, insert_debug_metric_log
from dataflow.stream.handlers import processing_job_info


class TestDebugDriver(APITestCase):
    def setUp(self):
        insert_debug_metric_log(
            debug_id="debug__123", job_type="stream", processing_id="123", job_id="debug__123", operator="test"
        )
        job_config = (
            '{"heads":"591_avg_new_new2","tails":"591_avg_new_new2_ta","concurrency":null,'
            + '"offset":1,"processings":["591_avg_new_new2_ta"]}'
        )
        processing_job_info.save(
            job_id="123",
            processing_type="stream",
            component_type="flink",
            jobserver_config='{"geog_area_code":"inland","cluster_id":"default"}',
            cluster_group="default",
            implement_type="sql",
            programming_language="java",
            code_version="default",
            cluster_name=None,
            deploy_mode=None,
            created_at="2021-06-01 00:00:01",
            deploy_config="{}",
            job_config=job_config,
        )

    def test_get_basic_info(self):
        debug_id = "debug__123"
        assert get_basic_info(debug_id) == {
            "debug_error": {},
            "result_tables": {"123": {"output_total_count": 0, "warning_count": 0}},
        }

    def test_get_node_info(self):
        debug_id = "debug__123"
        assert get_node_info({}, debug_id) == {
            "debug_metric": {
                "aggregator_discard_count": 0,
                "filter_discard_count": 0,
                "aggregator_discard_rate": 0.0,
                "warning_info": [],
                "metric_info": [
                    u"\u6570\u636e\u8fc7\u6ee4\u4e22\u5f03\u73870.0%",
                    u"\u6570\u636e\u8f6c\u6362\u4e22\u5f03\u73870.0%",
                    u"\u6570\u636e\u805a\u5408\u4e22\u5f03\u73870.0%",
                ],
                "filter_discard_rate": 0.0,
                "input_total_count": 0,
                "transformer_discard_rate": 0.0,
                "output_total_count": 0,
                "transformer_discard_count": 0,
            },
            "debug_data": {"discard_data": {"filter": [], "aggregator": [], "transformer": []}, "result_data": []},
            "debug_errcode": {},
        }
