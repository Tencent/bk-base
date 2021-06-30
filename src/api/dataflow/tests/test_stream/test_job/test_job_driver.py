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

import mock as mock
from rest_framework.test import APITestCase

from dataflow.stream.handlers import processing_job_info, processing_stream_info
from dataflow.stream.job.job_driver import create_job, update_job
from dataflow.stream.models import ProcessingStreamInfo


class TestJobDriver(APITestCase):
    def setUp(self):
        job_config = (
            '{"heads":"591_avg_new_new2","tails":"591_avg_new_new2_ta","concurrency":1,'
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

    @mock.patch.object(processing_stream_info, "get")
    def test_create_job(self, processing_stream_info_get):
        processing_stream_info_get.return_value = ProcessingStreamInfo(stream_id="xxx", checkpoint_type="offset")
        job_id = create_job(
            {
                "project_id": 123,
                "component_type": "",
                "job_config": "",
                "jobserver_config": "",
                "processings": ["123"],
                "code_version": "1.7.2",
                "cluster_group": "default",
                "deploy_config": {},
            }
        )
        assert job_id.startswith("123_")

    @mock.patch.object(processing_stream_info, "get")
    def test_update_job(self, processing_stream_info_get):
        processing_stream_info_get.return_value = ProcessingStreamInfo(stream_id="xxx", checkpoint_type="offset")
        job_id = update_job(
            {
                "project_id": 123,
                "component_type": "",
                "job_config": {},
                "jobserver_config": "",
                "processings": ["123"],
                "code_version": "1.7.2",
                "cluster_group": "default",
                "deploy_config": {},
            },
            job_id="123",
        )
        assert job_id == "123"
