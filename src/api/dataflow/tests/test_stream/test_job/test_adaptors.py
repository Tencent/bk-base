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

from dataflow.stream.api.api_helper import DatabusApiHelper
from dataflow.stream.handlers import processing_job_info
from dataflow.stream.job.adaptors import FlinkAdaptor
from dataflow.stream.job.flink_job import FlinkJob
from dataflow.stream.settings import DeployMode


class TestAdaptors(APITestCase):
    def setUp(self):
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
        job_id = "123"
        self._job = FlinkJob(job_id)
        self.adaptor = FlinkAdaptor(self._job)

    def test_source_partitions(self):
        DatabusApiHelper.get_partition_for_result_table = mock.Mock(return_value=1)
        assert self.adaptor.type_id == "stream"
        assert self.adaptor.version == "1.7.2"
        assert self.adaptor.parallelism == 1
        assert self.adaptor.source_partitions == 1
        assert self.adaptor.task_manager_memory == 1024
        assert self.adaptor.get_yarn_cluster_total_tm_mem(partition=5) == 5120
        assert self.adaptor.get_yarn_session_total_tm_mem(partition=1) == 1024
        assert self.adaptor.get_yarn_session_total_tm_mem(partition=10) == 20480
        assert self.adaptor.deploy_mode == DeployMode.YARN_SESSION.value
        assert self.adaptor.cluster_name == "default_standard4"
        conf = {
            "submit_args": {
                "parallelism": 1,
                "cluster_name": "",
                "task_manager_memory": "",
                "jar_files": "",
                "use_savepoint": "",
                "code": "",
                "user_main_class": "",
            },
            "job_args": "",
        }
        assert self.adaptor.build_yarn_cluster_extra_info(conf) == {
            "geog_area_code": u"inland",
            "resource_group_id": u"default",
            "use_savepoint": False,
            "container": 1,
            "component_type": "flink",
            "job_manager_memory": 1024,
            "jar_file_name": "",
            "argument": '""',
            "service_type": "stream",
            "parallelism": 1,
            "slot": 1,
            "task_manager_memory": "",
            "yarn_queue": "",
            "mode": "yarn-cluster",
        }

        assert self.adaptor.build_yarn_session_extra_info(conf) == {
            "task_manager_memory": 1024,
            "parallelism": 1,
            "argument": '""',
            "job_name": "123",
            "jar_file_name": "",
        }
        exp = {
            "event_name": "run_job",
            "execute_id": 123,
            "event_info": '{"task_manager_memory": 1024, "parallelism": 1, '
            + '"argument": "\\"\\"", "job_name": "123", "jar_file_name": ""}',
        }
        assert self.adaptor.build_yarn_session_params(execute_id=123, conf=conf) == exp
