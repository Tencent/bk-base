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

import mock
from rest_framework.test import APITestCase

from dataflow.stream.api.stream_jobnavi_helper import StreamJobNaviHelper
from dataflow.stream.exceptions.comp_execptions import JobNaviError


class TestStreamJobnaviHelper(APITestCase):
    def setUp(self):
        self.streamJobNaviHelper = StreamJobNaviHelper(geog_area_code="default")

    def test_get_execute_id(self, schedule_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.get_execute_result_by_status = mock.Mock(
            return_value=[{"execute_info": {"id": 456}}]
        )
        assert self.streamJobNaviHelper.get_execute_id(schedule_id, timeout) == 456

    def test_get_event_status(self, event_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(return_value={"is_processed": True, "process_info": 456})
        assert self.streamJobNaviHelper.get_event_status(event_id, timeout) == 456
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(return_value={"is_processed": False})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_event_status(event_id, timeout)

    def test_get_event_result(self, event_id=123):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.get_event_result(event_id) == 456
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(return_value={"is_processed": False})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_event_result(event_id)

    def test_result_for_kill_application(self, event_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.get_result_for_kill_application(event_id, timeout)
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(return_value={"is_processed": False})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_result_for_kill_application(event_id, timeout)

    def test_list_jobs_status(self, event_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.list_jobs_status(event_id, timeout) == 456
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": False}
        )
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.list_jobs_status(event_id, timeout)

    def test_get_run_cancel_job_result(self, event_id=123):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.get_run_cancel_job_result(event_id)
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(return_value={"is_processed": False})
        self.streamJobNaviHelper.get_jobnavi_timeout = mock.Mock(return_value=2)
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_run_cancel_job_result(event_id)

    def test_cancel_job_result_for_cluster(self, event_id=123):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.get_cancel_job_result_for_cluster(event_id)

    def test_get_execute_status(self, execute_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.get_execute_status = mock.Mock(return_value={"status": "running"})
        assert self.streamJobNaviHelper.get_execute_status(execute_id, timeout)
        self.streamJobNaviHelper.jobnavi.get_execute_status = mock.Mock(return_value={"status": "failed"})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_execute_status(execute_id, timeout)
        self.streamJobNaviHelper.jobnavi.get_execute_status = mock.Mock(return_value={"status": "stopped"})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_execute_status(execute_id, timeout)

    def test_get_yarn_session_execute_id(self, schedule_id=123):
        self.streamJobNaviHelper.jobnavi.get_execute_result_by_status = mock.Mock(
            return_value=[{"execute_info": {"id": 456}}]
        )
        assert self.streamJobNaviHelper.get_yarn_session_execute_id(schedule_id) == 456

    def test_yarn_cluster_all_execute_id(self, schedule_id=123):
        self.streamJobNaviHelper.jobnavi.get_execute_result_by_status = mock.Mock(
            return_value=[{"execute_info": {"id": 456}}]
        )
        assert self.streamJobNaviHelper.get_one_yarn_cluster_all_execute_id(schedule_id) == [456]

    def test_kill_execute_id(self, execute_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 456}
        )
        assert self.streamJobNaviHelper.kill_execute_id(execute_id) is None

    def test_request_jobs_status(self, execute_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={})
        assert self.streamJobNaviHelper.request_jobs_status(execute_id) == {}

    def test_request_jobs_resources(self, execute_id=123, extra_args={"key1": "value1"}):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={})
        assert self.streamJobNaviHelper.request_jobs_resources(execute_id, extra_args) == {}

    def test_run_flink_job(self, execute_id=123, job_name=123, resources_unit=123, jar_name=123, job_config=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={})
        assert self.streamJobNaviHelper.run_flink_job(execute_id, job_name, resources_unit, jar_name, job_config) == {}

    def test_cancel_flink_job(self, execute_id=123, job_name="job1"):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={})
        assert self.streamJobNaviHelper.cancel_flink_job(execute_id, job_name) == {}

    def test_submit_query_application_task(self, execute_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={"is_processed": True})
        assert self.streamJobNaviHelper.submit_query_application_task(execute_id) == {"is_processed": True}
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value=None)
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.submit_query_application_task(execute_id)

    def test_add_yarn_schedule(self, args=""):
        self.streamJobNaviHelper.jobnavi.create_schedule_info = mock.Mock(return_value={"is_processed": True})
        assert self.streamJobNaviHelper.add_yarn_schedule(args) == {"is_processed": True}

    def test_execute_yarn_schedule(self, schedule_id=123):
        self.streamJobNaviHelper.jobnavi.execute_schedule = mock.Mock(return_value={"is_processed": True})
        assert self.streamJobNaviHelper.execute_yarn_schedule(schedule_id) == {"is_processed": True}
        self.streamJobNaviHelper.jobnavi.execute_schedule = mock.Mock(return_value=None)
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.execute_yarn_schedule(schedule_id)

    def test_execute_schedule_meta_data(self, cluster_set=123, args=""):
        self.streamJobNaviHelper.jobnavi.get_schedule_info = mock.Mock(return_value={"is_processed": True})
        self.streamJobNaviHelper.jobnavi.update_schedule_info = mock.Mock(return_value=None)
        self.streamJobNaviHelper.jobnavi.create_schedule_info = mock.Mock(return_value=None)
        assert self.streamJobNaviHelper.execute_schedule_meta_data(cluster_set, args) is None
        self.streamJobNaviHelper.jobnavi.get_schedule_info = mock.Mock(return_value=None)
        assert self.streamJobNaviHelper.execute_schedule_meta_data(cluster_set, args) is None

    def test_get_schedule(self, schedule_id=123):
        self.streamJobNaviHelper.jobnavi.get_schedule_info = mock.Mock(return_value={"is_processed": True})
        assert self.streamJobNaviHelper.get_schedule(schedule_id) is True
        self.streamJobNaviHelper.jobnavi.get_schedule_info = mock.Mock(return_value=None)
        assert self.streamJobNaviHelper.get_schedule(schedule_id) is False

    def test_get_yarn_application_status(self, execute_id=123):
        self.streamJobNaviHelper.submit_query_application_task = mock.Mock(return_value=123)
        self.streamJobNaviHelper.get_event_status = mock.Mock(return_value="deployed")
        assert self.streamJobNaviHelper.get_cluster_app_deployed_status(execute_id) is True
        self.streamJobNaviHelper.get_event_status = mock.Mock(return_value="running")
        assert self.streamJobNaviHelper.get_cluster_app_deployed_status(execute_id) is False

    def test_send_event_for_kill_yarn_app(self, execute_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value=True)
        assert self.streamJobNaviHelper.send_event_for_kill_yarn_app(execute_id) is True

    def test_kill_yarn_application(self, execute_id=123, timeout=2):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value=True)
        self.streamJobNaviHelper.get_result_for_kill_application = mock.Mock(return_value=True)
        assert self.streamJobNaviHelper.kill_yarn_application(execute_id, timeout) is True

    def test_get_job_exceptions(self, execute_id=123, job_name="job1"):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value=True)
        self.streamJobNaviHelper.get_event_result = mock.Mock(return_value='{"status":"running"}')
        assert self.streamJobNaviHelper.get_job_exceptions(execute_id, job_name) == {"status": "running"}

    def test_send_event(self, execute_id=123, event_name="event1"):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value=True)
        assert self.streamJobNaviHelper.send_event(execute_id, event_name) is True

    def test_valid_yarn_service_schedule(self):
        self.streamJobNaviHelper.jobnavi.get_schedule_info = mock.Mock(return_value=False)
        self.streamJobNaviHelper.jobnavi.create_schedule_info = mock.Mock(return_value=True)
        assert self.streamJobNaviHelper.valid_yarn_service_schedule() is None

    def test_get_yarn_service_execute(self):
        self.streamJobNaviHelper.wait_for_deployed = mock.Mock(return_value=True)
        self.streamJobNaviHelper.jobnavi.get_execute_result_by_status = mock.Mock(
            return_value=[{"execute_info": {"id": 123}}]
        )
        assert self.streamJobNaviHelper.get_yarn_service_execute() == 123
        self.streamJobNaviHelper.jobnavi.get_execute_result_by_status = mock.Mock(return_value=None)
        self.streamJobNaviHelper.jobnavi.execute_schedule = mock.Mock(return_value=123)
        assert self.streamJobNaviHelper.get_yarn_service_execute() == 123

    def test_wait_for_deployed(self, execute_id=123):
        self.streamJobNaviHelper.jobnavi.get_execute_status = mock.Mock(return_value={"status": "running"})
        assert self.streamJobNaviHelper.wait_for_deployed(execute_id) == {"status": "running"}
        self.streamJobNaviHelper.jobnavi.get_execute_status = mock.Mock(return_value={"status": "stopped"})
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.wait_for_deployed(execute_id)

    def test_send_yarn_state_event(self, exec_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={"status": "running"})
        self.streamJobNaviHelper.get_result = mock.Mock(return_value={"status": "running"})
        assert self.streamJobNaviHelper.send_yarn_state_event(exec_id) == {"status": "running"}

    def test_send_yarn_apps_event(self, exec_id=123):
        self.streamJobNaviHelper.jobnavi.send_event = mock.Mock(return_value={"status": "running"})
        self.streamJobNaviHelper.get_result = mock.Mock(return_value={"status": "running"})
        assert self.streamJobNaviHelper.send_yarn_apps_event(exec_id) == {"status": "running"}

    def test_get_result(self, event_id=123):
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": True, "process_info": 123}
        )
        assert self.streamJobNaviHelper.get_result(event_id) == 123
        self.streamJobNaviHelper.jobnavi.get_event = mock.Mock(
            return_value={"is_processed": True, "process_success": False, "process_info": 123}
        )
        with self.assertRaises(JobNaviError):
            self.streamJobNaviHelper.get_result(event_id)
