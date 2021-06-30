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
import json

from common.api.base import DataResponse


class JobNaviApiMocker(object):

    def __init__(self, geog_area_code='inland', cluster="default"):
        self.geog_area_code = geog_area_code
        self.cluster = cluster

    def healthz(self):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def query_current_runners(self):
        return DataResponse({
            "result": True,
            "data": "{\"runner_1\": \"ok\", \"runner_2\": \"ok\"}",
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def add_schedule(self, data):
        return DataResponse({
            "result": True,
            "data": 101,
            "code": "1500200",
            "errors": None
        })

    def del_schedule(self, schedule_id):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def get_schedule(self, schedule_id):
        data = {
            "schedule_id": schedule_id,
            "type_id": "spark_sql",
            "description": "离线计算节点",
            "period": {
                "timezone": "Asia/shanghai",
                "cron_expression": "? ? ? ? ? ?",
                "frequency": 1,
                "start_time": 1539778425000,
                "delay": "1h",
            },
            "parents": [{
                "parent_id": "P_xxx",
                "rule": "all_finished",
                "type": "fixed",
                "value": "1h"
            }],
            "recovery": {
                "enable": True,
                "interval_time": "1h",
                "retry_times": 3
            },
            "execute_before_now": True,
            "node_label": "calc_cluster",
            "decommission_timeout": "1h",
            "max_running_task": 1,
            "created_by": "xxx"
        }

        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def update_schedule(self, args):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def overwrite_schedule(self, args):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def force_schedule(self, schedule_id):
        return DataResponse({
            "result": True,
            "data": "123456",
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def execute_schedule(self, schedule_id, schedule_time):
        return DataResponse({
            "result": True,
            "data": 101,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def get_execute_status(self, exec_id):
        data = {
            "execute_info": {
                "id": exec_id,
                "host": "xxx.xxx.xx.xx"
            },
            "schedule_id": "test_schedule",
            "schedule_time": 1539778425000,
            "status": "finished",
            "info": "xxx",
            "created_at": 1539778425000,
            "updated_at": 1539778425000
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def query_execute(self, schedule_id, limit):
        data = [
            {
                "execute_info": {
                    "id": 123,
                    "host": "xxx.xxx.xx.xx"
                },
                "schedule_id": schedule_id,
                "schedule_time": 1539778425000,
                "status": "running",
                "info": "xxx",
                "created_at": 1539778425000,
                "updated_at": 1539778425000
            },
            {
                "execute_info": {
                    "id": 124,
                    "host": "xxx.xxx.xx.xx"
                },
                "schedule_id": schedule_id,
                "schedule_time": 1539778425000,
                "status": "finished",
                "info": "xxx",
                "created_at": 1539778425000,
                "updated_at": 1539778425000
            }
        ]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def query_execute_by_status(self, schedule_id, schedule_time, status):
        data = [
            {
                "execute_info": {
                    "id": 123,
                    "host": "xxx.xxx.xx.xx"
                },
                "schedule_id": schedule_id,
                "schedule_time": schedule_time,
                "status": "failed",
                "info": "xxx",
                "created_at": 1539778425000,
                "updated_at": 1539778425000
            },
            {
                "execute_info": {
                    "id": 124,
                    "host": "xxx.xxx.xx.xx"
                },
                "schedule_id": schedule_id,
                "schedule_time": schedule_time,
                "status": "failed",
                "info": "xxx",
                "created_at": 1539778425000,
                "updated_at": 1539778425000
            }
        ]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def fetch_today_job_status(self):
        data = {
            "2018102210": {
                "running": 10,
                "preparing": 10,
                "finished": 10
            },
            "2018102211": {
                "running": 25,
                "preparing": 11,
                "finished": 11,
            }
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def fetch_last_hour_job_status(self):
        data = {
            "running": 10,
            "preparing": 10,
            "finished": 10
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def fetch_job_status_by_create_time(self):
        data = {
            "2018102210": {
                "running": 19,
                "preparing": 19,
                "finished": 19
            },
            "2018102211": {
                "running": 25,
                "preparing": 25,
                "finished": 25,
            }
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def send_event(self, execute_id, event_name, change_status, event_info):
        return DataResponse({
            "result": True,
            "data": 101,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def get_event_result(self, event_id):
        data = {
            "is_processed": True,
            "process_success": True,
            "process_info": "123"
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def query_processing_event_amount(self):
        return DataResponse({
            "result": True,
            "data": 101,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def admin_schedule_calculate_task_time(self, rerun_processings, rerun_model, start_time, end_time):
        data = [
            {
                "scheduleId": "job_1",
                "scheduleTime": 1234567890
            },
            {
                "scheduleId": "job_2",
                "scheduleTime": 1234567890
            }
        ]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def query_failed_executes(self, begin_time, end_time, type_id):
        data = [{
            "execute_info": {
                "id": 12345,
                "host": "host1",
                "rank": 0.0
            },
            "schedule_time": 1539778425000,
            "updated_at": 1539778425000,
            "data_time": 1539778425000,
            "created_at": 1539778425000,
            "started_at": 0,
            "schedule_id": "test_job",
            "status": "failed",
            "info": None
        }]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def list_data_makeup_execute(self, schedule_id, start_time, end_time, status):
        data = [
            {
                "schedule_time": 19254400,
                "status": "running",
                "created_at": 1234567890,
                "updated_at": 9876543210
            }
        ]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def check_data_makeup_allowed(self, schedule_id, schedule_time):
        data = {
            "status": "running",
            "is_allowed": False
        }
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def run_data_makeup(self, processing_id, rerun_processings, rerun_model,
                        target_schedule_time, source_schedule_time, dispatch_to_storage):
        return DataResponse({
            "result": True,
            "data": "[]",
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def list_runner_digests(self):
        return DataResponse({
            "result": True,
            "data": "{\"runner_1\": {\"status\":\"running\"}, \"runner_2\": {\"status\":\"running\"}}",
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def list_task_type(self):
        data = [
            {
                "type_id": "type-1",
                "tag": "stable",
                "main": "com.main1",
                "description": None,
                "env": "env1",
                "sys_env": None,
                "language": "python",
                "task_mode": "process",
                "recoverable": None
            },
        ]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def create_task_type(self, type_id, tag, main, env, sys_env, language, task_mode, recoverable, created_by,
                         description):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def delete_task_type_tag(self, type_id, tag):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def retrieve_task_type_tag_alias(self, type_id, tag):
        return DataResponse({
            "result": True,
            "data": "[]",
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def create_task_type_tag_alias(self, type_id, tag, alias, description):
        data = ["stable", "latest"]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def delete_task_type_tag_alias(self, type_id, tag, alias):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def retrieve_task_type_default_tag(self, type_id, node_label):
        data = ["stable", "latest"]
        return DataResponse({
            "result": True,
            "data": json.dumps(data),
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def create_task_type_default_tag(self, type_id, node_label, default_tag):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })

    def delete_task_type_default_tag(self, type_id, node_label):
        return DataResponse({
            "result": True,
            "data": None,
            "code": "1500200",
            "message": "ok",
            "errors": None
        })


def get_jobnavi_config_mock():
    return {"default": "inland"}
