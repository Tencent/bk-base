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

import time
from unittest import mock

from dm_engine.database.models import TaskInfo
from dm_engine.storage.status import TaskStorage
from dm_engine.worker.svr import WorkerSvr
from dm_engine.worker.tasks_handler import TaskHandler


@mock.patch("dm_engine.worker.tasks_handler.QueueManager.pop_task")
def test_hello_task(patch_pop_task):

    task = TaskInfo.create(
        {
            "task_id": "dm_engine_hello:x11",
            "task_code": "dm_engine_hello",
            "task_name": "dm_engine_hello",
            "task_category": "dm_engine_demo",
            "task_loader": "import",
            "task_entry": "dm_engine.plugin.example.hello",
            "task_params": "",
            "task_status": "on",
            "work_type": "interval",
            "work_crontab": "* * * * *",
            "work_status_interval": 60,
            "work_timeout": 60,
            "status_info": {
                "scheduled_time": time.time(),
                "start_time": 0,
                "end_time": 0,
                "heartbeat_time": 0,
                "execution_status": "waiting",
                "worker_id": "",
            },
        }
    )
    patch_pop_task.return_value = task

    handler = TaskHandler(WorkerSvr())
    handler.handle()

    task = TaskStorage().get_task("dm_engine_hello")
    assert task.status_info.execution_status.value == "success"


@mock.patch("dm_engine.worker.tasks_handler.QueueManager.pop_task")
def test_fly(patch_pop_task):

    task = TaskInfo.create(
        {
            "task_id": "dm_engine_fly:x11",
            "task_code": "dm_engine_fly",
            "task_name": "dm_engine_fly",
            "task_category": "dm_engine_demo",
            "task_loader": "import",
            "task_entry": "dm_engine.plugin.example.fly",
            "task_params": "",
            "task_status": "on",
            "work_type": "interval",
            "work_crontab": "* * * * *",
            "work_status_interval": 60,
            "work_timeout": 60,
            "status_info": {
                "scheduled_time": time.time(),
                "start_time": 0,
                "end_time": 0,
                "heartbeat_time": 0,
                "execution_status": "waiting",
                "worker_id": "",
            },
        }
    )
    patch_pop_task.return_value = task

    handler = TaskHandler(WorkerSvr())
    handler.handle()

    task = TaskStorage().get_task("dm_engine_fly")
    assert task.status_info.execution_status.value == "failed"
