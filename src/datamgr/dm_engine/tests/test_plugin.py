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

from dm_engine.config import settings
from dm_engine.database.models import TaskInfoManager, TaskRecordManager
from dm_engine.plugin.syncdb import SyncDB
from dm_engine.utils.db import DBClient
from dm_engine.utils.redis import RedisClient

event1 = {
    "task_id": "1616465646:dm_engine_sleep:ebj5f11Q",
    "task_code": "dm_engine_sleep",
    "task_name": "dm_engine_sleep",
    "task_category": "dm_engine_demo",
    "task_loader": "import",
    "task_entry": "dm_engine.plugin.example.SleepTask",
    "task_params": '{"a": 11, "b": 212}',
    "task_status": "on",
    "work_type": "long",
    "work_crontab": "* * * * *",
    "work_status_interval": 60,
    "work_timeout": 60,
    "status_info": {
        "scheduled_time": 1616465646.3712268,
        "start_time": 0,
        "end_time": 0,
        "heartbeat_time": 0,
        "execution_status": "waiting",
        "worker_id": "",
    },
}

event2 = {
    "task_id": "1616465646:dm_engine_sleep:ebj5fqww",
    "task_code": "dm_engine_sleep_once",
    "task_name": "dm_engine_sleep_once",
    "task_category": "dm_engine_demo",
    "task_loader": "import",
    "task_entry": "dm_engine.plugin.example.SleepTask",
    "task_params": "",
    "task_status": "on",
    "work_type": "once",
    "work_crontab": "* * * * *",
    "work_status_interval": 60,
    "work_timeout": 60,
    "status_info": {
        "scheduled_time": 1616465646.3712268,
        "start_time": 1616465646.3712268,
        "end_time": 1616465646.3712268,
        "heartbeat_time": 1616465646.3712268,
        "execution_status": "success",
        "worker_id": "",
    },
}


def test_syncdb_task():
    record_manager = TaskRecordManager(DBClient)
    record_manager.clear()
    RedisClient.delete(settings.TASK_UPDATE_EVENT_KEY)

    RedisClient.lpush(settings.TASK_UPDATE_EVENT_KEY, json.dumps(event1))
    SyncDB().run(once=True)

    objects = record_manager.list()
    assert len(objects) == 1
    assert objects[0]["task_id"] == "1616465646:dm_engine_sleep:ebj5f11Q"

    info_manager = TaskInfoManager(DBClient)
    info_manager.clear()

    RedisClient.lpush(settings.TASK_UPDATE_EVENT_KEY, json.dumps(event2))
    SyncDB().run(once=True)
    objects = info_manager.list()
    assert len(objects) == 1
    assert objects[0]["task_code"] == "dm_engine_sleep_once"
