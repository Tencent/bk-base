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
import logging
import time

from dm_engine.config import settings
from dm_engine.database.models import (
    Condition,
    FieldPair,
    TaskInfo,
    TaskInfoManager,
    TaskRecordManager,
    TaskStatus,
    WorkType,
)
from dm_engine.tasks.imports import ImportTaskCls
from dm_engine.utils.db import DBClient
from dm_engine.utils.redis import RedisClient

logger = logging.getLogger(__name__)


class SyncDB(ImportTaskCls):
    def run(self, params=None, once=False):

        event_key = settings.TASK_UPDATE_EVENT_KEY
        while 1:
            if not DBClient.exist():
                logger.warning("SyncDB will sleep 300 second, because not database backend is usable.")
                time.sleep(300)

            event = RedisClient.rpop(event_key)
            if event is None:
                time.sleep(1)
                continue

            try:
                task = TaskInfo(**json.loads(event))
                self.sync_record(task)

                if task.work_type == WorkType.ONCE.value:
                    self.sync_config(task)
            except Exception as err:
                logger.exception(f"Fail to handle event {event}, {err}")

            if once:
                logger.info("Only run once....")
                return
            logger.info(f"Succeed to handle event {event}")

    @staticmethod
    def sync_record(task: TaskInfo):
        """sync to TaskRecordManager"""
        objects = TaskRecordManager(DBClient)
        conditions = [Condition("task_id", task.task_id)]
        if objects.exists(conditions):
            objects.update(
                [
                    FieldPair("runtime_scheduled_time", task.status_info.scheduled_time),
                    FieldPair("runtime_start_time", task.status_info.start_time),
                    FieldPair("runtime_end_time", task.status_info.end_time),
                    FieldPair("runtime_execution_status", task.status_info.execution_status.value),
                    FieldPair("runtime_worker_id", task.status_info.worker_id),
                ],
                conditions,
            )
        else:
            objects.create(
                [
                    FieldPair("task_id", task.task_id),
                    FieldPair("task_code", task.task_code),
                    FieldPair("task_name", task.task_name),
                    FieldPair("task_category", task.task_category),
                    FieldPair("task_loader", task.task_loader),
                    FieldPair("task_entry", task.task_entry),
                    FieldPair("task_params", task.task_params),
                    FieldPair("work_type", task.work_type),
                    FieldPair("work_crontab", task.work_crontab),
                    FieldPair("work_status_interval", task.work_status_interval),
                    FieldPair("work_timeout", task.work_timeout),
                    FieldPair("runtime_scheduled_time", task.status_info.scheduled_time),
                    FieldPair("runtime_start_time", task.status_info.start_time),
                    FieldPair("runtime_end_time", task.status_info.end_time),
                    FieldPair("runtime_execution_status", task.status_info.execution_status.value),
                    FieldPair("runtime_worker_id", task.status_info.worker_id),
                ]
            )

    @staticmethod
    def sync_config(task: TaskInfo):
        """Writeback TaskConfig and avoid re-run one once-task twice"""
        objects = TaskInfoManager(DBClient)
        conditions = [Condition("task_code", task.task_code)]
        if objects.exists(conditions):
            objects.update(
                [
                    FieldPair("task_status", TaskStatus.OFF.value),
                ],
                conditions,
            )
        else:
            objects.create(
                [
                    FieldPair("task_code", task.task_code),
                    FieldPair("task_name", task.task_name),
                    FieldPair("task_category", task.task_category),
                    FieldPair("task_loader", task.task_loader),
                    FieldPair("task_entry", task.task_entry),
                    FieldPair("task_params", task.task_params),
                    FieldPair("task_status", TaskStatus.OFF.value),
                    FieldPair("work_type", task.work_type),
                    FieldPair("work_crontab", task.work_crontab),
                    FieldPair("work_status_interval", task.work_status_interval),
                    FieldPair("work_timeout", task.work_timeout),
                ]
            )
