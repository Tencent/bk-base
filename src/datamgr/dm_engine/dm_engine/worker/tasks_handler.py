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

import logging

from dm_engine.base.component import BaseComponentService
from dm_engine.config import settings
from dm_engine.monitor import worker_monitor
from dm_engine.storage.queue import QueueManager
from dm_engine.storage.status import TaskStorage
from dm_engine.worker.processors import TaskProcessor
from raven import Client

logger = logging.getLogger(__name__)
sentry_client = Client(settings.SENTRY_DSN) if settings.REPORT_SENTRY else None


class TaskHandler(BaseComponentService):
    """
    TaskHandler consume the tasks in the queue.
    """

    def __init__(self, worker):
        super(TaskHandler, self).__init__(worker)
        self.idle = True
        self.task_storage = TaskStorage()

    @property
    def task(self):
        return self.namespace.get("task")

    @task.setter
    def task(self, task):
        """
        Note, task is assigned here, but some attributes of the task can be change in other place of TaskHandler
        """
        self.namespace.set("task", task)

    def handle(self):
        task = QueueManager.pop_task()
        if task is not None:
            logger.info("Receive task({}), config={}".format(task.task_code, task))
            self.run_task(task)

    def run_task(self, task):
        """

        :param task
        :type  task: dm_engine.database.model.TaskInfo
        """
        self.idle = False

        self.task = task
        self.task.status_info.worker_id = self.svr.svr_id
        self.task.to_running()
        self.task_storage.set_task(self.task)

        worker_monitor.record_task_entry(self.task)
        try:
            processor = TaskProcessor(task)
            ret = processor.start()
            logger.info("Succeed to execute task {}, result={}".format(task.task_code, ret))
            self.task.to_success()
        except Exception as e:
            if sentry_client:
                sentry_client.captureException()
            logger.exception("Failed to execute task({}), error={}".format(task.task_code, e))
            self.task.to_failed()
        finally:
            self.task_storage.set_task(self.task)
            # Count execute result of task...
            worker_monitor.record_task_result(self.task)

            self.idle = True
            self.task = None
