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
import time
from abc import ABCMeta, abstractmethod

from dm_engine.database.models import ExecutionStatus
from dm_engine.storage.queue import QueueManager
from dm_engine.storage.status import TaskStorage

logger = logging.getLogger(__name__)


class BaseGenerator(object):
    """
    Base generator for publishing task.
    """

    __metaclass__ = ABCMeta

    def __init__(self, task):
        """
        @param {dm_engine.database.models.TaskInfo} task
        """
        self.task = task
        self.now_time = time.time()
        self.task_storage = TaskStorage()

    @abstractmethod
    def maintain(self, **kwargs) -> bool:
        """
        Processing and decide whether pushblish it.
        kwargs
            - latest_execution_status: str

        :return: whether the task is need been re-executed and published.
        """
        return False

    def publish(self):
        """
        The whole publish process. Including push task to the queeu and refresh
        task status infomation.
        """
        self.task.to_waiting()

        logger.info("Start to publish task({}), config={}.".format(self.task.task_code, self.task))

        QueueManager.push_task(self.task)
        # Record latest task status infomation
        self.task_storage.set_task(self.task)

        logger.info("Finish to publish task({}).".format(self.task.task_code))


class IntervalGenerator(BaseGenerator):
    """
    Generator for inverval tasks.
    """

    def maintain(self, **kwargs):
        if self.is_time():
            self.publish()
            return True
        return False

    def is_time(self):
        # last_scheduled_time = self.task.status_info.scheduled_time
        work_crontab = self.task.work_crontab
        return work_crontab.match(self.now_time)


class LongGenerator(BaseGenerator):
    """
    Generator for long tasks.
    """

    def maintain(self, **kwargs):
        self.publish()
        return True


class OnceGenerator(BaseGenerator):
    """
    Generator for once tasks.
    """

    def maintain(self, **kwargs):
        latest_execution_status = kwargs.get("latest_execution_status")
        # Only run once ...
        if latest_execution_status not in [ExecutionStatus.SUCCESS, ExecutionStatus.FAILED]:
            self.publish()
            return True
        return False
