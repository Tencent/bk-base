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
import os
import time

import yaml
from dm_engine.base.component import BaseComponentService
from dm_engine.base.exceptions import InvalidExecutionStatusError
from dm_engine.config import settings
from dm_engine.database.models import (
    ExecutionStatus,
    TaskInfo,
    TaskInfoManager,
    TaskStatus,
)
from dm_engine.master.generator import IntervalGenerator, LongGenerator, OnceGenerator
from dm_engine.monitor import master_monitor
from dm_engine.runtime import g_context
from dm_engine.storage.status import SvrStatusChoice, TaskStorage, WorkerStorage
from dm_engine.utils.db import DBClient

logger = logging.getLogger(__name__)


class TasksController(BaseComponentService):
    """
    TaskController pull task info from database, match task instance from global
    storage, and then maintain & publish it. TaskController has
    task management system. Task can be registed in the
    TaskController.registed_task_types dynamiclly. TaskController initial
    task instances accorrding to the TaskController.registed_task_types.
    """

    registed_generator = {"interval": IntervalGenerator, "long": LongGenerator, "once": OnceGenerator}

    interval = 1
    load_tasks_inverval = 60

    def __init__(self, svr):
        super(TasksController, self).__init__(svr)
        self.last_load_task_time = 0
        self.now_time = time.time()

        self.worker_storage = WorkerStorage()
        self.task_storage = TaskStorage()

    def handle(self):

        # Run only leader service.
        if self.namespace.get("is_leader"):
            self.now_time = time.time()
            self.renew_tasks_info()

    def renew_tasks_info(self):
        """
        Renew task info from db every 60 seconds
        """
        if self.now_time - self.last_load_task_time > self.load_tasks_inverval:
            latest_tasks = self.load_tasks()
            self.last_load_task_time = self.now_time

            logger.info("Reload latest task config, task count={}".format(len(latest_tasks)))

            logger.info("Maintain tasks including push & remove task...")
            self.maintain_tasks(latest_tasks)

    def maintain_tasks(self, latest_tasks):
        """
        Maintain and consider whether repush the task
        """
        current_tasks = self.task_storage.get_tasks()
        current_task_mapping = {task.task_code: task for task in current_tasks}

        for task in latest_tasks:
            task_code = task.task_code
            work_type = task.work_type

            if task_code in current_task_mapping:
                current_task = current_task_mapping[task_code]

                # Task from renew must inherit the current latest task status(Maybe in Redis)
                task.status_info = current_task.status_info

            logger.info("Start maintain task, config={}".format(task))
            if not self.is_endpoint_status(task):
                continue

            generator_cls = self.registed_generator.get(work_type, None)
            if generator_cls is not None:
                _obj = generator_cls(task)

                if _obj.maintain(latest_execution_status=task.status_info.execution_status):
                    # Return true, had published one task
                    master_monitor.push_task(self.svr, task_code)
            else:
                logger.error("Unknown work type {}.".format(work_type))

    def is_endpoint_status(self, task):
        execution_status = task.status_info.execution_status
        if execution_status in [ExecutionStatus.PEDDING, ExecutionStatus.FAILED, ExecutionStatus.SUCCESS]:
            return True
        elif execution_status in [ExecutionStatus.RUNNING]:
            worker_id = task.status_info.worker_id
            worker_status, worker_hearttime = self.worker_storage.get_status_conclusion_by_cache(worker_id)

            # If worker doesn't exist, running task need to restart.
            if worker_status == SvrStatusChoice.INACTIVE:
                logger.info(
                    "[{}] Previous task is running in the inactive worker, " "should be restart.".format(task.task_code)
                )
                return True
            elif worker_status == SvrStatusChoice.OFFLINE:
                logger.info(
                    "[{}] Previous task is running, but worker is offline, "
                    "last hearttime={}.".format(task.task_code, worker_hearttime)
                )
                return False
            else:
                logger.info("[{}] Previous task is running.".format(task.task_code))
                return False

        elif execution_status in [ExecutionStatus.WAITING]:
            logger.info("[{}] Previous task is still active in the queue.".format(task.task_code))
            return False

        raise InvalidExecutionStatusError()

    @classmethod
    def load_tasks(cls):
        """Load tasks configuration"""
        tasks = []
        try:
            tasks = cls.load_tasks_from_db()
            db_tasks_code_names = [t.task_code for t in tasks]
        except Exception as err:
            logger.exception(f"Fail to load tasks from Database, {err}")
            db_tasks_code_names = []

        try:
            fixed_tasks = cls.load_tasks_from_settings()
            # The Priority of TaskConfig from database is higher than [from settings]
            tasks.extend([t for t in fixed_tasks if t.task_code not in db_tasks_code_names])
        except Exception as err:
            logger.exception(f"Fail to load tasks from settings, {err}")

        # Filter with task_status=ON
        tasks = [t for t in tasks if t.task_status == TaskStatus.ON]

        return tasks

    @staticmethod
    def load_tasks_from_db():
        """
        Load task config from db and sync to this.task_info
        """
        if not DBClient.exist():
            return []

        objects = TaskInfoManager(DBClient)

        latest_tasks = []
        for _config in objects.list():
            latest_tasks.append(TaskInfo.create(_config))

        return latest_tasks

    @staticmethod
    def load_tasks_from_settings():
        """Load task config from project-settings configuration"""
        root = os.path.join(os.getcwd(), g_context.tasksdir)
        if not os.path.exists(root):
            return []

        # User Configuration
        yfiles = [
            os.path.join(root, filename) for filename in os.listdir(root) if os.path.splitext(filename)[1] == ".yaml"
        ]

        # Inner Configuration
        yfiles.append(os.path.join(settings.BASEPATH, "dm_engine/config/tasks/inner.yaml"))

        fixed_tasks = []
        for y in yfiles:
            with open(y) as f:
                content = yaml.load(f.read(), Loader=yaml.FullLoader)

            if content and content["plans"]:
                for item in content["plans"]:
                    fixed_tasks.append(TaskInfo.create(item))

        return fixed_tasks
