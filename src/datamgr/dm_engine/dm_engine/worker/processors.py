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
import os
import sys
import time
from abc import ABCMeta, abstractmethod
from importlib import import_module
from multiprocessing import Process, Value

from dm_engine.base.exceptions import RealTaskExecuteError, TaskExitedAbnormallyError
from dm_engine.config import settings
from dm_engine.env import TASK_CODE_ENVIRONMENT_VARIABLE
from dm_engine.tasks.imports import ImportTaskCls
from raven import Client

if os.name == "posix" and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

logger = logging.getLogger(__name__)
sentry_client = Client(settings.SENTRY_DSN) if settings.REPORT_SENTRY else None


class BaseTaskLoader:
    """
    Loader
    """

    __metaclass__ = ABCMeta

    def __init__(self, task):
        """
        According to entry function and execute paramaters, load and execute.

        @param {dm_engine.database.models.TaskInfo} task
        """
        self.task = task
        self.task_code = self.task.task_code
        self.task_category = self.task.task_category

        # Entry function path, such as 'module.func1'
        self.entry = self.task.task_entry

        # Paas to the task with kv structure.
        self.params = self.task.task_params

    def set_process_id(self, execute_process_id: int):
        """
        Set task.status_info.process_id field
        """
        self.task.status_info.process_id = execute_process_id

    @abstractmethod
    def run(self):
        pass


class ImportTaskLoader(BaseTaskLoader):
    """
    ImportTaskLoader can run function by import method.
    """

    @classmethod
    def setup_logging(cls, task_code):
        from dm_engine.config import settings
        from dm_engine.utils.log import configure_logging

        # Initial logging
        configure_logging(
            log_level=settings.LOG_LEVEL,
            log_module=task_code,
            log_dir=settings.LOG_DIR,
            add_root_logger=True,
            report_sentry=settings.REPORT_SENTRY,
            sentry_DSN=settings.SENTRY_DSN,
        )

    @classmethod
    def build_task(cls, task_entry: str, task_code: str, task_params: str):
        def _wrap(shared_result):
            cls.setup_logging(task_code)
            task_logger = logging.getLogger()

            try:
                params = json.loads(task_params) if task_params else None

                parts = task_entry.split(".")
                path_module = ".".join(parts[0:-1])
                path_func = parts[-1]

                entry_module = import_module(path_module)
                entry_func = getattr(entry_module, path_func)

                # Class Task
                if issubclass(entry_func, ImportTaskCls):
                    inst = entry_func()
                    if inst.run.__code__.co_argcount >= 2:
                        inst.run(params)
                    else:
                        inst.run()
                # Method Task
                else:
                    # Compatible with no parameters
                    if entry_func.__code__.co_argcount >= 1:
                        entry_func(params)
                    else:
                        entry_func()
            except Exception as err:
                if sentry_client:
                    sentry_client.captureException()
                task_logger.exception(f"Raise exception in task process: {err}")
                # Writeback result to master process ...
                shared_result.value = 0

        return _wrap

    def run(self):
        shared_result = Value("i", 1)
        p = Process(target=self.build_task(self.entry, self.task_code, self.params), args=(shared_result,))
        p.daemon = True
        p.start()

        # Sync PID Information for other threads
        self.set_process_id(p.pid)

        logger.info(f"Import task({self.entry}) and params({self.params})")
        p.join()

        if not shared_result.value:
            raise TaskExitedAbnormallyError()


class CommandTaskLoader(BaseTaskLoader):
    """
    CommandTaskLoader can run module by command.
    """

    def run(self):
        """
        Execution Entry
        """
        cmd = """python -m {entry} {params}""".format(entry=self.entry, params=self.params)
        env = dict(os.environ.items())
        env[TASK_CODE_ENVIRONMENT_VARIABLE] = self.task_code

        logger.info("Command TasK: {}".format(cmd))
        # @todo subprocee active status report to the main process
        popen_object = subprocess.Popen(
            cmd, shell=True, stderr=subprocess.STDOUT, cwd=os.getcwd(), universal_newlines=True, bufsize=0, env=env
        )

        while 1:
            return_code = popen_object.poll()
            if return_code is None:
                logger.debug("Task({}) running...".format(self.task_code))
                time.sleep(1)
                continue

            if return_code != 0:
                raise RealTaskExecuteError()

            return True


class TaskProcessor(object):

    registed_loaders = {"import": ImportTaskLoader, "command": CommandTaskLoader}

    def __init__(self, task):
        """

        @param {dm_engine.database.model.TaskInfo} task
        """
        self.task = task

    def start(self):
        """
        Main Entry
        """
        task_loader = self.task.task_loader

        if task_loader in self.registed_loaders:
            loader = self.registed_loaders[task_loader](self.task)
            loader.run()
