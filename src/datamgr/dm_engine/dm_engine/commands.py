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

import unittest

import attr
from dm_engine.config import settings
from dm_engine.master.tasks_controller import TasksController
from dm_engine.storage.queue import QueueManager
from dm_engine.storage.status import (
    AgentStorage,
    MasterStorage,
    TaskStorage,
    WorkerStorage,
)
from dm_engine.utils.redis import RedisClient
from dm_engine.utils.time import timestamp_to_arrow
from dm_engine.worker.processors import TaskProcessor


def list_tasks(show_detail, task_code):
    tasks = sorted(TaskStorage().get_tasks(), key=lambda _t: _t.task_code)
    for task in tasks:
        if task_code and task.task_code != task_code:
            continue

        if show_detail:
            print_task(task)
        else:
            print(
                "TASK:  {code:50} TYPE:  {type:10} STATUS:  {status:10} START:  {start:30}  END:  {end:30}  "
                "WORKER:  {worker:10}".format(
                    code=task.task_code[:50],
                    type=task.work_type,
                    status=task.status_info.execution_status.value,
                    start=task.status_info.start_time_str,
                    end=task.status_info.end_time_str,
                    worker=task.status_info.worker_id,
                )
            )


def list_master():
    list_svrs_status(MasterStorage())


def list_agent():
    list_svrs_status(AgentStorage())


def list_worker():
    list_svrs_status(WorkerStorage())


def read_queue_status():
    task_count = QueueManager.get_task_count()
    tasks = QueueManager.get_tasks()

    print("Task count is {}, now show the first 10 tasks...".format(task_count))
    for task in tasks:
        print_task(task)


def list_svrs_status(svr_storage):
    for status in svr_storage.get_all_status().values():
        print(
            "ID: {id:20}  CPU_COUNT: {cpu:10}  "
            "HEARTTIME:  {hearttime:40}".format(
                id=status["svr_id"], cpu=status["svr_cpu_count"], hearttime=to_timestr(status["svr_hearttime"])
            )
        )


def to_timestr(ts):
    return str(timestamp_to_arrow(ts, settings.TIME_ZONE))


def print_task(task):
    kwargs = attr.asdict(task)
    kwargs["status_info"]["scheduled_time"] = timestamp_to_arrow(task.status_info.scheduled_time, settings.TIME_ZONE)
    kwargs["status_info"]["start_time"] = timestamp_to_arrow(task.status_info.start_time, settings.TIME_ZONE)
    kwargs["status_info"]["end_time"] = timestamp_to_arrow(task.status_info.end_time, settings.TIME_ZONE)
    kwargs["status_info"]["heartbeat_time"] = timestamp_to_arrow(task.status_info.heartbeat_time, settings.TIME_ZONE)
    for k, v in kwargs.items():
        if type(v) == dict:
            print("- {}:".format(k))
            for _k, _v in v.items():
                print("    - {}: {}".format(_k, _v))
        else:
            print("- {}: {}".format(k, v))
    print("-" * 80)
    print("\n")


def execute_unittest():
    loader = unittest.TestLoader()
    tests = loader.discover("tests/")

    test_runner = unittest.runner.TextTestRunner(verbosity=2)
    test_runner.run(tests)


def list_plan_tasks():
    """List execute plans from conf/tasks in the project."""
    tasks = TasksController.load_tasks()
    for t in tasks:
        print("TASK: {:50}  ENTRY: {:80}  PARAMS: {}".format(t.task_code, t.task_entry, t.task_params))


def run_task_local(task_code):
    """Run task in localhost by command, just for test and not to use task framework to run.

    :return:
    """
    tasks = TasksController.load_tasks()

    for t in tasks:
        if t.task_code == task_code:
            tp = TaskProcessor(t)
            tp.start()


def push_task(task_code):
    """Just push task not through master scheduler."""
    tasks = TasksController.load_tasks()

    for t in tasks:
        if t.task_code == task_code:
            QueueManager.push_task(t)


def get_task_config(task_code):
    tasks = TasksController.load_tasks()
    for t in tasks:
        if t.task_code == task_code:
            return t
    return None


def clear_status():
    """Clear all components status stored in redis backend"""
    keys = [
        "TASK_QUEUE_KEY",
        "TASK_STATUS_KEY",
        "TASK_UPDATE_EVENT_KEY",
        "MASTER_STATUS_KEY",
        "MASTER_LEADER_STATUS_KEY",
        "WORKER_STATUS_KEY",
        "AGENT_STATUS_KEY",
    ]
    for k in keys:
        RedisClient.delete(getattr(settings, k))
        print("Clear KEY={}".format(k))


def generate_task_sql(task_code):
    """Generate task sql from yaml file, make insert sql easy for user"""
    task = get_task_config(task_code)
    if task:
        sql = f"""
        INSERT INTO dm_engine_task_config
            (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
            `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
            `work_timeout`)
        VALUES
            ('{task.task_code}', '{task.task_name}', '{task.task_category}', '{task.task_loader}',
             '{task.task_entry}', '{task.task_params}', '{task.task_status.value}', '{task.work_type}',
             '{task.work_crontab}', '{task.work_status_interval}', '{task.work_timeout}');
        """
        print(sql)
