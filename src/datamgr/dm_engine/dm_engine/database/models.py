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
from abc import ABCMeta
from collections import namedtuple
from enum import Enum
from typing import List

import attr
from dm_engine.config import settings
from dm_engine.utils.schedules import Crontab
from dm_engine.utils.time import timestamp_to_arrow
from dm_engine.utils.tools import generate_random_string


class TaskStatus(Enum):
    ON = "on"
    OFF = "off"


class ExecutionStatus(Enum):
    PEDDING = "pedding"  # Init status.
    WAITING = "waiting"  # MasterSvr push the task to queue and not be executed.
    RUNNING = "running"  # WorkerSvr run the task.
    FAILED = "failed"  # Task is done, but it failed in execution.
    SUCCESS = "success"  # Task is done and successful.


class WorkType(Enum):
    LONG = "long"
    INTERVAL = "interval"
    ONCE = "once"


@attr.s
class TaskStatusInfo(object):
    def _execution_status_converter(val):
        if type(val) is ExecutionStatus:
            return val
        return ExecutionStatus(val)

    # Timestamp Fields for Status Machine
    scheduled_time = attr.ib(type=int, default=0, converter=int)
    start_time = attr.ib(type=int, default=0, converter=int)
    end_time = attr.ib(type=int, default=0, converter=int)
    heartbeat_time = attr.ib(type=int, default=0, converter=int)
    execution_status = attr.ib(
        type=ExecutionStatus, default=ExecutionStatus.PEDDING, converter=_execution_status_converter
    )
    # Worker Information
    worker_id = attr.ib(type=str, default="")
    process_id = attr.ib(type=int, default=0)

    @property
    def scheduled_time_str(self):
        return self.to_timestr(self.scheduled_time)

    @property
    def start_time_str(self):
        return self.to_timestr(self.start_time)

    @property
    def end_time_str(self):
        return self.to_timestr(self.end_time)

    @property
    def heartbeat_time_str(self):
        return self.to_timestr(self.heartbeat_time)

    @staticmethod
    def to_timestr(ts: int) -> str:
        if ts == 0:
            return "NONE"
        return str(timestamp_to_arrow(ts, settings.TIME_ZONE))


@attr.s
class TaskInfo(object):
    def _status_info_converter(val):
        if isinstance(val, TaskStatusInfo):
            return val
        return TaskStatusInfo(**val)

    task_id = attr.ib(type=str, converter=str)
    task_code = attr.ib(type=str, converter=str)
    task_name = attr.ib(type=str, converter=str)
    task_category = attr.ib(type=str, converter=str)
    task_loader = attr.ib(type=str, converter=str)
    task_entry = attr.ib(type=str, converter=str)
    task_params = attr.ib(type=str, converter=str, default="")
    task_status = attr.ib(type=TaskStatus, converter=TaskStatus, default="on")
    work_type = attr.ib(type=str, converter=str, default=WorkType.ONCE.value)
    work_crontab = attr.ib(type=Crontab, converter=Crontab, default="* * * * *")
    work_status_interval = attr.ib(type=int, converter=int, default=60)
    work_timeout = attr.ib(type=int, converter=int, default=60)

    status_info = attr.ib(type=TaskStatusInfo, default=attr.Factory(TaskStatusInfo), converter=_status_info_converter)

    @classmethod
    def create(cls, info):
        """
        Use task configuration to create TaskInfo instance and fillin the necessary fields.
        """
        task_id = cls.build_task_id(info["task_code"])
        info["task_id"] = task_id

        cleaned_info = {}
        for f in attr.fields(cls):
            if f.name in info:
                cleaned_info[f.name] = info[f.name]
        return cls(**cleaned_info)

    @staticmethod
    def build_task_id(task_code):
        return "{time}:{code}:{index}".format(time=int(time.time()), code=task_code, index=generate_random_string(8))

    def to_waiting(self):
        self.status_info.scheduled_time = time.time()
        self.status_info.execution_status = ExecutionStatus.WAITING

        # Reset status
        self.status_info.start_time = 0
        self.status_info.end_time = 0
        self.status_info.worker_id = ""

    def to_running(self):
        self.status_info.start_time = time.time()
        self.status_info.execution_status = ExecutionStatus.RUNNING

    def to_success(self):
        self.status_info.end_time = time.time()
        self.status_info.execution_status = ExecutionStatus.SUCCESS

    def to_failed(self):
        self.status_info.end_time = time.time()
        self.status_info.execution_status = ExecutionStatus.FAILED


# For filter
Condition = namedtuple("Condition", ("field", "value"))

# For assignment
FieldPair = namedtuple("FieldPair", ("field", "value"))


class ModelManager:
    __metaclass__ = ABCMeta

    _db_table = ""
    _timestamp_field = ()

    def __init__(self, client):
        """

        :param client: database connnection client
        """
        self.client = client

    def exists(self, conditions: List[Condition]) -> bool:
        """

        :param conditions: list of condition for filter
        """
        statement = """
            SELECT count(*) as cnt FROM {db_table}
            WHERE {conditions}
        """.format(
            db_table=self._db_table, conditions=" AND ".join(f'`{k}`="{v}"' for k, v in conditions)
        )
        content = self.client.query(statement)
        return content[0]["cnt"] > 0

    def list(self, conditions: List[Condition] = None) -> List:
        """

        :param conditions: list of condition for filter
        :return: list of records
        :rtype: list
        """
        if conditions:
            statement = """
                SELECT * FROM {db_table} WHERE {conditions}
            """.format(
                db_table=self._db_table, conditions=" AND ".join(f'`{k}`="{v}"' for k, v in conditions)
            )
        else:
            statement = """
                SELECT * FROM {db_table}
            """.format(
                db_table=self._db_table
            )

        content = self.client.query(statement)
        return content

    def create(self, pairs: List[FieldPair]):
        """

        :param pairs: list of field pair for adding
        :return: the execute result of SQL
        """
        statement = """
            INSERT INTO {db_table}({fields}) VALUES ({values})
        """.format(
            db_table=self._db_table,
            fields=", ".join(p.field for p in pairs),
            values=", ".join(self._build_field_value(p.field, p.value) for p in pairs),
        )
        return self.client.execute(statement)

    def update(self, pairs: List[FieldPair], conditions: List[Condition]):
        """

        :param pairs: list of field pair for adding
        :param conditions: list of condition for filter
        :return: the execute result of SQL
        """
        statement = """
            UPDATE {db_table} SET {fields}
            WHERE {conditions}
        """.format(
            db_table=self._db_table,
            fields=", ".join(f"`{k}`={self._build_field_value(k, v)}" for k, v in pairs),
            conditions=" AND ".join(f'`{k}`="{v}"' for k, v in conditions),
        )
        return self.client.execute(statement)

    def clear(self):
        """
        :return:
        """
        statement = """DELETE FROM {db_table}""".format(db_table=self._db_table)
        return self.client.execute(statement)

    def _build_field_value(self, field, value):
        if field in self._timestamp_field:
            if value > 0:
                return "from_unixtime({})".format(value)
            else:
                return "NULL"

        return "'{}'".format(value)


class TaskInfoManager(ModelManager):
    _db_table = "dm_engine_task_config"


class TaskRecordManager(ModelManager):
    _db_table = "dm_engine_task_record"
    _timestamp_field = ("runtime_scheduled_time", "runtime_start_time", "runtime_end_time")
