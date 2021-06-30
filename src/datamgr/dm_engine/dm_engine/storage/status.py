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

from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
import time
from enum import Enum

import attr
from dm_engine.base.json import DMJSONEncoder
from dm_engine.config import settings
from dm_engine.database.models import ExecutionStatus, TaskInfo
from dm_engine.utils.redis import RedisClient
from dm_engine.utils.tools import safe_num

logger = logging.getLogger(__name__)


class TaskStorage(object):
    """
    TaskStatusStorage manage all task status information. Its a adaptor for redis
    backend.TaskStatusStorage make caller focus on the get & set logic and not
    to know redis backend
    """

    TASK_STATUS_KEY = settings.TASK_STATUS_KEY
    TASK_UPDATE_EVENT_KEY = settings.TASK_UPDATE_EVENT_KEY

    def __init__(self):
        self.redis_client = RedisClient

    def get_tasks(self):
        """
        Get all task status information.
        """
        content = self.redis_client.hvals(self.TASK_STATUS_KEY)
        return [TaskInfo(**json.loads(c)) for c in content]

    def get_task(self, task_code):
        """
        Get one task status.

        @param {String} task_code
        """
        content = self.redis_client.hget(self.TASK_STATUS_KEY, task_code)
        return TaskInfo(**json.loads(content))

    def set_task(self, task):
        """
        Set one task status.

        @param {dm_engine.database.model.TaskInfo} task
        """
        task_code = task.task_code
        content = json.dumps(attr.asdict(task), cls=DMJSONEncoder)

        self.redis_client.hset(self.TASK_STATUS_KEY, task_code, content)
        # Now, the method is called on the task.status change. The events are excepted.
        self.redis_client.lpush(self.TASK_UPDATE_EVENT_KEY, content)

        logger.debug(
            "[TaskStorage] Set task status into TaskStorage, task_code={}, " "content={}".format(task_code, content)
        )

    def remove_task(self, task_code):
        """
        Remove task status from Global status namespace(Now redis...)
        """
        self.redis_client.hdel(self.TASK_STATUS_KEY, task_code)
        logger.info("[TaskStorage] Remove task status from TaskStorage, task_code={}".format(task_code))

    def cleanup_tasks(self, worker_storage):
        tasks = self.get_tasks()

        for task in tasks:
            worker_id = task.status_info.worker_id
            execution_status = task.status_info.execution_status
            task_code = task.task_code

            # Cleanup has running record
            if execution_status in [ExecutionStatus.WAITING, ExecutionStatus.RUNNING]:
                svr_status, _ = worker_storage.get_status_conclusion_by_cache(worker_id)
                if svr_status == SvrStatusChoice.INACTIVE:
                    self.remove_task(task_code)


class SvrStatusChoice(Enum):
    """
    active    Process is running and reporting hearttime normally, it has hearttime(valid less than 1 minute).
    offline   Maybe process is running, noly has not reported hearttime,
              it may be active after waiting some times.
    inactive  Process has no heartime for 10 minutes, process should not be running as expected.
    """

    ACTIVE = "active"
    OFFLINE = "offline"
    INACTIVE = "inactive"


ACTIVE_TIMEOUT = 60
INACTIVE_TIMEOUT = 300


class BaseSvrStatusStorage(object):
    STATUS_KEY = ""

    def __init__(self):
        self.redis_client = RedisClient
        self._cache_status = dict()

    def report_status(self, svr_status):
        """
        Report master statue into backend storage, one master service including following metrics
            - svr_id
            - svr_ip
            - svr_pid
            - svr_cpu_count
            - svr_hearttime

        Maybe some svr has more metrics ...
        """
        svr_id = svr_status["svr_id"]
        self.redis_client.hset(self.STATUS_KEY, svr_id, json.dumps(svr_status))

    def clear_status(self, svr_id):
        self.redis_client.hdel(self.STATUS_KEY, svr_id)

    def get_status_conclusion_by_cache(self, svr_id):
        """
        Get status from backend storage, and give the conclusion whether the svr is active.
        """
        now_time = time.time()

        content = self.redis_client.hget(self.STATUS_KEY, svr_id)

        if content is not None:
            status_info = json.loads(content)
            hearttime = status_info["svr_hearttime"]
        elif svr_id in self._cache_status:
            hearttime = self._cache_status[svr_id]
        else:
            hearttime = now_time

        delay = now_time - hearttime

        if delay <= ACTIVE_TIMEOUT:
            svr_status = SvrStatusChoice.ACTIVE
        elif delay > ACTIVE_TIMEOUT and delay <= INACTIVE_TIMEOUT:
            svr_status = SvrStatusChoice.OFFLINE
        else:
            svr_status = SvrStatusChoice.INACTIVE

        self._cache_status[svr_id] = hearttime
        return svr_status, hearttime

    def cleanup_status(self, timeout=60):
        """
        Clearn up expired status, default 60 seconds as expired time.
        """
        content = self.redis_client.hgetall(self.STATUS_KEY)

        now_time = time.time()
        for svr_id, svr_status in content.items():
            _status = json.loads(svr_status)

            # More than 1 minutes illustrate process is not active, hearttime can not be used.
            if now_time - _status["svr_hearttime"] > timeout:
                self.clear_status(svr_id)

    def get_all_status(self):
        all_raw_status = self.redis_client.hgetall(self.STATUS_KEY)
        all_status = dict()
        for svr_id, status in all_raw_status.items():
            all_status[svr_id] = json.loads(status)

        return all_status


class MasterStorage(BaseSvrStatusStorage):
    STATUS_KEY = settings.MASTER_STATUS_KEY

    LEADER_STATUS_KEY = settings.MASTER_LEADER_STATUS_KEY
    LEADER_ID_KEY = "svr_id"
    LEADER_HEARTTIME_KEY = "svr_hearttime"

    def get_leader_status(self):
        """
        Get Leader status from LEADER_STATUS_KEY in redis, hash structure including svr_id & svr_heartime.
        Return svr_id='' and svr_hearttime=0 if leader status not exist.
        """
        svr_id = self.redis_client.hget(self.LEADER_STATUS_KEY, self.LEADER_ID_KEY)
        svr_hearttime = self.redis_client.hget(self.LEADER_STATUS_KEY, self.LEADER_HEARTTIME_KEY)
        return {"svr_id": svr_id if svr_id else "", "svr_hearttime": safe_num(svr_hearttime)}

    def set_leader_status(self, svr_id, svr_hearttime):
        self.redis_client.hset(self.LEADER_STATUS_KEY, self.LEADER_ID_KEY, svr_id)
        self.redis_client.hset(self.LEADER_STATUS_KEY, self.LEADER_HEARTTIME_KEY, svr_hearttime)


class WorkerStorage(BaseSvrStatusStorage):
    STATUS_KEY = settings.WORKER_STATUS_KEY

    def get_active_worker_ids(self):
        content = self.redis_client.hgetall(self.STATUS_KEY)
        return content.keys()


class AgentStorage(BaseSvrStatusStorage):
    STATUS_KEY = settings.AGENT_STATUS_KEY
