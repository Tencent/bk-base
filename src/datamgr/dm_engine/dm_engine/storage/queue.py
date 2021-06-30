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

import attr
from dm_engine.base.json import DMJSONEncoder
from dm_engine.config import settings
from dm_engine.database.models import TaskInfo
from dm_engine.utils.redis import RedisClient

logger = logging.getLogger(__name__)

DEFAULT_TASK_QUEUE = settings.TASK_QUEUE_KEY


class _QueueManager(object):
    def __init__(self):
        self.redis_client = RedisClient
        self.key_name = settings

    def push_task(self, task):
        """
        Push task into the queue

        @param {database.model.TaskInfo} task
        """
        content = json.dumps(attr.asdict(task), cls=DMJSONEncoder)
        self.redis_client.lpush(DEFAULT_TASK_QUEUE, content)
        logger.debug("[QueueManager] Push task into queue, task={}".format(task))

    def pop_task(self):
        """
        Pop task from the queue

        @return {database.model.TaskInfo}
        """
        content = self.redis_client.rpop(DEFAULT_TASK_QUEUE)
        if content is None:
            return None

        task = TaskInfo(**json.loads(content))
        logger.debug("[QueueManager] Pop task from queue, task={}".format(task))
        return task

    def get_task_count(self):
        return self.redis_client.llen(DEFAULT_TASK_QUEUE)

    def get_tasks(self, start=0, end=9):
        """
        Default return 10 tasks.
        """
        all_content = self.redis_client.lrange(DEFAULT_TASK_QUEUE, start, end)
        return [TaskInfo(**json.loads(content)) for content in all_content]


QueueManager = _QueueManager()
