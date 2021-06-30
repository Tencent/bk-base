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

import gevent
from gevent import monkey, pool

from api import datamanage_api
from dataquality import redis
from dataquality.audit.task import ResultTableAuditTaskGreenlet
from dataquality.model.base import AuditTaskStatus
from dataquality.settings import (
    AUDIT_TASK_POOL_SIZE,
    FETCH_TASK_TIMEOUT,
    RULE_AUDIT_TASK_QUEUE,
)

monkey.patch_all()


def audit_rules():
    task_pool = pool.Pool(AUDIT_TASK_POOL_SIZE)

    # 重启后从数据库中恢复正在运行中的审核任务
    try:
        audit_tasks = datamanage_api.tasks.list().data
        for audit_task in audit_tasks:
            if audit_task.get("status") == AuditTaskStatus.RUNNING.value:
                task = ResultTableAuditTaskGreenlet(
                    {
                        "data_set_id": audit_task.get("data_set_id"),
                        "rule_id": audit_task.get("rule"),
                        "rule_config": audit_task.get("rule_config"),
                    }
                )
                task.start()
                task_pool.add(task)
                gevent.sleep(FETCH_TASK_TIMEOUT)
    except Exception as e:
        logging.error("初始化运行中任务失败: {}".format(e), exc_info=True)

    while True:
        # TODO 接收框架信号终止进程

        # 从队列中获取审核任务
        logging.info("Fetching task from redis.")
        if not task_pool.full():
            audit_task_config = get_audit_task()
        else:
            logging.error("The task pool of current task is full.", exc_info=True)
            gevent.sleep(FETCH_TASK_TIMEOUT)
            continue

        if not audit_task_config:
            gevent.sleep(FETCH_TASK_TIMEOUT)
            continue

        rule_id = audit_task_config.get("rule_id")
        for greenlet in task_pool.greenlets:
            if greenlet._rule_id == rule_id:
                task_pool.kill(greenlet)

        logging.info(
            "Get task about result table({})".format(
                audit_task_config.get("data_set_id")
            )
        )
        try:
            task = ResultTableAuditTaskGreenlet(audit_task_config)
        except Exception as e:
            logging.error(
                (
                    "Raise exception({error}) when init audit task about result table({data_set_id})"
                ).format(
                    error=e,
                    data_set_id=audit_task_config.get("data_set_id"),
                ),
                exc_info=True,
            )
            continue

        task_pool.add(task)
        task.start()


def get_audit_task():
    task_config_str = redis.rpop(RULE_AUDIT_TASK_QUEUE)
    audit_task_config = json.loads(task_config_str or "{}")
    return audit_task_config


def push_audit_task(audit_task_config):
    redis.rpush(RULE_AUDIT_TASK_QUEUE, audit_task_config)
