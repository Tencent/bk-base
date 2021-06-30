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

from dataquality import redis, session
from dataquality.model.core import AuditTask, AuditTaskStatus
from dataquality.settings import RULE_AUDIT_TASK_QUEUE


def audit_rules_beat():
    running_audit_rules = {}
    while True:
        # TODO 接收框架信号终止进程

        tasks = session.query(AuditTask).filter(
            AuditTask.status == AuditTaskStatus.RUNNING.value
        )
        for audit_task in tasks:
            if audit_task.rule_id not in running_audit_rules:
                logging.info("Push {} to redis.".format(audit_task))
                running_audit_rules[audit_task.rule_id] = audit_task
                redis.rpush(
                    RULE_AUDIT_TASK_QUEUE,
                    json.dumps(
                        {
                            "data_set_id": audit_task.data_set_id,
                            "rule_id": audit_task.rule_id,
                            "rule_config": audit_task.rule_config,
                        }
                    ),
                )

        gevent.sleep(1)
