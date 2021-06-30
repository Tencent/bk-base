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

DM-Engine project configuration for running
"""


import os

# Enviroment Configuration
BASEPATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUN_ENV = "TEST"


# TimeZone
TIME_ZONE = "Asia/Shanghai"

# error report Configuration
REPORT_SENTRY = False
SENTRY_DSN = ""

# Loggger Configuration
LOG_LEVEL = "INFO"
LOG_DIR = None

REDIS_CONFIG = {"default": {"port": 6379, "host": "127.0.0.1", "password": ""}}

MIN_WORKER_NUM = 1
MAX_WORKER_NUM = 50

# REDIS KEYS
TASK_QUEUE_KEY = "dm_engine.tasks_mq"
TASK_STATUS_KEY = "dm_engine.tasks_status"
TASK_UPDATE_EVENT_KEY = "dm_engine.task_event_mq"
MASTER_STATUS_KEY = "dm_engine.master_status"
MASTER_LEADER_STATUS_KEY = "dm_engine.master_leader_status"
WORKER_STATUS_KEY = "dm_engine.worker_status"
AGENT_STATUS_KEY = "dm_engine.agent_status"

# Prometheus Export Information
MASTER_METRICS_PORT = 21001
WORKER_METRICS_PORT = 21002
