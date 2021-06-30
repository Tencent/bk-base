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

from conf import settings

BKDATA_BIZ_ID = settings.BKDATA_BIZ_ID

RULE_AUDIT_TASK_QUEUE = "data_set_audit_tasks"
AUDIT_TASK_POOL_SIZE = 300
AUDIT_TASK_WAITING_INTERVAL = 0.5
AUDIT_TASK_START_TIMEOUT = 10

FETCH_TASK_TIMEOUT = 5
EVENT_RAW_DATA_NAME = "dataquality_event"
EVENT_TSDB_DB = "monitor_data_metrics"
EVENT_MEASUREMENT = "dataquality_event"

METRIC_REPORT_TOPIC = "bkdata_monitor_metrics591"
EVENT_REPORT_TOPIC = "dataquality_event"

MANAGER_SESSION_COUNT = 1
SESSION_RESTART_RETRY_COUNT = 3
SESSION_RESTART_WAITING_INTERVAL = 1
CORRECTING_DEBUG_SESSION_KEY = "data_correct_debug_session"
DATA_PROFILING_SESSION_KEY = "data_profiling_session"
SESSION_STATUS_CHECK_INTERVAL = 60
SESSION_HEATBEAT_INTERVAL = 300

TIMEZONE = getattr(settings, "TIMEZONE", "Asia/Shanghai")
TSDB_OP_ROLE_NAME = "op"

INFLUXDB_CONFIG = {
    "monitor_data_metrics": {
        "host": settings.DMONITOR_DATA_TSDB_HOST,
        "port": settings.DMONITOR_DATA_TSDB_PORT,
        "database": "monitor_data_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_custom_metrics": {
        "host": settings.DMONITOR_CUSTOM_TSDB_HOST,
        "port": settings.DMONITOR_CUSTOM_TSDB_PORT,
        "database": "monitor_custom_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_performance_metrics": {
        "host": settings.DMONITOR_PERFORMANCE_TSDB_HOST,
        "port": settings.DMONITOR_PERFORMANCE_TSDB_PORT,
        "database": "monitor_performance_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
}

PROFILING_REGISTERED_UDFS = [
    "udf_data_distribution",
    "udf_descriptive_statistics",
    "udf_cardinal_distribution",
]

CORRECTION_REGISTERED_UDFS = [
    "udf_regexp",
]

DEFAULT_GROUP_ID = getattr(settings, "DEFAULT_GROUP_ID", "data_profiling")
