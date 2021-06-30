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

from dm_engine.database.models import ExecutionStatus
from prometheus_client import CollectorRegistry, Counter, Gauge, multiprocess


class WorkerMonitor:
    def __init__(self):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)

        self.collector_registry = registry

        self.worker_running_seconds = Gauge(
            name="worker_running_seconds",
            documentation="",
            labelnames=("svr_id", "worker_type"),
            registry=self.collector_registry,
            multiprocess_mode="liveall",
        )

        self.worker_task_execute_count = Counter(
            name="worker_task_execute_count",
            documentation="",
            labelnames=("worker_id", "task_code", "work_type"),
            registry=self.collector_registry,
        )
        self.worker_task_success_count = Counter(
            name="worker_task_success_count",
            documentation="",
            labelnames=("worker_id", "task_code", "work_type"),
            registry=self.collector_registry,
        )
        self.worker_task_fail_count = Counter(
            name="worker_task_fail_count",
            documentation="",
            labelnames=("worker_id", "task_code", "work_type"),
            registry=self.collector_registry,
        )

        self.worker_task_latest_running_seconds = Gauge(
            name="worker_task_latest_running_seconds",
            documentation="",
            labelnames=("task_code", "work_type"),
            registry=self.collector_registry,
        )

        self.svr_time_cache = dict()
        self.task_time_cache = dict()

    def record_hearttime(self, svr, worker_type="agent"):
        import os

        pid = os.getpid()
        if str(pid) not in svr.svr_id:
            print(21321321321)

        now_time = time.time()
        pre_time = self.svr_time_cache.setdefault(svr.svr_id, now_time)

        if now_time < pre_time:
            return

        self.worker_running_seconds.labels(svr.svr_id, worker_type).set(now_time - pre_time)

    def record_task_entry(self, task):
        self.worker_task_execute_count.labels(task.status_info.worker_id, task.task_code, task.work_type).inc()

    def record_task_result(self, task):
        if task.status_info.execution_status == ExecutionStatus.SUCCESS:
            self.worker_task_success_count.labels(task.status_info.worker_id, task.task_code, task.work_type).inc()
        else:
            self.worker_task_fail_count.labels(task.status_info.worker_id, task.task_code, task.work_type).inc()

        # Record hearttime one time at least.
        self.record_task_hearttime(task)
        self.task_time_cache.pop(task.task_code)

    def record_task_hearttime(self, task):
        now_time = time.time()
        pre_time = self.task_time_cache.setdefault(task.task_code, now_time)

        # The first 2 minute, not to report metrics. Task having less cost time, the latest result is more important.
        if now_time < pre_time:
            return

        self.worker_task_latest_running_seconds.labels(task.task_code, task.work_type).set(now_time - pre_time)
