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

from prometheus_client import CollectorRegistry, Counter, Gauge, multiprocess


class MasterMonitor:
    def __init__(self):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        self.collector_registry = registry
        self.master_push_task_count = Counter(
            name="master_push_task_count",
            documentation="",
            labelnames=("svr_id", "task_code"),
            registry=self.collector_registry,
        )
        self.master_running_seconds = Gauge(
            name="master_running_seconds",
            documentation="",
            labelnames=("svr_id",),
            registry=self.collector_registry,
            multiprocess_mode="liveall",
        )
        self.master_leader_flag = Gauge(
            name="master_leader_flag",
            documentation="",
            labelnames=("svr_id",),
            registry=self.collector_registry,
            multiprocess_mode="liveall",
        )
        self.svr_time_cache = dict()

    def push_task(self, svr, task_code):
        self.master_push_task_count.labels(svr.svr_id, task_code).inc()

    def record_hearttime(self, svr):
        now_time = time.time()
        pre_time = self.svr_time_cache.setdefault(svr.svr_id, now_time)

        if now_time < pre_time:
            return

        self.master_running_seconds.labels(svr.svr_id).set(now_time - pre_time)
        self.master_leader_flag.labels(svr.svr_id).set(int(svr.is_leader))
