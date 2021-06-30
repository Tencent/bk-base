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
import logging
import time

from dm_engine.base.component import BaseComponentService
from dm_engine.monitor import worker_monitor
from dm_engine.storage.status import AgentStorage

logger = logging.getLogger(__name__)


class StatusHandler(BaseComponentService):
    def __init__(self, svr):
        super(StatusHandler, self).__init__(svr)
        self.now_time = time.time()
        self.report_status_time = 0
        self.agent_storage = AgentStorage()

    def handle(self):
        self.now_time = time.time()
        self.report_agent_status()

    def report_agent_status(self):

        if self.now_time - self.report_status_time > 10:
            self.agent_storage.report_status(
                {
                    "svr_id": self.svr.svr_id,
                    "svr_ip": self.svr.svr_ip,
                    "svr_pid": self.svr.svr_pid,
                    "svr_cpu_count": self.svr.svr_cpu_count,
                    "svr_hearttime": self.now_time,
                }
            )
            self.report_status_time = self.now_time

            worker_monitor.record_hearttime(self.svr, worker_type="agent")

    def stop_after(self):
        self.agent_storage.clear_status(self.svr.svr_id)
