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
from dm_engine.monitor import master_monitor
from dm_engine.storage.status import AgentStorage, MasterStorage, WorkerStorage

logger = logging.getLogger(__name__)


class StatusHandler(BaseComponentService):
    def __init__(self, svr):
        super(StatusHandler, self).__init__(svr)

        self.master_storage = MasterStorage()
        self.now_time = time.time()
        self.report_status_time = 0

    @property
    def is_leader(self):
        return self.namespace.get("is_leader")

    @is_leader.setter
    def is_leader(self, value):
        with self.namespace.atomic():
            self.namespace.set("is_leader", value)

    def handle(self):
        self.now_time = time.time()
        self.elect_leader()

        if self.now_time - self.report_status_time > 10:
            self.report_status()

            if self.is_leader:
                self.hold_leader_status()
                self.cleanup_components_status()

            self.report_status_time = self.now_time

    def elect_leader(self):
        """
        Elect master leader and ensure only one leader to work.
        """
        leader_status = self.master_storage.get_leader_status()
        svr_id = leader_status["svr_id"]
        svr_hearttime = leader_status["svr_hearttime"]

        # Check Existence
        if not svr_id:
            self.hold_leader_status()
            logger.info("No Leader, SVR_ID({}) win leader identity.".format(self.svr.svr_id))
            return

        # Leader identity has been taken away
        if self.is_leader and svr_id != self.svr.svr_id:
            self.is_leader = False
            logger.info("SVR_ID({}) lost leader identity.".format(self.svr.svr_id))
            return

        # Hearttime miss the deadline 15 seconds
        if self.now_time - svr_hearttime > 15:
            self.hold_leader_status()
            logger.info("Hearttime expired, SVR_ID({}) win leader identity.".format(self.svr.svr_id))
            return

    def hold_leader_status(self):
        """
        Hold leader at once on the right condition.
        """
        self.master_storage.set_leader_status(self.svr.svr_id, self.now_time)

        # Waiting 1 second to confirm whether succeed to set leader.
        time.sleep(1)

        leader_status = self.master_storage.get_leader_status()
        if self.svr.svr_id != leader_status["svr_id"]:
            logger.warning("SVR_ID({}) failed to hold leader identity, maybe another succeed.".format(self.svr.svr_id))
            return False

        self.is_leader = True
        return True

    def report_status(self):
        self.master_storage.report_status(
            {
                "svr_id": self.svr.svr_id,
                "svr_ip": self.svr.svr_ip,
                "svr_pid": self.svr.svr_pid,
                "svr_cpu_count": self.svr.svr_cpu_count,
                "svr_hearttime": self.now_time,
            }
        )

        master_monitor.record_hearttime(self.svr)

    def cleanup_components_status(self):
        """
        Status Protector. Clean up expired components status, including master, agent and worker.
        """
        try:
            MasterStorage().cleanup_status()
            WorkerStorage().cleanup_status()
            AgentStorage().cleanup_status()
        except Exception as err:
            logger.exception("Fail to clean up components status, {}".format(err))

    def stop_after(self):
        self.master_storage.clear_status(self.svr.svr_id)
