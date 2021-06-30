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

import atexit

from dm_engine.base.component import BaseComponentService
from dm_engine.base.svr import BaseHostSvr
from dm_engine.master.status_handler import StatusHandler
from dm_engine.master.tasks_controller import TasksController
from prometheus_client import multiprocess


class MasterSvr(BaseHostSvr):
    """
    Only one active leader server at one time，re-elect by status_handler component
    """

    NAME = "master"

    def __init__(self):
        super(MasterSvr, self).__init__()

        # Initial shared variable for threads
        with self.namespace.atomic():
            # Default noLeader mode
            self.namespace.set("is_leader", False)

        # Register cleanup process
        atexit.register(self.clear_metrics)

    @property
    def is_leader(self):
        return self.namespace.get("is_leader")

    def handle(self):
        pass

    def setup_services(self):
        """
        Setup components services and then they will be start in the main circle of host service.
        """
        # to_add_services = [TestComponentService(self)]
        to_add_services = [TasksController(self), StatusHandler(self)]
        self.services.extend(to_add_services)

    def clear_metrics(self):
        """
        Clean up when process exit.
        """
        multiprocess.mark_process_dead(self.svr_pid)


class TestComponentService(BaseComponentService):
    def handle(self):
        pass


if __name__ == "__main__":
    svr = MasterSvr()
    svr.start()
