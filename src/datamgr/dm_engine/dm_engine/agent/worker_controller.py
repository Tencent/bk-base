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
import logging
import multiprocessing as mp
import time

from dm_engine.base.component import BaseComponentService
from dm_engine.config import settings
from dm_engine.storage.queue import QueueManager
from dm_engine.storage.status import SvrStatusChoice, WorkerStorage
from dm_engine.worker.svr import main
from prometheus_client import multiprocess

logger = logging.getLogger(__name__)


class WorkerController(BaseComponentService):

    interval = 0.5

    def __init__(self, svr):
        super(WorkerController, self).__init__(svr)
        self.now_time = time.time()
        self.workers_group = dict()
        self.min_workers_num = settings.MIN_WORKER_NUM
        self.max_workers_num = settings.MAX_WORKER_NUM

        self.worker_storage = WorkerStorage()
        self.last_clean_zombies_time = 0

        # Register cleanup process
        atexit.register(self.clear_metrics)

    def handle(self):
        self.now_time = time.time()
        if self.current_worker_count < self.min_workers_num:
            self.create_worker()
            logger.info(
                "Started {} init worker, current worker number {}.".format(
                    self.min_workers_num, len(self.workers_group)
                )
            )
        # Worker Expansion condition
        # [Todo] 1. Queue tasks start to accumulate ...
        # [Done] 2. First task is waiting for long time ...
        tasks = QueueManager.get_tasks(0, 0)
        first_task = tasks[0] if len(tasks) > 0 else None

        if first_task is not None:
            if self.now_time - first_task.status_info.scheduled_time > 10:
                logger.info("First task is waiting for >= 10 seconds and create worker to support ...")
                self.create_worker()

        self.cleanup_zombies()

    def create_worker(self):
        if self.current_worker_count <= self.max_workers_num:
            proc = mp.Process(target=main)
            # proc.daemon = True
            proc.start()

            worker_id = self.svr.svr_ip + "_" + str(proc.pid)
            self.workers_group[worker_id] = proc

    def kill_worker(self, proc):
        """
        Kill process if process is active and pop it from worker group.
        """
        worker_id = self.svr.svr_ip + "_" + str(proc.pid)
        if proc.is_alive():
            proc.terminate()

        self.clear_process_metrics(proc.pid)
        self.workers_group.pop(worker_id)

    def cleanup_zombies(self):

        procs = list(self.workers_group.values())
        logger.debug("All process id is %s" % [proc.pid for proc in procs])

        for proc in procs:
            worker_id = "{}_{}".format(self.svr.svr_ip, proc.pid)
            if not proc.is_alive():
                logger.warning("Worker {} exited unexpectedly.".format(worker_id))
                self.kill_worker(proc)
                return

            # Accumulated count and focus on whether up to NO_HEARTTIME_KILL_DELAY_TIME point
            svr_status, _ = self.worker_storage.get_status_conclusion_by_cache(worker_id)

            if svr_status == SvrStatusChoice.INACTIVE:
                self.kill_worker(proc)
                logger.info("Kill zombie process {}".format(worker_id))

    @property
    def current_worker_count(self):
        return len(self.workers_group)

    def clear_metrics(self):
        """
        Clean up when process exit.
        """
        for proc in self.workers_group.values():
            self.clear_process_metrics(proc.pid)

    @staticmethod
    def clear_process_metrics(pid):
        multiprocess.mark_process_dead(pid)
