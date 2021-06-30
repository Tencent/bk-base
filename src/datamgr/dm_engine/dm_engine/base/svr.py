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

from __future__ import absolute_import, unicode_literals

import logging
import os
import threading
import time
from abc import ABCMeta
from multiprocessing import cpu_count
from threading import BoundedSemaphore
from weakref import WeakValueDictionary

from dm_engine.base.common import get_ip
from dm_engine.base.component import BaseComponentService
from dm_engine.config import settings
from dm_engine.utils.log import configure_logging

logger = logging.getLogger(__name__)


class SharedNamespace(object):
    """
    Shared namespace for communication for HOSTSVR and ComponentSVR
    """

    def __init__(self):
        self._sem = BoundedSemaphore(1)
        self._namespace = {}

    def set(self, key, value):
        self._namespace[key] = value

    def get(self, key):
        return self._namespace[key]

    def atomic(self):
        return Atomic(self._sem)


class Atomic(object):
    def __init__(self, sem):
        self._sem = sem

    def __enter__(self):
        self._sem.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()


class BaseHostSvr(object):
    """
    Host service processing standard works, such as long-time running, status reportor, etc.
    Another core work is keeping component services running normally.
    """

    __metaclass__ = ABCMeta

    NAME = "default"

    def __init__(self):
        # SVR shared namespace, create & update operation must use SharedNamespace method.
        self.namespace = SharedNamespace()

        # [Declaration From Subclass] Logger Configuration
        self.log_config = None

        # [Declaration From Subclass] Component Services
        self.services = []
        self.services_threads_group = WeakValueDictionary()

        # Machine infomation
        self.svr_id = ""
        self.svr_ip = ""
        self.svr_cpu_count = 1
        self.svr_pid = ""

        # Running event represent that the service is avtive
        self.running = threading.Event()

    def setup(self):
        configure_logging(
            log_level=settings.LOG_LEVEL,
            log_module=self.NAME,
            log_dir=settings.LOG_DIR,
            report_sentry=settings.REPORT_SENTRY,
            sentry_DSN=settings.SENTRY_DSN,
        )

    def start(self):
        """
        Entry function
        """
        try:
            self.setup()
            self.running.set()

            # Gain server ip & process id, and generate svr_id (service identifier)
            self.svr_ip = get_ip()
            self.svr_pid = os.getpid()
            self.svr_cpu_count = cpu_count()
            self.svr_id = "{}_{}".format(self.svr_ip, self.svr_pid)

            self.setup_services()

            logger.info("Start main circulation of host service.")
            while self.running.is_set():
                logger.debug("Heartbeat from host servie...")
                self.handle()
                self.guard_services()
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def handle(self):
        """
        Main execution code in the circulation of host service. This method can
        be overrided according to different scene.
        """
        pass

    def setup_services(self):
        """
        Setup component-services, default []. Subclass can add the compnents
        services to self.services attribute by overriding this method.
        """
        self.services = []

    def stop(self):
        """
        Stop process

        Clear running event and waiting for service stop gracefully.
        """
        self.running.clear()

        for thread_obj in self.services_threads_group.values():
            thread_obj.join()

    def guard_services(self):
        """
        Guard component services registed in the host service. Check and restart the component-services every 1s.
        Note component service must be the subclass of BaseComponentService.
        """
        for service_obj in self.services:
            _service_name = service_obj.__class__

            if (_service_name not in self.services_threads_group) or not (
                self.services_threads_group[_service_name].is_alive()
            ):
                if not isinstance(service_obj, BaseComponentService):
                    logger.error("Invalid BaseComponentService.")
                    continue

                _thread_obj = threading.Thread(target=lambda _s: _s.start(), args=(service_obj,))
                self.services_threads_group[_service_name] = _thread_obj

                logger.info("Start to run service {}, the interval is {} sec".format(service_obj, service_obj.interval))
                _thread_obj.start()
