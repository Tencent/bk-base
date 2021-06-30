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
import time
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class BaseComponentService(object):
    """
    Component service perform its duty, follow single responsibility principle. Component service is affiliated to the
    host service, it run hander method periodically by interval=0.1s.
    """

    __metaclass__ = ABCMeta
    interval = 0.1

    def __init__(self, svr):
        """
        @param {dm_engine.base.svr.BaseHostSvr} svr: host service
        """
        self.svr = svr
        self.namespace = self.svr.namespace
        self.running = self.svr.running

    def start(self):
        """
        Component service entry ...
        Process sleep some seconds according to self.interval after call handler method.

        @todo: switch logger to component-logger
        """
        while self.running.is_set():
            try:
                logger.debug("Heartbeat from component service {} ...".format(self.__class__))
                self.handle()
            except Exception as e:
                logger.exception(
                    "Service {} fails to handle once, will sleep {} sec, " "error={}".format(self, self.interval, e)
                )
            time.sleep(self.interval)

        self.stop_after()

    @abstractmethod
    def handle(self):
        """
        Main circle method that must be overrided by subclass
        """
        pass

    def stop_after(self):
        """
        Callback method is registed in service-stop-after.
        """
        pass
