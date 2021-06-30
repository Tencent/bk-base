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
from threading import Event, Lock, Thread

# import six


class Reporter(object):
    def create_thread(self):
        # 设置线程为 self._loop
        self._loop_thread.setDaemon(True)

    def __init__(self):
        # 上报线程
        self._loop_thread = Thread(
            target=self._loop,
            name="reporter thread",
        )
        # 上报内容 key 为生成的 id
        # value 为内容, registry 对象
        self._registry_map = {}
        self._reporting_interval = None
        self._stopped = Event()
        self.create_thread()

    def start(self):
        if self._stopped.is_set():
            return False
        r = str(self._loop_thread)
        if "stopped" in r:
            self.create_thread()
        elif "started" in r:
            return False
        self._loop_thread.start()
        return True

    # 停止上报线程
    def stop(self):
        self._stopped.set()

    # 移除上报线程中的上报内容
    def remove(self, uuid):
        self._registry_map.pop(uuid)

    def _loop(self):
        next_loop_time = time.time()
        while not self._stopped.is_set():
            try:
                self.report_now(self._registry_map)
            except Exception as e:
                raise e
            next_loop_time += self._reporting_interval
            wait = max(0, self._reporting_interval)
            if self._stopped.wait(timeout=wait):
                break

    def report_now(self, registry_map=None):
        """
        后续的来继承和实现该函数,每个子类上报的方式不一样
        :param registry_map:
        :return:
        """
        raise NotImplementedError(self.report_now)

    def set_reporting_interval(self, interval):
        self._reporting_interval = interval

    def set_metric_registry(self, key, registry):
        self._registry_map[key] = registry

    # 设置所有的 registry 结束上报
    def set_all_registry_closed(self):
        with Lock():
            for key in self._registry_map:
                self._registry_map[key].set_end()
        # 上报最后一次
        self.report_now(self._registry_map)
        self.stop()
