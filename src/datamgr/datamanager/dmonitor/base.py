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
import json
import logging
import time

import gevent
from gevent import Greenlet, monkey, pool

from common.mixins import ConsumerMixin, KafkaProducerMixin
from dmonitor.mixins import DmonitorMetaCacheMixin


monkey.patch_all()


class BaseDmonitorTaskGreenlet(
    DmonitorMetaCacheMixin, KafkaProducerMixin, ConsumerMixin, Greenlet
):
    OPDATA_INTERVAL = 60

    def __init__(self, *args, **kwargs):
        """初始化数据监控任务配置"""
        super(BaseDmonitorTaskGreenlet, self).__init__(*args, **kwargs)

        self._last_opdata_time = int(time.time())

    def init_task_pool(self, task_pool_size):
        self._task_pool = pool.Pool(task_pool_size)

    def log_opdata(self, now):
        statistics_diff = now - self._last_opdata_time
        if statistics_diff > self.OPDATA_INTERVAL:
            if hasattr(self, "_consume_count"):
                logging.info(
                    (
                        "\nConsume(count): {}, Counsume(speed): {}/min, Consume(error): {}"
                    ).format(
                        self._consume_count,
                        int(self._consume_count * 60.0 / statistics_diff),
                        self._handle_error_count,
                    )
                )
                self._consume_count = 0
                self._handle_error_count = 0

            if hasattr(self, "_produce_count"):
                logging.info(
                    (
                        "\nProduce(count): {}, Produce(speed): {}/min, Produce(error): {}"
                    ).format(
                        self._produce_count,
                        int(self._produce_count * 60.0 / statistics_diff),
                        self._produce_error_count,
                    )
                )
                self._produce_count = 0
                self._produce_error_count = 0

            self._last_opdata_time = now

    def _run(self):
        """从任务数据来源中消费待处理数据，并加入到协程池中进行指标处理和加工，或者生成告警"""
        while True:
            now = time.time()

            self.log_opdata(now)

            self.refresh_metadata_cache(now)

            # 如果没有kafka消费者配置，说明不需要从kafka中读取监控源数据
            if getattr(self, "_consumer_configs", None):
                consumed_data = self.collect_kafka_data()

                if not consumed_data:
                    gevent.sleep(5)
                    continue

                for message in consumed_data:
                    self._task_pool.spawn(self.handle_monitor_message, message, now)

            self.do_monitor(now, self._task_pool)

    def handle_monitor_message(self, message, now=None):
        """处理消费者读出的消息

        :param message: 来源于消费者的消息，可能是kafka或者pulsar的数据格式
        :param now: 当前处理数据的时间
        """
        now = now or int(time.time())
        if message.error() is not None:
            raise Exception("Consumer kafka error: %s" % message.error())

        self._consume_count += 1
        value = message.value().decode("utf-8").strip().strip(chr(0))

        try:
            self.handle_monitor_value(json.loads(value, encoding="utf-8"), now)
        except Exception as e:
            self._handle_error_count += 1
            logging.error(
                "Handle monitor_data failed, value: {}, error: {}".format(value, e)
            )

    def refresh_metadata_cache(self, now):
        """刷新元数据缓存

        :param now: 当前时间
        """
        raise NotImplementedError()

    def handle_monitor_value(self, value, now):
        """处理每条消息的内容

        :param value: 消息内容
        :param now: 当前处理数据的时间
        """
        raise NotImplementedError()

    def do_monitor(self, now, task_pool):
        """执行衍生指标或者监控的逻辑

        :param now: 当前时间戳
        :param task_pool: 任务处理协程池
        """
        pass
