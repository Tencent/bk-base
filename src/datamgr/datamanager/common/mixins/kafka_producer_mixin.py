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

import gevent

from common.kafka import clusters


class KafkaProducerMixin(object):
    """用于把任务过程中的产生的埋点数据写到kafka"""

    def __init__(self, *args, **kwargs):
        super(KafkaProducerMixin, self).__init__(*args, **kwargs)

        self._kafka_producer = clusters.get_confluent_producer("op")
        self._produce_count = 0
        self._produce_error_count = 0

    def produce_metric(self, topic, message, partition=None, callback=None, timeout=10):
        """生产埋点指标

        :param topic 目标数据的Topic
        :param message 数据内容
        :param partition 生产数据目标分区
        :param callback 数据写成功后的回调函数
        :param timeout 超时时间
        """
        callback = callback or clusters.confluent_produce_callback
        max_timeout = timeout
        while True:
            try:
                self._kafka_producer.poll(0)
                if partition is not None:
                    self._kafka_producer.produce(
                        topic, message, callback=callback, partition=partition
                    )
                else:
                    self._kafka_producer.produce(
                        topic,
                        message,
                        callback=callback,
                    )
                self._produce_count += 1
                return True
            except BufferError:
                max_timeout -= 1
                gevent.sleep(0.1)

            if max_timeout < 0:
                logging.error(
                    "Produce message error, buffer too long: {}, message: {}".format(
                        len(self._kafka_producer),
                        message,
                    )
                )
                self._produce_error_count += 1
                return False

        self._produce_count += 1
        return True
