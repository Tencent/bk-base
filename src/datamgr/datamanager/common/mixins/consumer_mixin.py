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
import enum

from common.kafka import clusters as kafka_clusters


class ConsumerType(enum.Enum):
    KAFKA = "kafka"
    PULSAR = "pulsar"


class ConsumerMixin(object):
    def __init__(self, *args, **kwargs):
        super(ConsumerMixin, self).__init__(*args, **kwargs)

    def init_consumer(self, consumer_configs):
        self._consumer_configs = consumer_configs or {}
        self._consumer_type = self._consumer_configs.pop("type")
        self._topic = self._consumer_configs.pop("topic")
        self._partition = self._consumer_configs.pop("partition", None)
        self._alias = self._consumer_configs.pop("alias", "op")

        self._consumer = self.get_consumer()
        self._consume_count = 0
        self._handle_error_count = 0

    def get_consumer(self):
        """根据消费者类型获取"""
        if self._consumer_type == ConsumerType.KAFKA.value:
            return kafka_clusters.get_confluent_consumer(
                self._topic,
                self._partition,
                self._alias,
                configs=self._consumer_configs,
            )
        elif self._consumer_type == ConsumerType.PULSAR.value:
            # TODO
            raise NotImplementedError()

    def collect_data(self, num_messages, timeout=5):
        """消费数据

        :param num_messages: 消息数量
        :param timeout: 超时时间
        """
        if self._consumer_type == ConsumerType.KAFKA.value:
            self.collect_data(num_messages, timeout)
        elif self._consumer_type == ConsumerType.PULSAR.value:
            # TODO
            raise NotImplementedError()

    def collect_kafka_data(self, num_messages=5000, timeout=0.2):
        """从kafka中消费数据

        :param num_messages: 消息数量
        :param timeout: 超时时间
        """
        num_messages = self._consumer_configs.get(
            "batch_message_max_count", num_messages
        )
        timeout = self._consumer_configs.get("batch_message_timeout", timeout)

        data = self._consumer.consume(num_messages, timeout)
        self._consume_count += len(data)
        return data
