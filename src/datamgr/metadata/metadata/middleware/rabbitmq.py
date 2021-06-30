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
"""
rabbitmq 中间件类
"""


import enum

from kombu import Connection, Consumer, Exchange, Producer, Queue


class ExchangeType(enum.Enum):
    DIRECT = 'direct'  # 1:1绑定
    TOPIC = 'topic'  # N:1绑定
    FANOUT = 'fanout'  # 1:N绑定
    HEADERS = 'headers'


class RabbitMQProducer(object):
    def __init__(
        self, conn_addr, queue_names=None, exchange_name='', routing_key='', exchange_type=ExchangeType.DIRECT.value
    ):
        self.queue_names = queue_names if queue_names else []
        self.exchange_type = exchange_type
        self.exchange = Exchange(name=str(exchange_name), type=str(exchange_type))
        self.routing_key = str(routing_key)
        self.bound_queues = self._bind_queues()
        self.conn = Connection(conn_addr)

    def release(self):
        self.conn.release()

    def _bind_queues(self):
        bound_queues = []
        if self.queue_names and not isinstance(self.queue_names, list):
            self.queue_names = [self.queue_names]
        for name in self.queue_names:
            bound_queues.append(Queue(name=str(name), exchange=self.exchange, routing_key=str(self.routing_key)))
        return bound_queues

    def publish(self, body):
        """
        发布消息到队列

        :param body: dict/list 消息信息
        :return: boolean
        """

        with self.conn.channel() as channel:
            with Producer(channel) as producer:
                producer.publish(
                    body,
                    routing_key=self.routing_key,
                    exchange=self.exchange,
                    declare=self.bound_queues,
                    retry=True,
                    retry_policy={
                        'interval_start': 0,  # First retry immediately,
                        'interval_step': 2,  # then increase by 2s for every retry.
                        'interval_max': 5,  # but don't exceed 5s between retries.
                        'max_retries': 5,  # give up after 5 tries.
                    },
                )


class RabbitMQConsumer(object):
    def __init__(self, conn_addr, queue_names=None, routing_key='', exchange_type=ExchangeType.DIRECT.value):
        self.queue_names = queue_names if queue_names else []
        self.exchange_type = exchange_type
        self.exchange = Exchange(str('metadata'), type=str(exchange_type))
        self.routing_key = routing_key
        self.listening_queues = self._get_listening_queues()
        self.callbacks = []
        self.conn = Connection(conn_addr)

    def release(self):
        self.conn.release()

    def _get_listening_queues(self):
        listening_queues = []
        if self.queue_names and not isinstance(self.queue_names, list):
            self.queue_names = [self.queue_names]
        for name in self.queue_names:
            listening_queues.append(Queue(name=str(name), routing_key=str(self.routing_key), exchange=self.exchange))
        return listening_queues

    def register_callback(self, callback):
        """
        订阅consumer的回到函数

        :param callback: function 回到函数
        :return: None
        """
        self.callbacks.append(callback)

    def start_scan(self):
        """
        开始消耗

        :return: None
        """
        with Consumer(self.conn, queues=self.listening_queues, callbacks=self.callbacks):
            self.conn.drain_events()
