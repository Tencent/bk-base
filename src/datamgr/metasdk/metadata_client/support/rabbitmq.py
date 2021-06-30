# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
"""
rabbitmq 中间件类
"""

import enum

import requests
from kombu import Connection, Consumer, Exchange, Queue

from metadata_client.exc import EventSubscribeConfigError
from metadata_client.support.base_crypt import BaseCrypt


class ExchangeType(enum.Enum):
    DIRECT = "direct"  # 1:1绑定
    TOPIC = "topic"  # N:1绑定
    FANOUT = "fanout"  # 1:N绑定
    HEADERS = "headers"


class RabbitMQConsumer(object):
    def __init__(
        self, backend, queue_mapping=None, exchange_name="", exchange_type=ExchangeType.TOPIC.value, callbacks=None
    ):
        self.queue_mapping = queue_mapping if queue_mapping else dict()
        self.exchange_type = exchange_type
        self.exchange = Exchange(str(exchange_name), type=str(exchange_type))
        self.listening_queues = self._get_listening_queues()
        self.callbacks = callbacks if callbacks else []
        backend.get_backend_config()
        backend.decrypt_addr()
        self.conn = Connection(backend.addr)

    def release(self):
        """
        释放连接

        :return:
        """

        self.conn.release()

    def _get_listening_queues(self):
        """
        获取监听的队列列表

        :return: list 根据配置声明的队列列表
        """

        listening_queues = []
        if not self.queue_mapping or not isinstance(self.queue_mapping, dict):
            raise
        for queue_name, queue_config in self.queue_mapping.items():
            listening_queues.append(
                Queue(name=str(queue_name), routing_key=str(queue_config["routing_key"]), exchange=self.exchange)
            )
        return listening_queues

    def start_scan(self, timeout=1):
        """
        开始消耗

        :return: None
        """
        with Consumer(self.conn, queues=self.listening_queues, callbacks=self.callbacks):
            self.conn.drain_events(timeout=timeout)


class BackendClient(object):
    """
    事件系统队列后端Client
    """

    def __init__(self, supports_config):
        """
        :param supports_config: 事件系统相关配置参数的获取支持
        """
        self.supports_config = supports_config
        self.method = "GET"
        self.backend_config = None
        self.addr = None

    def get_backend_config(self):
        """
        获取队列后端配置参数

        :return: dict
        """
        resp = requests.get(url=self.supports_config["supporter"])
        try:
            resp.raise_for_status()
            response_result = resp.json()
        except Exception as he:
            message = "初始化RABBITMQ错误: {}".format(he)
            raise EventSubscribeConfigError(message_kv={"detail": message})
        self.backend_config = response_result["data"]

    def decrypt_addr(self):
        """
        解码队列后端地址

        :return:
        """
        instance_key = self.supports_config["crypt_key"]
        crypt = BaseCrypt()
        crypt.set_instance_key(instance_key)
        decrypt_txt = crypt.decrypt(self.backend_config["addr"])
        self.addr = decrypt_txt.decode("utf-8")
