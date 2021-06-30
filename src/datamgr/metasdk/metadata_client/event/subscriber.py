# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有

from __future__ import absolute_import, print_function, unicode_literals

import inspect
import re
import socket
import time

from metadata_client.exc import EventSubscribeConfigError
from metadata_client.resource import resource_lock
from metadata_client.support.rabbitmq import BackendClient, RabbitMQConsumer


class MetaEventSubscriber(object):

    META_EVENT_PREFIX = "meta.event"

    backend = None
    """
    订阅队列后端实例
    """

    subscriber_existed = False
    """
    订阅器是否已注册
    """

    def __init__(self, subscribe_config=None, supports_config=None):
        self.subscribe_orders = dict()
        self.subscriber = None
        self.callbacks = []
        if subscribe_config and isinstance(subscribe_config, dict):
            self.set_subscribe_config(**subscribe_config)
        if supports_config:
            self.backend = BackendClient(supports_config)

    def gen_subscribe_orders(self, config):
        """
        根据用户输入构建订阅参数，生成订阅单

        :param config: dict 用户输入配置参数
        :return: tuple (order_name, order) 订阅单
        """

        order_name = "{}.{}.{}".format(config["refer"], config["key"], config["name"])
        routing_key = "{}.{}".format(self.META_EVENT_PREFIX, config["key"])
        order = dict(routing_key=routing_key)
        return order_name, order

    @staticmethod
    def _verify_config_item(item):
        """
        校验配置项字符串的合法性

        :param item: 配置项
        :return: boolean
        """

        if re.match("^[_a-zA-Z0-9]+$", str(item)) is None:
            return False
        return True

    def set_subscribe_config(self, name=None, key=None, refer=None, *args, **kwargs):
        """
        设置订阅配置

        :param name: 订阅名称
        :param key: 订阅关键字
        :param refer: 订阅者所属来源
        :param args: 其他参数
        :param kwargs: 其他关键字参数
        :return: None
        """

        config = dict(
            name=name if self._verify_config_item(name) else None,
            key=key if self._verify_config_item(key) else None,
            refer=refer if self._verify_config_item(refer) else None,
        )
        if all(config.values()):
            order_name, order = self.gen_subscribe_orders(config)
            self.subscribe_orders[order_name] = order
        else:
            raise EventSubscribeConfigError(message_kv={"detail": config})

    def set_callback(self, callbacks=None):
        """
        设置回调函数

        :param callbacks: 回调函数列表
        :return: None
        """

        if not isinstance(callbacks, list):
            callbacks = [callbacks]
        valid_callbacks = [callback for callback in callbacks if inspect.isfunction(callback)]
        if valid_callbacks:
            self.callbacks.extend(valid_callbacks)

    def register_subscriber(self):
        """
        注册订阅器，每个进程仅能注册一次

        :return: None
        """

        if self.subscribe_orders and not self.__class__.subscriber_existed:
            with resource_lock:
                if not self.__class__.subscriber_existed:
                    self.__class__.subscriber_existed = True
                    consumer = RabbitMQConsumer(
                        self.backend,
                        queue_mapping=self.subscribe_orders,
                        exchange_name="meta_event_system",
                        callbacks=self.callbacks,
                    )
                    self.subscriber = consumer

    def start_to_listening(self, timeout=1, detection_interval=1):
        """
        开始接收订阅消息

        :param timeout: int 每次探测等待消息到来的超时时间(s)
        :param detection_interval: int 探测间隔(s)
        :return: None
        """
        self.register_subscriber()
        if self.subscriber:
            try:
                while True:
                    try:
                        self.subscriber.start_scan(timeout)
                    except socket.timeout:
                        time.sleep(detection_interval)
            except Exception:
                print("stop listening")
                raise
            finally:
                self.subscriber.release()
