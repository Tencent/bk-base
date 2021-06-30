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
from datetime import datetime

import attr
import cattr
from common.base_crypt import BaseCrypt
from django.conf import settings

from meta.utils.rabbitmq_util import ExchangeType, RabbitMQProducer


@attr.s
class EventMessage(object):
    """
    事件消息实例
    """

    msg_id = attr.ib(type=str)  # 事件唯一id
    event_type = attr.ib(type=str)  # 事件类型
    event_level = attr.ib(type=str)  # 事件等级
    triggered_by = attr.ib(type=str)  # 事件触发者/系统
    triggered_at = attr.ib(type=str)  # 事件触发时间
    created_at = attr.ib(type=str, default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # 事件记录时间
    refer = attr.ib(type=str, default="metadata")  # 事件来源模块名
    generate_type = attr.ib(type=str, default="meta_hook")  # 事件来源类型 第三方上报 or 元数据抓取
    expire = attr.ib(type=int, default=-1)  # 事件过期时间(秒数,从triggered_at开始算)
    description = attr.ib(type=str, default="")  # 事件描述
    event_message = attr.ib(type=str, default="{}")  # json 事件消息详情


class EventManager(object):
    def __init__(self, event_params):
        self.event = EventMessage(**event_params)

    def gen_event_message(self):
        return json.dumps(cattr.unstructure(self.event))

    def publish_event(self):
        producer = RabbitMQProducer(
            conn_addr=settings.RABBITMQ_ADDR,
            exchange_name=settings.META_EVENT_SYSTEM_NAME,
            exchange_type=ExchangeType.TOPIC.value,
            routing_key="{}.{}".format("meta.event", self.event.event_type),
        )
        event_message = self.gen_event_message()
        producer.publish(event_message)

    @classmethod
    def get_encrypted_addr(cls):
        crypt = BaseCrypt(instance_key=settings.CRYPT_INSTANCE_KEY)
        return crypt.encrypt(settings.RABBITMQ_ADDR)
