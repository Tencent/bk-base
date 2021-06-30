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

# 事件订阅者，消费事件消息
import logging

from metadata.util.common import StrictABCMeta

module_logger = logging.getLogger(__name__)


class SubscriberPrototype(object, metaclass=StrictABCMeta):
    """
    订阅者原型类
    """

    __abstract__ = True

    @staticmethod
    def handle_message(body, message):
        """
        处理抓到的订阅消息

        :param body: dict 消息实体
        :param message: object 消息实例
        :return: None
        """
        message.ack()


class InternalSubscriber(SubscriberPrototype):
    """
    metadata内部消耗事件订阅者基类
    """

    __abstract__ = True

    def __init__(self):
        self.logger = module_logger


class RemoteSubscriber(SubscriberPrototype):
    """
    外部远程事件订阅者基类
    """

    __abstract__ = True

    def __init__(self):
        """
        初始化http库
        """
        pass

    def scan_message(self):
        """
        开始处理消息订阅
        """
        pass

    def handle_message(self, body, message):
        """
        订阅消息回调, 根据订阅数据，调用订阅者提供的回调接口
        :param body:
        :param message:
        :return:
        """
        pass
