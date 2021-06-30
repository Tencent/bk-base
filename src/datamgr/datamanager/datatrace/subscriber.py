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
from __future__ import absolute_import, print_function, unicode_literals

import logging

from metadata_client import DEFAULT_SETTINGS, MetadataClient

from conf.settings import CRYPT_INSTANCE_KEY, META_API_HOST, META_API_PORT
from datatrace.data_trace_dict import DATA_TRACE_SUBSCRIBE_CONFIG

logger = logging.getLogger(__name__)
META_CLIENT_SETTINGS = DEFAULT_SETTINGS.copy()
META_CLIENT_SETTINGS.update(
    dict(
        META_API_HOST=META_API_HOST,
        META_API_PORT=META_API_PORT,
        CRYPT_INSTANCE_KEY=CRYPT_INSTANCE_KEY,
    )
)


class DataTraceSubscriber:
    """数据足迹订阅者"""

    def __init__(self):
        meta_client = MetadataClient(META_CLIENT_SETTINGS)
        self.subscriber = meta_client.event_subscriber(DATA_TRACE_SUBSCRIBE_CONFIG)

    def subscribe(self):
        """订阅事件

        :return:
        """
        self.subscriber.start_to_listening()

    def set_callback(self, callback):
        self.subscriber.set_callback(callbacks=callback)
