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
from abc import ABCMeta, abstractmethod

from collection.common.constants import CollectMethod, EventType
from collection.common.message import QueueMessage
from utils.rawdata_producer import RawDataProducer


class BaseCollector(metaclass=ABCMeta):
    """
    数据采集基类
    """

    @abstractmethod
    def report(self):
        """
        主动上报数据
        """
        pass


class BKDRawDataCollector(BaseCollector):
    """
    数据平台数据源上报机制
    """

    def __init__(self, config: dict, init_producer=True):
        self.config = config
        self.bk_biz_id = config["bk_biz_id"]
        self.raw_data_name = config["raw_data_name"]

        self.producer = None
        if init_producer:
            self.init_producer(self.bk_biz_id, self.raw_data_name)

    def init_producer(self, bk_biz_id, raw_data_name):
        self.producer = RawDataProducer(bk_biz_id, raw_data_name)
        if self.producer is None:
            raise Exception("No producer for {} ...".format(raw_data_name))

    def produce_message(
        self,
        message,
        collect_time=None,
        event_type=EventType.UPDATE.value,
        collect_method=CollectMethod.BATCH.value,
    ):
        """
        仅传入 event content 的 message
        """
        if collect_time is None:
            collect_time = int(time.time() * 1000)

        qm = QueueMessage(
            event_content=message,
            collect_time=collect_time,
            event_type=event_type,
            collect_method=collect_method,
        )
        self.producer.produce_message(qm.to_message())
