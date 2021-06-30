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

import gevent

from common.kafka import clusters


class KafkaConsumerMixin(object):
    def __init__(self, configs=None):
        super(KafkaConsumerMixin, self).__init__(configs)
        configs = configs or {}

        kafka_config = configs.get("kafka_config", {})
        topic = kafka_config.pop("topic")
        partition = kafka_config.pop("partition", None)
        alias = kafka_config.pop("alias", "op")

        self.consumer = clusters.get_consumer(alias, topic, partition)

    def collect_kafka_data(self, num_messages, timeout=5):
        data = []

        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                break

            message = self.consumer.consume(block=False)
            if message is None:
                gevent.sleep(0.1)
                continue

            data.append(message.value)

            if len(data) >= num_messages:
                break
        return data
