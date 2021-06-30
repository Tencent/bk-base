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
from bkbase.dataflow.metrics.reporter.reporter import Reporter
from bkbase.dataflow.metrics.util.constants import *
from kafka import KafkaProducer


class _KafkaReporter(Reporter):
    def __init__(self):
        super(_KafkaReporter, self).__init__()
        self._producer = None
        self._kafka_host = None
        self._topic = None

    # 设置地址和topic
    def set_producer(self, kafka_host, topic):
        self._kafka_host = kafka_host
        self._topic = topic
        # 设置生产者
        self._producer = KafkaProducer(
            bootstrap_servers=[self._kafka_host],
            acks=ACK,
            retries=RETRIES,
            max_in_flight_requests_per_connection=MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            batch_size=BATCH_SIZE,
            linger_ms=LINGER_MS,
            buffer_memory=BUFFER_MEMORY,
            max_request_size=MAX_REQUEST_SIZE,
        )

    # 生产者上报
    def report_now(self, registry_map=None):
        for key in list(registry_map.keys()):
            registry = registry_map[key]
            metrics = registry.dump_metrics()
            self._producer.send(
                self._topic,
                # key='my_key',
                # kafka 在线上不转化为流的形式无法上报
                value=metrics.encode("utf-8"),
            )
            # 判断是否上报结束, 若结束, 从 map 中移除
            if registry.is_end():
                self.remove(key)


# 单例对象
kafka_reporter = _KafkaReporter()
