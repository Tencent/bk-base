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
import logging

from pykafka import SimpleConsumer
from pykafka.exceptions import SocketDisconnectedError

from dataquality.settings import DEFAULT_GROUP_ID


class DataSetConsumer(object):
    """数据集消费者，主要负责从数据平台的数据集中消费来自kafka的数据"""

    def __init__(self, data_set_id, cluster, topic):
        logging.info("Init consumer for data set: %s" % data_set_id)
        self.data_set_id = data_set_id
        self.topic = topic
        self.cluster = cluster
        self.status = "init"

        self.reconnect()

    def reconnect(self, retry=3):
        """构建SimpleConsumer实例，实例化后会与kafka建立长连接，不断从kafka中拉取数据到当前实例的缓存队列"""
        while True:
            try:
                self.partition_consumers = {}
                for partition_id, partition in self.topic.partitions.items():
                    self.partition_consumers[partition_id] = SimpleConsumer(
                        topic=self.topic,
                        cluster=self.cluster,
                        partitions=[partition],
                        consumer_group=DEFAULT_GROUP_ID,
                        consumer_timeout_ms=10000,
                        fetch_message_max_bytes=10 * 1024 * 1024,
                        queued_max_messages=10,
                        auto_start=True,
                    )
                self.status = "ready"
                break
            except SocketDisconnectedError as e:
                retry -= 1
                if retry == 0:
                    self.status = "disconnected"
                    raise Exception(
                        "Create consumer for data_set(%s) error: %s"
                        % (self.data_set_id, e)
                    )

    def consume(self, partition_id, block=True):
        """消费指定partition的数据

        :param partition_id 分区ID
        :param block 是否阻塞消费
        """
        if partition_id not in self.partition_consumers:
            raise Exception(
                "The topic %s has no partition with id %s"
                % (self.topic.name.partition_id)
            )
        consumer = self.partition_consumers[partition_id]
        return consumer.consume(block=block)

    def flush(self, partition_id):
        """清空指定分区缓存的数据，每次消费完执行，减小等待过程中内存的消耗

        :param partition_id 分区ID
        """
        if partition_id not in self.partition_consumers:
            raise Exception(
                "The topic %s has no partition with id %s"
                % (self.topic.name.partition_id)
            )
        consumer = self.partition_consumers[partition_id]
        for partition in consumer._partitions.values():
            partition.flush()

    def start(self, partition_id):
        """启动拉取数据的后台进程

        :param partition_id 分区ID
        """
        if partition_id not in self.partition_consumers:
            raise Exception(
                "The topic %s has no partition with id %s"
                % (self.topic.name.partition_id)
            )
        consumer = self.partition_consumers[partition_id]
        consumer._running = True
        consumer._fetch_workers = consumer._setup_fetch_workers()
        consumer._raise_worker_exceptions()

    def reset_offsets(self, partition_id, *args, **kwargs):
        """重置消费者的offset

        :param partition_id 分区ID
        """
        if partition_id not in self.partition_consumers:
            raise Exception(
                "The topic %s has no partition with id %s"
                % (self.topic.name.partition_id)
            )
        consumer = self.partition_consumers[partition_id]
        consumer.reset_offsets(*args, **kwargs)
