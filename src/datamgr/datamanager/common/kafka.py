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

from confluent_kafka import Consumer, Producer, TopicPartition
from pykafka import Cluster
from pykafka.handlers import GEventHandler
from pykafka.simpleconsumer import SimpleConsumer

from common.exceptions import KafkaAliasNotFoundError
from conf import settings


class KafkaConnectionHandler(object):
    """
    databases is an optional dictionary of database definitions (structured
    like settings.KAFKA_CLUSTERS).
    """

    def __init__(self, clusters):
        self._clusters = clusters
        self._clients = {}

        self._cache = {}

    def __getitem__(self, alias):
        hosts = self.get_hosts(alias)

        client = Cluster(
            hosts=hosts,
            handler=GEventHandler(),
            socket_timeout_ms=1200000,
            offsets_channel_socket_timeout_ms=1200000,
            broker_version="0.10.2",
        )
        self._clients[alias] = client
        return client

    def get_hosts(self, alias):
        if alias in self._clients:
            return self._clients[alias]

        if alias not in self._clusters:
            raise KafkaAliasNotFoundError()

        return self._clusters[alias]

    def get_consumer(self, alias, topic, partition=0, configs=None):
        configs = configs or {}

        client = self.__getitem__(alias)
        topic = client.topics[topic]

        partitions = []
        if isinstance(partition, list):
            for par in partition:
                if par in topic.partitions:
                    partitions.append(topic.partitions[par])
        else:
            if partition in topic.partitions:
                partitions.append(topic.partitions[partition])

        consumer = SimpleConsumer(
            topic=topic, cluster=client, partitions=partitions, **configs
        )
        return consumer

    def get_producer(self, alias, topic):
        client = self.__getitem__(alias)
        topic = client.topics[topic]

        return topic.get_producer()

    def get_confluent_producer(
        self, alias="op", reconnect=False, hosts=None, configs=None
    ):
        cache_key = "%s_confluent_producer" % alias
        if self._cache.get(cache_key, False) is not False and reconnect is False:
            return self._cache[cache_key]

        try:
            configs = configs or {}

            if not hosts:
                hosts = self.get_hosts(alias)

            producer = Producer(
                {
                    "bootstrap.servers": hosts,
                    "queue.buffering.max.messages": configs.get(
                        "max_messages", settings.KAFKA_PRODUCE_MAX_MESSAGES
                    ),
                    "queue.buffering.max.kbytes": configs.get(
                        "max_kbytes", settings.KAFKA_PRODUCE_MAX_BYTES
                    ),
                }
            )
            self._cache[cache_key] = producer
        except Exception as e:
            logging.error("create confluent kafka producer failed: {}".format(e))
            return False

        return self._cache[cache_key]

    def confluent_produce_callback(self, err, msg):
        if err is not None:
            logging.error("Metric delivery failed: {}".format(err))

    def get_confluent_consumer(
        self, topic, partition, alias="op", reconnect=False, hosts=None, configs=None
    ):
        cache_key = "%s_confluent_consumer" % alias
        if self._cache.get(cache_key, False) is not False and reconnect is False:
            return self._cache[cache_key]

        try:
            configs = configs or {}

            if not hosts:
                hosts = self.get_hosts(alias)

            consumer = Consumer(
                {
                    "bootstrap.servers": hosts,
                    "group.id": configs.get("group_id"),
                    "auto.offset.reset": configs.get(
                        "auto_offset_reset", settings.KAFKA_CONSUME_OFFSET_RESET
                    ),
                    "auto.commit.interval.ms": configs.get(
                        "auto_commit_interval_ms",
                        settings.KAFKA_CONSUME_COMMIT_INTERVAL,
                    ),
                    "fetch.max.bytes": settings.KAFKA_CONSUME_MAX_BYTES,
                }
            )

            # 分配消费者消费数据的配置
            topic_partitions = []
            if isinstance(partition, list):
                for p in partition:
                    topic_partitions.append(TopicPartition(topic=topic, partition=p))
            else:
                topic_partitions.append(
                    TopicPartition(topic=topic, partition=partition)
                )
            consumer.assign(topic_partitions)

            self._cache[cache_key] = consumer
        except Exception as e:
            logging.error("create confluent kafka consumer failed: {}".format(e))
            return False

        return self._cache[cache_key]


clusters = KafkaConnectionHandler(settings.KAFKA_CLUSTERS)
