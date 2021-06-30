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
import logging
import time

import gevent
from common.mixins.kafka_producer_mixin import KafkaProducerMixin
from dataquality.sample.consumer import DataSetConsumer
from dmonitor.mixins import DmonitorMetaCacheMixin
from dmonitor.settings import DMONITOR_TOPICS
from gevent import Greenlet, monkey, pool
from pykafka import Cluster, Topic
from pykafka.exceptions import LeaderNotAvailable
from pykafka.handlers import GEventHandler
from utils.rawdata_producer import RawDataProducer

monkey.patch_all()


class SamplingConsumeTaskGreenlet(DmonitorMetaCacheMixin, KafkaProducerMixin, Greenlet):
    """用于从数据平台的数据集中采样数据的任务"""

    REFRESH_INTERVAL = 3600
    REFRESH_ERROR_INTERVAL = 180
    INIT_TIMEOUT = 300

    def __init__(self, sampling_task_config):
        """初始化采样任务的配置

        :param sampling_task_config: 采样任务配置
            {
                'consume_count': 100,            # 每次采样数据量
                'consume_interval': 60,          # 每次采样间隔时间
                'consume_hash_values': [0],      # 采样结果表的hash值
                'consume_task_count': 1,         # 采样任务总数量
                'produce_raw_data_name': 'bkdata_sampled_datasets',
                'produce_partition_count': 1,    # 生产数据的分区数量
                'filtered_data_sets': [],        # 需要进行过滤的数据集
                'task_pool_size': 300,           # 采样任务协程池大小
                'heat_score': None,              # 采样数据集热度阈值
                'heat_rate': None,               # 采样数据集热度比例
                'recent_data_time': 3600,        # 采样数据集最近有数据的时间
            }
        """
        super(SamplingConsumeTaskGreenlet, self).__init__()
        self._sampling_task_config = sampling_task_config

        # 任务配置
        self._consume_count = sampling_task_config.get("consume_count", 100)
        self._consume_interval = sampling_task_config.get("consume_interval", 60)
        self._consume_hash_values = sampling_task_config.get("consume_hash_values", [0])
        self._consume_task_count = sampling_task_config.get("consume_task_count", 128)
        self._produce_bk_biz_id = sampling_task_config.get("produce_bk_biz_id")
        self._produce_raw_data_name = sampling_task_config.get("produce_raw_data_name")
        self._produce_partition_count = sampling_task_config.get("produce_partition_count", 1)
        self._filtered_data_sets = sampling_task_config.get("filtered_data_sets", [])
        self._task_pool_size = sampling_task_config.get("task_pool_size", 300)
        self._heat_score = sampling_task_config.get("heat_score", 10)
        self._heat_rate = sampling_task_config.get("heat_rate", 30)
        self._recent_data_time = sampling_task_config.get("recent_data_time", 86400)

        # 缓存配置和状态
        self._data_sets = self.refresh_sampling_data_sets()
        self._kill_flags = {}
        self._consumers_status = {}

        # 任务公共集群配置和消费者实例
        self._task_pool = pool.Pool(self._task_pool_size)
        self._consumer_offsets = {}
        self._consumers = {}
        self._clusters = {}
        self._data_set_clusters = {}
        self._data_set_topics = {}
        self._cluster_topics = {}

        self.init_all()

        logging.info("Init the task for sampling consume from kafka success.")

    def _run(self):
        """按一定周期对采样数据集进行更新，同时按一定周期来对数据进行采样"""
        refresh_time = time.time()
        last_consume_time = time.time()

        while True:
            now = time.time()

            # 定期刷新采样数据集
            if now - refresh_time > self.REFRESH_INTERVAL:
                gevent.spawn(
                    self.refresh_metadata,
                    self._data_sets,
                    self.refresh_sampling_data_sets,
                    update=False,
                )
                refresh_time = now

            # 定期从kafka消费数据
            if now - last_consume_time > self._consume_interval:
                self.reset_statistics()
                self.consume_and_produce(now)
                self.log_statistics()
                last_consume_time = now

    def refresh_sampling_data_sets(self):
        """刷新待采样的数据集列表"""
        data_sets = self.fetch_data_set_infos_from_redis()
        sampling_result_tables = set(
            self.fetch_sampling_result_tables(
                self._heat_score,
                self._heat_rate,
                self._recent_data_time,
            )
        )

        result_tables = {}
        for result_table_id in sampling_result_tables:
            if result_table_id not in data_sets:
                continue

            result_table_info = data_sets[result_table_id]
            # 目前采样来源只支持kafka，因此如果该数据集没有kafka，则不进行采样
            if "kafka" not in result_table_info.get("storages", {}):
                continue

            if result_table_id in self._filtered_data_sets:
                continue

            hash_value = self.get_data_set_hash_value(result_table_id)
            if hash_value % self._consume_task_count not in self._consume_hash_values:
                continue
            result_tables[result_table_id] = result_table_info

        logging.info("Refresh information about %s result tables for sampling" % len(result_tables))
        return result_tables

    def get_data_set_hash_value(self, data_set_id):
        """获取数据集的hash值

        :param data_set_id 数据集ID
        """
        cnt = 0
        for char in data_set_id:
            cnt += ord(char)
        return cnt

    def reset_statistics(self):
        """重置消费者统计值"""
        self._consumers_status = {}

        for data_set_id, consumer in self._consumers.items():
            self._consumers_status[data_set_id] = {}
            for partition_id in consumer.partition_consumers.keys():
                self._consumers_status[data_set_id][partition_id] = "timeout"

    def log_statistics(self):
        """输出消费者统计日志"""
        statistics = {
            "finished": 0,
            "error": 0,
            "timeout": 0,
            "total": 0,
            "skipped_nodata": 0,
            "skipped_noleader": 0,
            "skipped_new": 0,
        }
        for data_set_id in self._consumers_status.keys():
            for partition_id in self._consumers_status[data_set_id].keys():
                status = self._consumers_status[data_set_id][partition_id]
                statistics[status] += 1
                statistics["total"] += 1

        logging.info(
            (
                "\nTask: {}, Total: {}, Finished: {}, Skipped(nodata): {}, Skipped(noleader): {}, "
                "Skipped(new): {}, Timeout: {},  Error: {}"
            ).format(
                self._consume_hash_values,
                statistics["total"],
                statistics["finished"],
                statistics["skipped_nodata"],
                statistics["skipped_noleader"],
                statistics["skipped_new"],
                statistics["timeout"],
                statistics["error"],
            )
        )

    def init_all(self):
        """初始化集群，待消费主题和消费者"""
        self._producer = RawDataProducer(bk_biz_id=self._produce_bk_biz_id, raw_data_name=self._produce_raw_data_name)
        self.init_clusters()
        self.init_topics()
        self.init_consumers()

    def init_clusters(self):
        """初始化公共集群配置

        这里之所以把集群配置抽离出来单独初始化，主要是这些消费者正常初始化时，都会实例化Cluster，而Cluster实例本身非常占用内存
        因此抽离出公共集群配置可以有效减少内存资源消耗
        """
        self.check_metadata(self._data_sets, self.REFRESH_ERROR_INTERVAL, "datasets")

        for data_set_id, data_set_info in self._data_sets.items():
            storage_info = data_set_info["storages"]["kafka"]
            channel_info = storage_info.get("storage_channel", {})
            topic_name = str(storage_info.get("physical_table_name"))

            if not channel_info.get("cluster_domain"):
                continue

            hosts = ",".join(
                [
                    "{}:{}".format(
                        channel_info.get("cluster_domain"),
                        channel_info.get("cluster_port"),
                    )
                ]
            )

            if hosts not in self._cluster_topics:
                self._task_pool.spawn(self.init_cluster, hosts)
                self._cluster_topics[hosts] = []

            self._data_set_topics[data_set_id] = {
                "hosts": hosts,
                "topic_name": topic_name,
                "topic": None,
            }
            self._cluster_topics[hosts].append(str.encode(topic_name))

        self._task_pool.join(self.INIT_TIMEOUT)

        for data_set_id, topic_info in self._data_set_topics.items():
            self._data_set_clusters[data_set_id] = self._clusters[topic_info["hosts"]]

    def init_cluster(self, hosts):
        """根据hosts列表实例化Pykafka集群

        :param hosts kafka集群配置地址
        """
        logging.info("Init cluster: {}".format(hosts))
        cluster = Cluster(
            hosts=hosts,
            handler=GEventHandler(),
            socket_timeout_ms=1200000,
            offsets_channel_socket_timeout_ms=1200000,
            broker_version="0.10.2",
        )
        self._clusters[hosts] = cluster

    def init_topics(self):
        """初始化所有待消费数据集的消费主题"""
        meta_datas = {}
        for hosts, cluster in self._clusters.items():
            self._task_pool.spawn(self.init_topic, hosts, cluster, meta_datas)

        self._task_pool.join(self.INIT_TIMEOUT)

        for hosts, meta_data in meta_datas.items():
            for data_set_id, data_set_cluster in self._data_set_clusters.items():
                topic_name = str.encode(self._data_set_topics[data_set_id]["topic_name"])
                if data_set_cluster is self._clusters[hosts] and topic_name in meta_data.topics:
                    topic_metadata = meta_data.topics[topic_name]

                    if topic_metadata.err == LeaderNotAvailable.ERROR_CODE:
                        logging.error(
                            "Raise LeaderNotAvailable error when get metadata about {} from broker".format(topic_name)
                        )
                        continue

                    if not self._multiple_partitions:
                        del_partitions = []
                        for partition_id in topic_metadata.partitions.keys():
                            if partition_id != 0:
                                del_partitions.append(partition_id)

                        for partition_id in del_partitions:
                            del topic_metadata.partitions[partition_id]

                    self._data_set_topics[data_set_id]["topic"] = Topic(data_set_cluster, topic_metadata)

    def init_topic(self, hosts, cluster, meta_datas):
        """初始化单个消费主题

        :param hosts kafka集群配置地址
        :param cluster 实例化的Pykafka集群
        :param meta_datas 实例化的Pykafka集群的元数据
        """
        topics = self._cluster_topics[hosts]
        meta_data = cluster._get_metadata(topics)
        meta_datas[hosts] = meta_data

    def init_consumers(self):
        """初始化消费者"""
        inited = {}
        for data_set_id, cluster in self._data_set_clusters.items():
            if data_set_id in self._consumers:
                continue

            if data_set_id not in self._data_set_topics:
                continue

            if not self._data_set_topics[data_set_id]["topic"]:
                continue

            try:
                topic = self._data_set_topics[data_set_id]["topic"]
                self._task_pool.spawn(self.init_consumer, data_set_id, cluster, topic)
                inited[data_set_id] = True
            except Exception:
                continue

            if data_set_id not in self._consumer_offsets:
                self._consumer_offsets[data_set_id] = {}
        self._task_pool.join(self.INIT_TIMEOUT)

        for data_set_id, consumer_offset in self._consumer_offsets.items():
            if data_set_id in inited:
                topic = self._data_set_topics[data_set_id]["topic"]
                self._task_pool.spawn(self.init_topic_offsets, data_set_id, topic)
        self._task_pool.join(self.INIT_TIMEOUT)

    def init_consumer(self, data_set_id, cluster, topic):
        """初始化消费者

        :param data_set_id 数据集ID
        :param cluster 集群实例
        :param topic 数据主题
        """
        retry_times = 3
        while retry_times > 0:
            try:
                consumer = DataSetConsumer(data_set_id, cluster, topic)
                self._consumers[data_set_id] = consumer
                self._kill_flags[data_set_id] = None
                break
            except Exception:
                retry_times -= 1

    def init_topic_offsets(self, data_set_id, topic):
        """初始化数据主题的offset

        :param data_set_id 数据集ID
        :param topic 数据主题
        """
        for partition_id, offset_response in topic.latest_available_offsets().items():
            self._consumer_offsets[data_set_id][partition_id] = offset_response.offset[0]

    def consume_and_produce(self, now):
        """采样消费数据并把它们重新写到另一个Topic中

        :param now 当前时间（主要为了传递统一的时间作为输出采样数据的时间）
        """
        start_time = int(time.time())
        for data_set_id, consumer in self._consumers.items():
            if data_set_id not in self._consumer_offsets:
                continue

            logging.info('Consumer\'s status for dataset({}) is "{}"'.format(data_set_id, consumer.status))
            if consumer.status in ("ready", "skipped"):
                self._task_pool.spawn(self.sampling_consume, data_set_id, consumer, now)
            elif consumer.status in ("timeout", "disconnected", "init"):
                self._task_pool.spawn(self.reconnect_consumer, data_set_id, consumer)
        last_timeout = self._consume_interval - (int(time.time()) - start_time)
        self._task_pool.join(timeout=last_timeout)
        for greenlet in self._task_pool.greenlets:
            if len(greenlet.args) < 2 or isinstance(greenlet.args[1], DataSetConsumer):
                continue
            data_set_id, consumer = greenlet.args[0], greenlet.args[1]
            if self._kill_flags[data_set_id] is True:
                logging.error("Kill consumer: %s" % data_set_id)
            consumer.flush()
            consumer.status = "timeout"
            self._kill_flags[data_set_id] = False
            self.produce_metric(
                topic=DMONITOR_TOPICS["data_cleaning"],
                message=json.dumps(
                    {
                        "time": now,
                        "database": "monitor_data_metrics",
                        "dataset_sampling_consume": {
                            "consume_count": 0,
                            "min_offset": 0,
                            "max_offset": 0,
                            "tags": {
                                "data_set_id": data_set_id,
                                "topic": consumer.topic.name,
                                "partition": 0,
                                "kafka": consumer.cluster._seed_hosts,
                                "status": "failed",
                            },
                        },
                    }
                ),
            )
        self._task_pool.kill()

        self._producer.flush()

    def reconnect_consumer(self, data_set_id, consumer):
        """重连僵死的消费者

        :param data_set_id 数据集ID
        :param consumer 消费者
        """
        try:
            logging.info("Start to reconnect consumer for dataset(%s)" % data_set_id)
            consumer.reconnect()
            logging.info("Success to reconnect consumer for dataset(%s)" % data_set_id)
        except Exception as e:
            logging.error("Reconnect consumer({}) error: {}".format(data_set_id, e))

    def sampling_consume(self, data_set_id, consumer, now):
        """采样消费数据集

        :param data_set_id 数据集ID
        :param consumer 消费者
        :param now 当前时间
        """
        try:
            for (
                partition_id,
                offset_response,
            ) in consumer.topic.latest_available_offsets().items():
                max_offset = offset_response.offset[0]

                if data_set_id not in self._consumer_offsets:
                    self._consumers_status[data_set_id][partition_id] = "skipped_noleader"
                    consumer.status = "skipped"
                    continue

                if partition_id not in self._consumer_offsets[data_set_id]:
                    self._consumer_offsets[data_set_id][partition_id] = max_offset
                    self._consumers_status[data_set_id][partition_id] = "skipped_new"
                    consumer.status = "skipped"
                    continue
                min_offset = self._consumer_offsets[data_set_id][partition_id]

                consumed_count = 0
                if max_offset == min_offset:
                    self._consumers_status[data_set_id][partition_id] = "skipped_nodata"
                    consumer.status = "skipped"
                    logging.warning(
                        "The max and the min offset is the same for dataset({}), partition({}), offset: {}".format(
                            data_set_id, partition_id, max_offset
                        )
                    )
                    continue

                if max_offset < min_offset:
                    self._consumers_status[data_set_id][partition_id] = "error"
                    consumer.status = "error"
                    continue

                consumed_count = self.sampling_consume_by_partition(consumer, partition_id, min_offset, max_offset)
                consumer.flush(partition_id)

                self._consumer_offsets[data_set_id][partition_id] = max_offset

                self.produce_metric(
                    topic=DMONITOR_TOPICS["data_cleaning"],
                    message=json.dumps(
                        {
                            "time": now,
                            "database": "monitor_data_metrics",
                            "dataset_sampling_consume": {
                                "consume_count": consumed_count,
                                "min_offset": min_offset,
                                "max_offset": max_offset,
                                "tags": {
                                    "data_set_id": data_set_id,
                                    "topic": consumer.topic.name.decode("utf-8"),
                                    "partition": partition_id,
                                    "kafka": consumer.cluster._seed_hosts,
                                    "status": "succeed",
                                },
                            },
                        }
                    ),
                )
            if self._kill_flags[data_set_id] is False:
                logging.error("Recover consumer: %s" % data_set_id)
            self._kill_flags[data_set_id] = True
        except Exception as e:
            logging.error("Failed to consume data from dataset({}), error: {}".format(data_set_id, e))

    def sampling_consume_by_partition(self, consumer, partition_id, min_offset, max_offset):
        """采样消费某个Topic的指定分区的数据

        :param consumer 消费者
        :param partition_id 分区ID
        :parma min_offset 待消费数据最小offset
        :param max_offset 待消费数据最大offset
        """
        logging.info("Begin starting consume data from %s" % consumer.topic.name)
        topic = consumer.topic
        try:
            logging.info(
                "Before reset offset, data_set: {}, partition: {}, offset(min): {}, offset(max): {}".format(
                    consumer.data_set_id, partition_id, min_offset, max_offset
                )
            )
            consumer.reset_offsets(partition_id, [(topic.partitions[partition_id], min_offset)])
            logging.info("After reset offset, data_set: {}, partition: {}".format(consumer.data_set_id, partition_id))
        except Exception as e:
            logging.error(
                "Reset offsets for dataset({}) in partition({}) error: {}".format(consumer.data_set_id, partition_id, e)
            )
            self._consumers_status[consumer.data_set_id][partition_id] = "error"
            return 0

        consume_target_count = self._consume_count
        gevent.sleep(0.1)

        consumed_count = 0
        try:
            while consumed_count < consume_target_count:
                message = consumer.consume(partition_id=partition_id)

                if message is None:
                    break

                hash_value = self.get_data_set_hash_value(consumer.data_set_id)
                consumed_count += self._producer.produce_avro_message(
                    message=message,
                    data_set_id=consumer.data_set_id,
                    partition=hash_value % self._produce_partition_count,
                )

                if message.offset > max_offset:
                    break
        except Exception as e:
            logging.error(
                "Consume from dataset({}) in partition({}) error: {}".format(consumer.data_set_id, partition_id, e)
            )
            self._consumers_status[consumer.data_set_id][partition_id] = "error"
            return 0

        logging.info(
            "Finish to consume sampling data about topic: {}, consumed {} messages".format(
                consumer.topic.name, consumed_count
            )
        )
        self._consumers_status[consumer.data_set_id][partition_id] = "finished"
        return consumed_count
