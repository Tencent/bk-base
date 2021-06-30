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
import time

import kafka
from kafka.common import OffsetRequestPayload, TopicPartition
from kafka.errors import KafkaError

from api import databus_api

from dmonitor.settings import GEOG_AREA_CODE, KAFKA_OUTER_ROLE_NAME
from dmonitor.metrics.collectors.base import BaseOffsetCollector


class KafkaOffsetCollector(BaseOffsetCollector):
    DEFAULT_HASH_DIVISOR = 4

    def __init__(self, task):
        super(KafkaOffsetCollector, self).__init__(task)

    def init_clients(self, cur_time):
        """初始化所有kafka客户端

        :param cur_time: 当前时间
        """
        try:
            new_clusters = []
            res = databus_api.channels.list({
                'tags': [GEOG_AREA_CODE, KAFKA_OUTER_ROLE_NAME],
            }, raise_exception=True)
            for item in res.data:
                cluster_name = item.get('cluster_name')
                new_clusters.append(cluster_name)
                kafka_config = ['{host}:{port}'.format(
                    host=item.get('cluster_domain'),
                    port=item.get('cluster_port'),
                )]
                self.init_cluster_client(cluster_name, kafka_config)
            for old_cluster_name in self._clusters:
                if old_cluster_name not in new_clusters:
                    self.clear_cluster(old_cluster_name)
            self._clusters = new_clusters
            self._last_refresh_clusters_time = cur_time
        except Exception as e:
            logging.error('get kafka clusters error: %s' % e)

    def init_cluster_client(self, cluster_name, kafka_config=False):
        """初始化客户端

        :param cluster_name: 集群名称
        :param kafka_config: kafka配置
        """
        if kafka_config is False:
            try:
                res = databus_api.channels.retrieve({
                    'cluster_name': cluster_name,
                }, raise_exception=True)
                kafka_config = ['{host}:{port}'.format(
                    host=res.data.get('cluster_domain'),
                    port=res.data.get('cluster_port'),
                )]
            except Exception as e:
                logging.error('get kafka({cluster_name}) config error: {error}'.format(
                    cluster_name=cluster_name,
                    error=e
                ))

        logging.info('init kafka client %s' % kafka_config)
        self._clients[cluster_name] = self.gen_kafka_client(kafka_config=kafka_config)
        return True

    def gen_kafka_client(self, kafka_config):
        """生成kafka客户端

        :param kafka_config: kafka配置
        """
        return kafka.KafkaClient(kafka_config)

    def collect_cluster(self, cluster_name):
        """获取集群kafka元信息

        :param cluster_name: 集群名称
        """
        logging.info(' == collect kafka cluster [ %s ] == ' % cluster_name)
        result = self.refresh_all_topics(cluster_name)
        if not result:
            return False

        result = self.collect_topics(cluster_name)
        if not result:
            return False
        return True

    def refresh_all_topics(self, cluster_name):
        """刷新当前Topic的元信息

        :param cluster_name: 集群名称
        """
        kafka_client = self._clients.get(cluster_name, False)
        if not kafka_client:
            # 重连一次这个kafka client
            self.init_cluster_client(cluster_name)
            logging.error('connect kafka failed')
            return False

        try:
            kafka_client.load_metadata_for_topics()
            self.update_topics(cluster_name, kafka_client.topic_partitions)
        except Exception as e:
            logging.error('get kafka cluster [ %s ] topic list exception %s' % (cluster_name, e))
            self.init_cluster_client(cluster_name)
            return False
        return True

    def update_topics(self, cluster_name, new_topics):
        """更新Topic信息

        :param cluster_name: 集群名称
        :param new_topics: 新的Topic
        """
        if cluster_name not in self._topics:
            self._topics[cluster_name] = {}
            for topic, topic_info in new_topics.items():
                self._topics[cluster_name][topic] = {
                    'is_new': False,
                    'partitions': topic_info
                }
        else:
            old_topics = self._topics[cluster_name].keys()
            self._topics[cluster_name] = {}
            for topic, topic_info in new_topics.items():
                if topic not in old_topics:
                    self._topics[cluster_name][topic] = {
                        'is_new': True,
                        'partitions': topic_info
                    }
                else:
                    self._topics[cluster_name][topic] = {
                        'is_new': False,
                        'partitions': topic_info
                    }

    def collect_topics(self, cluster_name):
        """获取Topic的Offset信息

        :param clsuter_name: 集群名称
        """
        kafka_client = self._clients.get(cluster_name, False)
        if not kafka_client:
            # 重连一次这个kafka client
            self.init_cluster_client(cluster_name)
            logging.error('connect kafka %s failed' % cluster_name)
            return False

        topics = self._topics.get(cluster_name, False)
        if not topics:
            logging.error('kafka cluster %s has no topics' % cluster_name)
            return False

        offset_requests = []
        succ_cnt = 0
        fail_cnt = 0
        last_exception = False
        start_time = time.time()
        for topic in topics.keys():
            if self._task_topic_hash_value is not None:
                hash_value = self.get_topic_hash(topic)
                if hash_value != self._task_topic_hash_value:
                    continue
            for partition in topics.get(topic, {}).get('partitions', {}).keys():
                offset_requests.append(OffsetRequestPayload(topic, partition, -1, 1))
                if len(offset_requests) > 100:
                    temp_succ_cnt, temp_fail_cnt, temp_exception = self.get_offset_by_offset_requests(
                        kafka_client, cluster_name, offset_requests)
                    succ_cnt += temp_succ_cnt
                    fail_cnt += temp_fail_cnt
                    temp_exception = temp_exception or last_exception
                    offset_requests = []

        if len(offset_requests) > 0:
            temp_succ_cnt, temp_fail_cnt, temp_exception = self.get_offset_by_offset_requests(
                kafka_client, cluster_name, offset_requests)
            succ_cnt += temp_succ_cnt
            fail_cnt += temp_fail_cnt
            temp_exception = temp_exception or last_exception

        end_time = time.time()

        all_cnt = succ_cnt + fail_cnt
        if (all_cnt > 0) and ((fail_cnt * 100 / all_cnt) > 90):
            logging.error('【%s】query kafka exception more than 90%%, last message %s' % (cluster_name, last_exception))
            # 重连一次这个kafka client
            self.init_cluster_client(cluster_name)

        logging.info('【%s】query kafka %s offset cost time %s sec' % (cluster_name, all_cnt, (end_time - start_time)))

    def get_offset_by_offset_requests(self, kafka_client, cluster_name, offset_requests):
        """通过kafka客户端的请求获取当前最新的offset信息

        :param kafka_client: kafka客户端
        :param cluster_name: 集群名称
        :param offset_requests: offset请求
        """
        indexes = {}
        succ_cnt, fail_cnt = 0, 0
        for index, request in enumerate(offset_requests):
            indexes[(request.topic, request.partition)] = index
        try:
            responses = kafka_client.send_offset_request(offset_requests, fail_on_error=False)
            self.handle_offset_response(responses, cluster_name)
            succ_cnt += len(offset_requests)
            return succ_cnt, fail_cnt, None
        except Exception as e:
            tp = e.args[0]
            index = 0
            if isinstance(tp, TopicPartition):
                index = indexes.get((tp.topic, tp.partition), 0)

            part1_offset_requests = offset_requests[:index]
            part2_offset_requests = offset_requests[index + 1:]
            fail_cnt += 1

            if len(part1_offset_requests) > 0:
                part_succ_cnt, part_fail_cnt, exc = self.get_offset_by_offset_requests(
                    kafka_client, cluster_name, part1_offset_requests)
                succ_cnt += part_succ_cnt
                fail_cnt += part_fail_cnt
            if len(part2_offset_requests) > 0:
                part_succ_cnt, part_fail_cnt, exc = self.get_offset_by_offset_requests(
                    kafka_client, cluster_name, part2_offset_requests)
                succ_cnt += part_succ_cnt
                fail_cnt += part_fail_cnt

            return succ_cnt, fail_cnt, e

    def handle_offset_response(self, responses, cluster_name):
        """处理offset请求的返回信息

        :param responses: offset获取请求的返回
        :param cluster_name: 集群名称
        """
        nowtime = time.time()
        # 解析kafka响应
        for resp in responses:
            try:
                if getattr(resp, 'error', None) != 0:
                    continue

                if isinstance(resp, KafkaError):
                    logging.error()

                topic = resp.topic
                partition = int(resp.partition or 0)
                offset = int(resp.offsets[0] or 0)
                unique_key = '{}_{}_{}'.format(cluster_name, topic, partition)

                if topic not in self._topics[cluster_name]:
                    continue

                if unique_key in self._last_offsets:
                    if offset < self._last_offsets[unique_key]:
                        continue

                if (offset is None) or (offset < 0):
                    if unique_key not in self._last_offsets:
                        continue
                    # 如果offset变得更小了，则继续忽略
                    offset = self._last_offsets[unique_key]

                # 同步给计算增量的协程
                self._task._offset_queue.put({
                    'time': nowtime,
                    'offset': offset,
                    'partition': partition,
                    'topic': topic,
                    'setid': cluster_name,
                    'is_new': self._topics[cluster_name][topic]['is_new'],
                })
                self._last_offsets[unique_key] = offset
                self._topics[cluster_name][topic]['is_new'] = False
            except Exception as e:
                logging.error('parse kafka response failed %s' % e)

        return True
