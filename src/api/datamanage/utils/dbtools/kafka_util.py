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
from django.utils.translation import ugettext_lazy as _
from kafka import KafkaConsumer, KafkaProducer, KafkaClient

from common.log import logger
from datamanage.pizza_settings import KAFKA_ADDRS, KAFKA_OP_ROLE_NAME
from datamanage.exceptions import DatamanageErrorCode, ClusterNotExistError
from datamanage.utils.api import DatabusApi

_kafka_cache = {}
_kafka_producer_cache = {}


def get_kafka_client(svr_type='kafka', reconnect=False):
    client = False
    svrs = _get_kafkasvrs_by_type(svr_type)
    if _kafka_cache.get(svr_type, False) and (not reconnect):
        return _kafka_cache.get(svr_type, False)
    try:
        client = KafkaClient(svrs)
        _kafka_cache[svr_type] = client
    except Exception as e:
        logger.error('create kafka client[%s] failed:%s' % (svr_type, e), DatamanageErrorCode.KAFKA_CONN_ERR)
        return False
    return client


def get_kafka_consumer(
    topic,
    group_id,
    svr_type='kafka',
    partition=False,
    reconnect=False,
    fetch_message_max_bytes=1024000,
    auto_offset_reset='largest',
    auto_commit_interval_ms=10000,
):
    svrs = _get_kafkasvrs_by_type(svr_type)
    # 看是不是已经创建了这个consumer
    key = '%s_%s_%s_%s' % (svr_type, topic, group_id, partition)
    if _kafka_cache.get(key, False) and (not reconnect):
        return _kafka_cache.get(key, False)
    # 连接kafka
    try:
        conn_config = {
            "bootstrap_servers": svrs,
            "group_id": group_id,
            "auto_commit_enable": True,
            "auto_commit_interval_ms": auto_commit_interval_ms,
            "auto_offset_reset": auto_offset_reset,
            "max_partition_fetch_bytes": fetch_message_max_bytes,
        }
        consumer = KafkaConsumer(topic, **conn_config)
        if type(partition) in [list, int]:
            consumer.set_topic_partitions({topic: partition})
    except Exception as e:
        logger.error('KAFKA[%s] conn failed: %s' % (key, e), DatamanageErrorCode.KAFKA_CONN_ERR)
        return False
    _kafka_cache[key] = consumer
    return consumer


def reconn_kafka_by_consumer(consumer):
    for key in list(_kafka_cache.keys()):
        if _kafka_cache[key] == consumer:
            del _kafka_cache[key]
            return True
    return False


def reconn_kafka_by_producer(producer):
    for key in list(_kafka_producer_cache.keys()):
        if _kafka_producer_cache[key] == producer:
            del _kafka_producer_cache[key]
            return True
    return False


def get_kafka_producer(svr_type, geog_area=None, reconnect=False):
    # 看是不是已经创建了这个producer
    if geog_area is not None:
        svr_key = '%s_%s' % (svr_type, geog_area)
    else:
        svr_key = svr_type
    if _kafka_producer_cache.get(svr_key, False) and (not reconnect):
        return _kafka_producer_cache.get(svr_key, False)

    svrs = _get_kafkasvrs_by_type(svr_type, geog_area)
    # 连接kafka
    try:
        producer = KafkaProducer(bootstrap_servers=svrs)
    except Exception as e:
        logger.error('KAFKA[%s] connect failed:%s' % (svr_type, e), DatamanageErrorCode.KAFKA_CONN_ERR)
        return False
    _kafka_producer_cache[svr_key] = producer
    return producer


def send_kafka_messages(svr_type, topic, messages, geog_area=None, retry=1):
    producer = get_kafka_producer(svr_type, geog_area)
    try:
        for message in messages:
            producer.send(topic, str(message))
        producer.flush()
    except Exception:
        reconn_kafka_by_producer(producer)
        if retry > 0:
            return send_kafka_messages(svr_type, topic, messages, retry - 1)
    return True


def get_kafka_messages(consumer=False, kafka_config={}):
    messages = False
    try:
        if not consumer:
            svr_type = kafka_config.get('svr_type', 'kafka')
            topic = kafka_config.get('topic', '')
            group_id = kafka_config.get('group_id', '')
            partition = kafka_config.get('partition', False)
            fetch_message_max_bytes = kafka_config.get('fetch_message_max_bytes', 1024000)
            # fetch_max_wait_ms = kafka_config.get('fetch_max_wait_ms', 5000)
            auto_offset_reset = kafka_config.get('auto_offset_reset', 'largest')
            auto_commit_interval_ms = kafka_config.get('auto_commit_interval_ms', 10000)
            consumer = get_kafka_consumer(
                topic,
                group_id,
                svr_type,
                partition,
                False,
                fetch_message_max_bytes,
                auto_offset_reset,
                auto_commit_interval_ms,
            )
            if not consumer:
                return False
        messages = consumer.fetch_messages()
    except Exception as e:
        logger.error('kafka read message failed %s ' % e, DatamanageErrorCode.KAFKA_READ_ERR)
        return False
    return messages


def _get_kafkasvrs_by_type(svr_type='kafka', geog_area=None):
    """
    根据地域，服务类型，查询对应 kafka 集群信息

    :param {string} svr_type
    """
    if geog_area is not None:
        res = DatabusApi.channels.list(
            {
                'tags': [KAFKA_OP_ROLE_NAME, geog_area],
            }
        )
        if res.is_success() and res.data:
            if isinstance(res.data, list):
                channel_config = res.data[0]
            else:
                channel_config = res.data
            return [
                '{host}:{port}'.format(
                    host=channel_config.get('cluster_domain'),
                    port=channel_config.get('cluster_port'),
                )
            ]
        else:
            raise ClusterNotExistError(
                _('地区({geog_area})没有角色为{role}的channel集群').format(
                    geog_area=geog_area,
                    role=KAFKA_OP_ROLE_NAME,
                )
            )

    if svr_type == 'kafka_inner':
        svrs = KAFKA_ADDRS.get('KAFKA_INNER_ADDR', [])
    elif svr_type == 'kafka-common':
        svrs = KAFKA_ADDRS.get('KAFKA_COMMON_ADDR', [])
    elif svr_type == 'kafka-op':
        svrs = KAFKA_ADDRS.get('KAFKA_OP_ADDR', [])
    elif svr_type.startswith('raw:'):
        return [svr_type[4:]]
    else:
        svrs = False
    return svrs
