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

import requests
from common.log import logger
from datahub.databus.exceptions import (
    CreateTopicErr,
    DeleteTopicErr,
    IncrementsPartitionsErr,
    RequestErr,
)

from datahub.databus import settings


def _get_pulsar_header(token):
    header = {"Content-Type": "application/json"}
    if token is not None:
        header["Authorization"] = "Bearer %s" % token
    return header


def create_partition_topic_if_not_exist(channel_info, topic, partition, token):
    """
    创建分区topic, 若不存在则创建, 若存在则忽略
    :param channel_info: 集群信息
    :param topic: 名称
    :param partition: 分区数
    :param token: outer集群token
    :return: True/False，是否创建成功
    """
    channel_host = "{}:{}".format(channel_info.cluster_domain, channel_info.cluster_port)
    if not exist_partition_topic(channel_host, topic, token):
        return create_partition_topic(channel_host, topic, partition, token)
    return True


def exist_partition_topic(
    channel_host,
    topic,
    token=None,
    tenant=settings.PULSAR_OUTER_TENANT,
    namespace=settings.PULSAR_OUTER_NAMESPACE,
):
    """
    是否存在partition_topic
    :param namespace: 命名空间
    :param tenant: 租户
    :param channel_host: 集群地址
    :param topic: topic
    :param token: pulsar token
    :return: True/False
    """
    url = settings.PULSAR_PARTITION_TOPIC_URL_TPL % (channel_host, tenant, namespace, topic) + "/partitioned-stats"
    header = _get_pulsar_header(token)
    try:
        res = requests.get(url, headers=header)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}".format(url, e))
        raise RequestErr(message_kv={"msg": str(e)})
    logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
    check_request_auth_state(res.status_code, url)
    return res.status_code != 404


def get_partitions(channel_host, topic, token=None):
    """
    是否存在partition_topic
    :param channel_host: 集群地址
    :param topic: topic
    :param token: pulsar token
    :return: True/False
    """
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    url = settings.PULSAR_PARTITION_TOPIC_URL_TPL % (channel_host, tenant, namespace, topic) + "/partitions"
    try:
        header = _get_pulsar_header(token)
        res = requests.get(url, headers=header)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}, headers={}".format(url, e, str(header)))
        raise RequestErr(message_kv={"msg": str(e)})
    logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
    check_request_auth_state(res.status_code, url)
    # 返回格式示例: {"partitions":3}
    partitions = 1  # 默认至少1个
    try:
        partitions = res.json()["partitions"]
    except Exception as e:
        logger.error("{} can not get partitions, {}".format(topic, e))

    if partitions == 0:  # 非分区topic的partition返回0, 需要至少保持1个分区
        partitions = 1
    return partitions


def increment_partitions(channel_host, topic, partition, token=None):
    """
    是否存在partition_topic
    :param channel_host: 集群地址
    :param topic: topic
    :param partition: 分区数
    :param token out集群token
    :return: True/False
    """
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    url = settings.PULSAR_PARTITION_TOPIC_URL_TPL % (channel_host, tenant, namespace, topic) + "/partitions"
    try:
        headers = _get_pulsar_header(token)
        res = requests.post(url, data=str(partition), headers=headers)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, data={}, error={}".format(url, partition, e))
        raise RequestErr(message_kv={"msg": str(e)})

    logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
    check_request_auth_state(res.status_code, url)
    if res.status_code in [200, 204]:
        logger.info("success create topic {}[{}]".format(topic, partition))
    elif res.status_code == 409:  # Conflict
        # 分区数只能增加
        if "number of partitions must be more than existing" in res.text:
            logger.warning("{} increase partition failed, {}".format(topic, res.text))
        else:
            raise IncrementsPartitionsErr(message_kv={"msg": res.text})
    else:
        raise IncrementsPartitionsErr(message_kv={"msg": res.text})

    return True


def create_partition_topic(channel_host, topic, partition, token=None):
    """
    创建分区topic
    :param channel_host: 集群地址
    :param topic: 名称
    :param partition: 分区数
    :param token: 鉴权token
    :return: True/False，是否创建成功
    """
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    url = settings.PULSAR_PARTITION_TOPIC_URL_TPL % (channel_host, tenant, namespace, topic) + "/partitions"
    try:
        headers = _get_pulsar_header(token)
        res = requests.put(url, data=str(partition), headers=headers)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, data={}, error={}".format(url, partition, e))
        raise RequestErr(message_kv={"msg": str(e)})

    logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
    check_request_auth_state(res.status_code, url)
    if res.status_code in [200, 204]:
        logger.info("success create topic %s[%d]" % (topic, partition))
    elif res.status_code == 409:  # Conflict
        if "This topic already exists" in res.text:
            return True
        elif "Partitioned topic already exists" in res.text:
            logger.warning("%s, Partitioned topic already exists" % topic)
    else:
        logger.error("failed to create topic %s[%d], url=%s" % (topic, partition, url))
        raise CreateTopicErr(message_kv={"msg": res.text})

    return True


def delete_topic(
    channel_host,
    topic,
    token=None,
    tenant=settings.PULSAR_OUTER_TENANT,
    namespace=settings.PULSAR_OUTER_NAMESPACE,
    force=False,
):
    """
    删除topic
    :param namespace: 命名空间
    :param tenant: 租户
    :param channel_host: 集群地址
    :param topic: 名称
    :param force: 强制删除
    :param token: out集群token
    :return: True/False，是否创建成功
    """
    url = settings.PULSAR_PARTITION_TOPIC_URL_TPL % (
        channel_host,
        tenant,
        namespace,
        topic,
    )
    try:
        headers = _get_pulsar_header(token)
        res = requests.delete(url, headers=headers)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}".format(url, e))
        raise RequestErr(message_kv={"msg": str(e)})

    logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
    check_request_auth_state(res.status_code, url)
    if res.status_code in [200, 204, 404]:
        logger.info("success delete topic %s" % topic)
    elif res.status_code == 409:  # Conflict
        if "This topic already exists" in res.text:
            raise DeleteTopicErr(message_kv={"msg": res.text})
        elif "Partitioned topic already exists" in res.text:
            logger.warning("%s, Partitioned topic already exists" % topic)
    else:
        raise DeleteTopicErr(message_kv={"msg": res.text})

    return True


def get_pulsar_topic_name(topic):
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    return "persistent://{}/{}/{}".format(tenant, namespace, topic)


def check_request_auth_state(status_code, url):
    if status_code == 401:
        logger.error("requests failed, url=%s, no auth please check pulsar token" % url)
        raise RequestErr(message_kv={"msg": "requests failed, url=%s, no auth please check pulsar token" % url})


def get_pulsar_channel_token(source_channel):
    """
    针对给定的channel判断是否需要token鉴权，kafka类型或token配置为空，返回PULSAR_OUT_NO_TOKEN
    """
    if (
        source_channel.cluster_type == settings.TYPE_PULSAR
        and source_channel.zk_domain is not None
        and source_channel.zk_domain != ""
    ):
        return source_channel.zk_domain
    else:
        return None


def generate_transport_pulsar_topic(source_rt_id, source_type, sink_rt_id, sink_type):
    """
    构造任务名
    :param sink_type: 目标存储类型
    :param sink_rt_id: 目标rt_id
    :param source_type: 源存储类型
    :param source_rt_id: 源rt_id
    :return: pulsar topic
    """
    return "transport_{}_{}_{}_{}".format(source_rt_id, source_type, sink_rt_id, sink_type)


def get_pulsar_topic_schema(channel_host, topic, token=None):
    """
    获取pulsar topic 的schema信息
    :param channel_host: 集群地址
    :param topic: 名称
    :param token: out集群token
    :return: 最新的schema 信息
    """
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    url = settings.PULSAR_SCHEMA_URL_TPL % (channel_host, tenant, namespace, topic)
    try:
        header = _get_pulsar_header(token)
        res = requests.get(url, headers=header)
        logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
        check_request_auth_state(res.status_code, url)
        if res.status_code == 404:
            return None
        return res.json()
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}, headers={}".format(url, e, str(header)))
        raise RequestErr(message_kv={"msg": str(e)})


def update_pulsar_topic_schema(channel_host, topic, token=None):
    """
    更新pulsar topic 的schema信息
    :param channel_host: 集群地址
    :param topic: 名称
    :param token: out集群token
    """
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    url = settings.PULSAR_SCHEMA_URL_TPL % (channel_host, tenant, namespace, topic)
    try:
        header = _get_pulsar_header(token)
        schema_json = {"type": "STRING", "schema": "", "properties": {}}
        res = requests.post(url, data=json.dumps(schema_json), headers=header)
        logger.info("request url={}, code={}, text={}".format(url, res.status_code, res.text))
        check_request_auth_state(res.status_code, url)
        return True if res.status_code == 200 else False
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}, headers={}".format(url, e, str(header)))
        raise RequestErr(message_kv={"msg": str(e)})
