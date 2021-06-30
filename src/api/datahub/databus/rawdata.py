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

from common.log import logger
from datahub.access.collectors.factory import CollectorFactory
from datahub.common.const import OK
from datahub.databus.channel import create_multi_partition_kafka_topic
from datahub.databus.exceptions import AlterKafkaPartitionsError, AlterKafkaTopicError
from datahub.databus.models import DatabusClean
from datahub.databus.task import task_utils
from datahub.databus.task.pulsar import channel as pulsar_channel
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.pulsar.topic import get_pulsar_channel_token

from datahub.databus import channel, cmd_helper, model_manager, settings, tag


def get_daily_record_count(raw_data_id):
    """
    获取rawdata最近24小时的数据量
    :param raw_data_id: rawdata id
    :return: 最近24小时rawdata对应的kafka topic中消息数量
    """
    result = 0
    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    if rawdata:
        cluster_type = model_manager.get_channel_by_id(rawdata.storage_channel_id).cluster_type
        if cluster_type == settings.TYPE_PULSAR:
            # pulsar 暂时不支持获取数据量
            return 0

        kafka_bs = _get_rawdata_kafka_bootstrap_server(rawdata)
        # 最近24小时kafka中的消息数量
        result = channel.get_msg_count_in_passed_ms(kafka_bs, get_topic_name(rawdata), 24 * 3600 * 1000)

    return result


def rawdata_tail(raw_data_id, partition, limit):
    """
    获取rawdata最近的几条记录
    :param raw_data_id: rawdata表的id
    :param partition: 分区编号
    :param limit: 最大返回记录条数
    :return: rawdata对应的kafka topicc上最近的记录内容
    """
    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    if not rawdata:
        return []

    storage_channel = model_manager.get_channel_by_id(rawdata.storage_channel_id)

    if storage_channel.cluster_type == settings.TYPE_PULSAR:
        # pulsar 使用 zk_port 作为proxy的端口; zk_domain 存储 pulsar的outer写入token
        rest_server = "{}:{}".format(
            storage_channel.cluster_domain,
            storage_channel.cluster_port,
        )
        proxy_server = "pulsar://{}:{}".format(
            storage_channel.cluster_domain,
            storage_channel.zk_port,
        )
        token = storage_channel.zk_domain
        return pulsar_channel.tail(
            raw_data_id,
            rest_server,
            proxy_server,
            get_topic_name(rawdata),
            partition,
            token,
            limit,
        )
    else:
        server = "{}:{}".format(
            storage_channel.cluster_domain,
            storage_channel.cluster_port,
        )
        return channel.outer_kafka_tail(server, get_topic_name(rawdata), partition, limit)


def rawdata_kafka_message(raw_data_id, partition, offset, msg_count=1):
    """
    获取rawdata对应的kafka的topic上指定分区和offset的消息内容
    :param raw_data_id: rawdata表的id
    :param partition: 分区编号
    :param offset: 消息的offset
    :param msg_count: 拉取的消息数量
    :return: rawdata对应的kafka topic上的消息内容
    """
    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    if rawdata:
        kafka_bs = _get_rawdata_kafka_bootstrap_server(rawdata)
        if kafka_bs:
            return channel.outer_kafka_message(kafka_bs, get_topic_name(rawdata), partition, offset, msg_count)
    else:
        return []


def rawdata_kafka_offsets(raw_data_id):
    """
    获取rawdata对应的kafka topic上每个分区里消息数量和offset信息
    :param raw_data_id: rawdata表的id
    :return: rawdata对应的kafka topic的offset信息
    """
    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    if rawdata:
        kafka_bs = _get_rawdata_kafka_bootstrap_server(rawdata)
        if kafka_bs:
            return channel.get_topic_offsets(kafka_bs, get_topic_name(rawdata))
    else:
        return {}


def get_partitions(raw_data_id):
    """
    获取rt_id对应的kafka topic的partition数量
    @:param rt_id: result_table表的id
    :return: kafka上rt_id对应的topic的分区数量
    """
    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    if rawdata:
        kafka_bs = _get_rawdata_kafka_bootstrap_server(rawdata)
        if kafka_bs:
            return channel.get_topic_partition_num(kafka_bs, get_topic_name(rawdata))

    # 默认返回0，代表rt对于的kafka topic不存在
    return 0


def set_topic(raw_data_id, r_size, r_hours):

    rawdata = model_manager.get_raw_data_by_id(raw_data_id)
    databus_channel = model_manager.get_channel_by_id(rawdata.storage_channel_id)
    zk_addr = "{}:{}{}".format(
        databus_channel.zk_domain,
        databus_channel.zk_port,
        databus_channel.zk_root_path,
    )
    topic = get_topic_name(rawdata)

    succ, output = cmd_helper.get_cmd_response(
        [
            "KAFKA_CONFIG_SCRIPT",
            "--zookeeper",
            zk_addr,
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--add-config",
            "retention.ms={},retention.bytes={}".format(r_hours * 3600 * 1000, r_size * 1024 * 1024 * 1024),
        ]
    )

    if succ:
        result = "ok"
        return result
    else:
        logger.warning("failed to alter topic {}: {}".format(topic, output))
        raise AlterKafkaTopicError(message_kv={"topic": topic})


def set_partitions(raw_data_id, partitions):
    """
    扩容rawdata对应的kafka接入层中topic的分区数量
    :param raw_data_id: rawdata的id
    :param partitions: 分区数量
    :return: 命令执行的输出
    """
    raw_data = model_manager.get_raw_data_by_id(raw_data_id)
    channel_info = model_manager.get_channel_by_id(raw_data.storage_channel_id)
    topic = get_topic_name(raw_data)
    channel_host = "{}:{}".format(
        channel_info.cluster_domain,
        channel_info.cluster_port,
    )
    if channel_info.cluster_type == settings.TYPE_PULSAR:
        pulsar_topic.increment_partitions(channel_host, topic, partitions, get_pulsar_channel_token(channel_info))
    else:
        create_multi_partition_kafka_topic(channel_host, topic, partitions)

    result = {"topic": topic, "partitions": partitions, "output": "SUCCESS"}
    # 获取相关的清洗rt列表，列出提醒用户
    objs = DatabusClean.objects.filter(raw_data_id=raw_data_id).values()
    if objs:
        rt_ids = [obj["processing_id"] for obj in objs]
        result["warning"] = "should check kafka partitions for clean rt_ids {} based on rawdata {}".format(
            ",".join(rt_ids),
            raw_data_id,
        )

        # 调用access同步分区信息的方法
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(raw_data.data_scenario)
        raw_data.storage_partitions = partitions
        collector.update_route_info(
            raw_data_id,
            raw_data,
            task_utils.get_channel_info_by_id(raw_data.storage_channel_id),
        )
        return {"result": OK}
    else:
        logger.warning("failed to alter topic {} partitions to {}. {}".format(topic, partitions, "failed"))
        raise AlterKafkaPartitionsError(message_kv={"topic": topic})


def bad_msg(raw_data_id, limit=10):
    """
    获取在清洗过程中处理失败的数据的采样，这类数据存储在kafka op中指定的topic上
    """
    geog_area = tag.get_tag_by_raw_data_id(raw_data_id)
    kafka_bs = settings.KAFKA_OP_HOST[geog_area]
    topic = "{}_{}".format(settings.CLEAN_BAD_MSG_TOPIC_PREFIX, raw_data_id)
    messages = channel.outer_kafka_tail(kafka_bs, topic, 0, limit)
    result = []
    for message in messages:
        str_val = message["value"]
        try:
            json_val = json.loads(str_val)
            record = {
                "raw_data_id": json_val["dataId"],
                "result_table_id": json_val["rtId"],
                "msg_key": json_val["msgKey"],
                "msg_value": json_val["msgValue"],
                "error": json_val["error"],
                "partition": json_val["partition"],
                "offset": json_val["offset"],
                "timestamp": json_val["timestamp"],
            }
            result.append(record)
        except Exception as e:
            logger.exception(e, "failed to decode json value from %s" % str_val)

    return result


def _get_rawdata_kafka_bootstrap_server(rawdata):
    """
    获取rawdata对应的kafka集群的地址
    :param raw_data_id: rawdata表的id
    :return: rawdata对应的kafka集群的地址，也可能是None
    """
    databus_channel = model_manager.get_channel_by_id(rawdata.storage_channel_id)
    if databus_channel:
        return "{}:{}".format(databus_channel.cluster_domain, databus_channel.cluster_port)
    else:
        logger.warning("rawdata {} kafka channel id {} is not exists!".format(rawdata.id, rawdata.storage_channel_id))
        # 没找到对应的数据时，默认返回None
        return None


def get_topic_name(rawdata):
    """
    获取rawdata对应的kafka的topic名称
    :param rawdata:  rawdata对象
    :return: rawdata对象对应的kafka topic名称
    """
    return rawdata.topic_name
