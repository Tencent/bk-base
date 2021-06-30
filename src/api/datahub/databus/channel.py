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

import base64
import io
import random
import time

import fastavro
from common.base_crypt import BaseCrypt
from common.exceptions import DataNotFoundError
from common.log import logger
from datahub.access.api import DataRouteApi
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.common.const import (
    ADMIN,
    BK_BIZ_ID,
    BK_BIZ_ID_BKDATA,
    BK_BIZ_NAME,
    BK_BIZ_NAME_BKDATA,
    BK_BKDATA_NAME,
    CLUSTER_DOMAIN,
    CLUSTER_KAFKA_DEFAULT_PORT,
    CLUSTER_NAME,
    CLUSTER_PORT,
    CLUSTER_TYPE,
    CONDITION,
    CONNECTION,
    EMPTY_STRING,
    IP,
    KAFKA,
    LABEL,
    METADATA,
    NAME,
    OPERATION,
    OPERATOR_NAME,
    PLAT_NAME,
    PORT,
    PULSAR,
    QUEUE,
    REPORT_MODE,
    SASL__PASS,
    SASL__USER,
    SASL_MECHANISMS,
    SASL_PASSWD,
    SASL_USERNAME,
    SECURITY_PROTOCOL,
    SPECIFICATION,
    STORAGE_ADDRESS,
    STREAM_TO,
    STREAM_TO_ID,
    TOKEN,
    USE__SASL,
    VALUE,
    ZK_DOMAIN,
)
from datahub.databus.exceptions import (
    CreateKafkaTopicError,
    KafkaTopicOffsetNotExistsError,
)
from datahub.databus.models import DatabusChannel
from datahub.databus.settings import (
    CHANNEL_VERSION,
    CONSUMER_TIMEOUT,
    DATA_ROUTE_NAME_TEMPLATE,
    SASL_PLAINTEXT,
    SCRAM_SHA_512,
    TYPE_KAFKA,
)
from datahub.databus.task.pulsar import channel as pulsar_channel
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition

from datahub.databus import model_manager
from datahub.databus import tag as meta_tag

MAX_FETCH_SIZE_BYTES = 64 * 1024 * 1024


def get_channel_by_name(channel_name):
    try:
        # 参数为整型时，为channel的id，通过id进行查询，否则根据channel_name进行查询
        channel_id = int(channel_name)
        obj = model_manager.get_channel_by_id(channel_id)
    except ValueError:
        obj = model_manager.get_channel_by_name(channel_name)

    if not obj:
        raise DataNotFoundError(
            message_kv={
                "table": "databus_channel",
                "column": "id/name",
                "value": channel_name,
            }
        )
    return obj


def get_channel_by_storage_name(channel_storage_name):
    """
    通过channel在存储中的集群名来查询
    :param channel_storage_name:
    :return:
    """
    obj = DatabusChannel.objects.filter(storage_name=channel_storage_name)

    if not obj or len(obj) <= 0:
        raise DataNotFoundError(
            message_kv={
                "table": "databus_channel",
                "column": "name",
                "value": channel_storage_name,
            }
        )
    return obj[0]


def get_channel_by_stream_to_id(stream_to_id):
    """
    channel在存储中的stream_to_id来查询
    :param stream_to_id:
    :return:
    """
    obj = DatabusChannel.objects.filter(stream_to_id=stream_to_id)
    if not obj or len(obj) <= 0:
        obj = DatabusChannel.objects.filter(id=stream_to_id)

    if not obj or len(obj) <= 0:
        raise DataNotFoundError(
            message_kv={
                "table": "databus_channel",
                "column": "name",
                "value": stream_to_id,
            }
        )

    return obj[0]


def filter_clusters_by_geog_area(channels, geog_area):
    ret = None
    if geog_area:
        # query from meta by tags
        channel_id_list = meta_tag.get_channels_by_tags([geog_area])
        channel_id_set = set(channel_id_list)
        for channel in channels:
            if channel.id in channel_id_set:
                ret = channel
                break
    else:
        if channels:
            ret = channels[0]

    if not ret:
        logger.warning("failed to find a inner channel in databus_channel table!")
    return ret


def get_inner_channel_to_use(project_id=None, bk_biz_id=None, geog_area=None, cluster_type=TYPE_KAFKA):
    """
    :param project_id: 项目id
    :param bk_biz_id:  业务id
    :param geog_area: 区域
    :param cluster_type: databus 队列类型（TYPE_KAFKA、TYPE_PULSAR）
    :return: 优先根据project_id，bk_biz_id选择集群然后根据channel类型过滤出中间层kafka集群，按照priority排序，将priority最高的一条记录返回
    """
    project_route_config = model_manager.get_cluster_route_config(
        dimension_name=project_id, dimension_type="project", cluster_type="inner"
    )
    if project_route_config:
        obj = model_manager.get_inner_channel_by_name(project_route_config.cluster_name)
        return filter_clusters_by_geog_area([obj], geog_area)

    biz_route_config = model_manager.get_cluster_route_config(
        dimension_name=bk_biz_id, dimension_type="biz", cluster_type="inner"
    )
    if biz_route_config:
        obj = model_manager.get_inner_channel_by_name(biz_route_config.cluster_name)
        return filter_clusters_by_geog_area([obj], geog_area)

    channels = model_manager.get_inner_channel_by_priority(cluster_type)
    return filter_clusters_by_geog_area(channels, geog_area)


def get_config_channel_to_use():
    """
    根据channel类型过滤出配置kafka集群，按照priority排序，将priority最高的一条记录返回
    :return:
    """
    objs = DatabusChannel.objects.filter(cluster_role="config").filter(cluster_type="kafka").order_by("-priority")
    if objs:
        return objs[0]
    else:
        logger.warning("failed to find a config channel in databus_channel table!")
        return None


def get_kafka_queues():
    """
    获取channel中队列服务的kafka的列表
    :return: channel中用于队列服务的kafka的列表
    """
    objs = DatabusChannel.objects.filter(cluster_role="queue").filter(cluster_type="kafka").order_by("-priority")
    result = []
    for obj in objs:
        result.append(model_to_dict(obj))

    return result


def inner_kafka_tail(kafka_bs, topic, partition, limit):
    """
    获取kafka inner中指定topic的最近几条数据记录，转换为可读的格式显示
    :param kafka_bs: kafka集群地址
    :param topic: topic名称
    :param partition: 分区编号
    :param limit: 返回数据记录数量限制
    :return: topic中最新的几条数据记录的内容
    """
    result = []
    consumer = _get_kafka_consumer(kafka_bs)
    if partition >= 0:
        # 找到合适的offset，然后开始消费kafka中的数据
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        consumer.seek_to_end()
        tail = consumer.position(tp)
        # 多取一条消息，因为最后一条消息可能是总线事件消息，且每条消息中包含多条数据记录
        offset = tail - 2
        messages = _get_kafka_message(consumer, tp, offset, 2)
        # 按照倒序处理消息内容，最新的放在前面
        for message in reversed(messages):
            # 跳过可能存在的总线事件消息
            if message.key and "DatabusEvent:" in message.key:
                continue

            # kafka中存储的内容为utf8格式的字符串，需先解码，然后转为ISO-8859-1格式的字节流，用于avro解析
            logger.debug(
                "decoding avro message in %s-%s-%s, msg key is %s"
                % (message.topic, message.partition, message.offset, message.key)
            )
            for item in _decode_avro_record(message.value):
                # 拆包数据，逐条解析放入
                for entry in item["_value_"]:
                    result.append(entry)
                    if len(result) == limit:
                        return result

    return result


def inner_pulsar_tail(channel_name, topic, partition, limit):
    """
    获取kafka inner中指定topic的最近几条数据记录，转换为可读的格式显示
    :param channel_name: pulsar集群名称
    :param topic: topic名称
    :param partition: 分区编号
    :param limit: 返回数据记录数量限制
    :return: topic中最新的几条数据记录的内容
    """
    result = []

    if partition >= 0:
        storage_channel = model_manager.get_channel_by_name(channel_name)
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
        messages = pulsar_channel.tail(-1, rest_server, proxy_server, topic, partition, token, 2)
        # 按照倒序处理消息内容，最新的放在前面
        for message in messages:
            # 跳过可能存在的总线事件消息
            if len(message[VALUE]) == 0:
                continue

            # kafka中存储的内容为utf8格式的字符串，需先解码，然后转为ISO-8859-1格式的字节流，用于avro解析
            logger.debug("decoding avro message in {}-{},".format(topic, partition))
            for item in _decode_avro_record(message[VALUE]):
                # 拆包数据，逐条解析放入
                for entry in item["_value_"]:
                    result.append(entry)
                    if len(result) == limit:
                        return result

    return result


def inner_kafka_message(kafka_bs, topic, partition, offset, msg_count=1):
    """
    获取中间层kafka中topic的指定partition、offset上的消息内容
    :param kafka_bs: kafka集群地址
    :param topic: kafka topic名称
    :param partition: 分区编号
    :param offset: 消息的offset
    :param msg_count: 从offset开始，获取的消息数量
    :return: kafka上指定的消息内容
    """
    result = []
    consumer = _get_kafka_consumer(kafka_bs)
    # 校验partition是否存在，offset是否在越界
    if partition >= 0:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        if _validate_offset(consumer, tp, offset):
            # 获取指定数量的kafka消息
            messages = _get_kafka_message(consumer, tp, offset, msg_count)
            for message in reversed(messages):
                # 逐条avro记录放入返回结果中
                for record in _decode_avro_record(message.value):
                    result.append(
                        {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "key": message.key,
                            "value": record,
                        }
                    )

    return result


def outer_kafka_tail(kafka_bs, topic, partition, limit):
    """
    获取接入层kafka中某个topic-partition上最新的几条消息内容
    :param kafka_bs: kafka集群地址
    :param topic: topic名称
    :param partition: 分区编号
    :param limit: 返回消息条数限制
    :return: topic中最新的几条消息内容
    """
    result = []
    consumer = _get_kafka_consumer(kafka_bs)
    if partition >= 0:
        # 找到合适的offset，然后开始消费kafka中的数据
        tp = TopicPartition(topic, partition)

        consumer.assign([tp])
        consumer.seek_to_end()
        tail = consumer.position(tp)
        # 多取几条消息，因为其中可能包含总线事件消息
        offset = tail - limit - 10
        messages = _get_kafka_message(consumer, tp, offset, limit + 10)
        # 按照倒序处理消息内容，最新的放在前面
        for message in reversed(messages):
            if message.key and "DatabusEvent:" in message.key:
                continue

            result.append(
                {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key,
                    "value": _decode_raw_msg(message.value),
                    "base64_data": base64.encodestring(message.value),
                }
            )
            if len(result) == limit:
                break

    return result


def outer_pulsar_tail(channel_name, topic, partition, limit):
    """
    获取pulsar inner中指定topic的最近几条数据记录
    :param channel_name: pulsar集群名称
    :param topic: topic名称
    :param partition: 分区编号
    :param limit: 返回数据记录数量限制
    :return: topic中最新的几条数据记录的内容
    """
    result = []

    if partition >= 0:
        storage_channel = model_manager.get_channel_by_name(channel_name)
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
        result = pulsar_channel.tail(-1, rest_server, proxy_server, topic, partition, token, limit)
    return result


def outer_kafka_message(kafka_bs, topic, partition, offset, msg_count):
    """
    获取接入层kafka中topic的指定partition、offset上的消息内容
    :param kafka_bs: kafka集群地址
    :param topic: kafka topic名称
    :param partition: 分区编号
    :param offset: 消息的offset
    :param msg_count: 从offset开始，获取的消息数量
    :return: kafka上指定的消息内容
    """
    result = []
    consumer = _get_kafka_consumer(kafka_bs)
    # 校验partition是否存在，offset是否在越界
    if partition >= 0:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        if _validate_offset(consumer, tp, offset):
            # 获取指定数量的kafka消息
            messages = _get_kafka_message(consumer, tp, offset, msg_count)
            for message in reversed(messages):
                result.append(
                    {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key,
                        "value": _decode_raw_msg(message.value),
                        "base64_data": base64.encodestring(message.value),
                    }
                )

    return result


def get_topic_partition_num(kafka_bs, topic):
    """
    通过kafka bootstrap获取指定的kafka上某个topic的partition数量，当topic不存在时，会尝试创建topic
    :param kafka_bs: kafka集群地址
    :param topic: topic名称
    :return: topic的partition数量
    """
    consumer = _get_kafka_consumer(kafka_bs)
    tps = _get_topic_partitions(consumer, topic)
    if not tps:
        # 需要创建一下topic
        create_kafka_topic(kafka_bs, topic)
        tps = _get_topic_partitions(consumer, topic)
        if not tps:
            raise CreateKafkaTopicError(message_kv={"topic": topic, "kafka_bs": kafka_bs})

    return len(tps)


def get_topic_offsets(kafka_bs, topic):
    """
    获取指定kafka的topic上partitions和offsets信息
    :param kafka_bs: kafka集群地址
    :param topic: topic名称
    :return: partitions和offsets信息
    """
    result = {}
    consumer = _get_kafka_consumer(kafka_bs)
    tps = _get_topic_partitions(consumer, topic)
    if not tps:
        # 需要创建一下topic
        create_kafka_topic(kafka_bs, topic)
        tps = _get_topic_partitions(consumer, topic)
        if not tps:
            raise CreateKafkaTopicError(message_kv={"topic": topic, "kafka_bs": kafka_bs})

    start_offsets = consumer.beginning_offsets(tps)
    end_offsets = consumer.end_offsets(tps)
    for tp in tps:
        # end_offset是下一个要创建的kafka日志的offset，我们这里需将其减一，代表当前的最新一条日志的offset
        result[tp.partition] = {
            "head": start_offsets[tp],
            "tail": end_offsets[tp] - 1,
            "length": end_offsets[tp] - start_offsets[tp],
        }

    return result


def get_topic_offsets_with_group(kafka_bs, topic, group_id):
    """
    获取指定group id的kafka的topic上partitions和offsets信息
    :param group_id: 消费者所属group
    :param kafka_bs: kafka集群地址
    :param topic: topic名称
    :return: partitions和offsets信息
    """
    result = {}
    consumer = _get_kafka_consumer(kafka_bs, group_id)
    tps = _get_topic_partitions(consumer, topic)
    for tp in tps:
        topic_partition = "{}-{}".format(tp.topic, tp.partition)
        result[topic_partition] = str(consumer.committed(tp))

    return result


def get_msg_count_in_passed_ms(kafka_bs, topic, passed_ms):
    """
    获取topic在最近指定的时间段内总共的消息数量
    :param kafka_bs: kafka集群的地址
    :param topic: kafka topic的名称
    :param passed_ms: 过去的时间数量，单位毫秒
    :return:
    """
    result = 0
    consumer = _get_kafka_consumer(kafka_bs)
    tps = _get_topic_partitions(consumer, topic)
    ts = int(time.time() * 1000) - passed_ms
    if tps:
        try:
            end_offsets = consumer.end_offsets(tps)
            timestamps = {tp: ts for tp in tps}
            passed_offsets = consumer.offsets_for_times(timestamps)  # 将时间转换为毫秒
            logger.info(
                "topic %s current end offsets: %s, passed %s(ms) offsets: %s"
                % (topic, end_offsets, passed_ms, passed_offsets)
            )
            for tp in tps:
                if passed_offsets[tp]:
                    result += end_offsets[tp] - passed_offsets[tp].offset
        except Exception as e:
            logger.exception(
                e,
                "failed to get kafka msg count for {} in last {}(ms)".format(topic, passed_ms),
            )
    else:
        logger.warning("topic {} not exists on kafka {}!".format(topic, kafka_bs))

    return result


def create_kafka_topic(kafka_bs, topic):
    """
    创建kafka的topic
    :param kafka_bs: kafka集群地址
    :param topic: kafka的topic名称
    :return: True/False，是否创建成功
    """
    # 发送一条初始化消息到目标kafka中，会默认创建topic。调用此方法前，需确保topic不存在。
    _send_message_to_kafka(kafka_bs, topic, [{"key": "", "value": ""}])


def create_multi_partition_kafka_topic(kafka_bs, topic, target_partition_nums):
    """
    创建多个分区的kafka topic, 若已经创建则检查分区数是否对应，不对应时进行扩建
    :param kafka_bs: kafka_bs
    :param topic: topic
    """
    partiton_nums = get_topic_partition_num(kafka_bs, topic)
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_bs)
    if not partiton_nums or partiton_nums <= 0:
        # 如果topic不存在, 创建分区
        admin_client.create_topics(topic)

    if partiton_nums < target_partition_nums:
        # 如果分区存在,并小于指定分区扩分区
        admin_client.create_partitions(topic_partitions={topic: target_partition_nums})


def delete_kafka_topic(channel_id, topic):
    """
    通过命令行工具删除kafka上指定的topic
    :param channel_id: kafka channel id
    :param topic: kafka topic名称
    :return: True/False，是否删除成功
    """
    kafka_channel = model_manager.get_channel_by_id(channel_id)
    kafka_bs = "{}:{}".format(kafka_channel.cluster_domain, kafka_channel.cluster_port)
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_bs)
    admin_client.delete_topics([topic])


def _get_topic_partitions(consumer, topic):
    """
    获取topic下所有的topic-partition列表，当topic不存在时，返回空数组
    :param consumer: kafka consumer对象
    :param topic: topic的名称
    :return: topic/partition的list
    """
    tps = []
    if topic in consumer.topics():
        partitions = consumer.partitions_for_topic(topic)
        for par in partitions:
            tps.append(TopicPartition(topic, par))
    else:
        logger.warning("failed to find topic partition info for %s, return empty result!" % topic)

    return tps


def _send_message_to_kafka(kafka_bs, topic, msgs):
    """
    发送消息到指定的kafka上的topic中（默认为key/value都为空字符串的消息）。当topic不存在时，会自动创建topic
    :param kafka_bs:  kafka集群的地址
    :param topic: topic的名称
    :param msgs: 要发送的消息列表，包含key/value两个字段
    :return: 无
    """
    producer = KafkaProducer(client_id="api_init_topic", bootstrap_servers=[kafka_bs], acks="all")
    for msg in msgs:
        producer.send(topic, msg["value"], msg["key"])
    logger.info("sending msgs {} to {} on {}!".format(msgs, topic, kafka_bs))
    producer.close(3)


def _get_kafka_consumer(kafka_bs, group_id=None):
    """
    根据kafka集群地址返回consumer对象，设置timeout、fetch_bytes等参数
    :param kafka_bs: kafka集群地址
    :param group_id: 消费者所属group
    :return: kafka consumer对象
    """
    client_id = "api{}-{}".format(random.randint(0, 100), random.randint(0, 1000))
    kafka_domain = kafka_bs.split(":")[0]
    version = CHANNEL_VERSION[kafka_domain] if kafka_domain in CHANNEL_VERSION else CHANNEL_VERSION["default"]

    return KafkaConsumer(
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=[kafka_bs],
        auto_offset_reset="earliest",
        api_version=version,
        max_in_flight_requests_per_connection=15,
        consumer_timeout_ms=15000,
        fetch_min_bytes=1024,
        fetch_max_wait_ms=300,
        max_poll_records=100,
        max_partition_fetch_bytes=5242880,
        session_timeout_ms=25000,
        request_timeout_ms=26000,
    )


def _validate_topic_partition(consumer, topic, partition):
    """
    验证在kafka集群上topic partition是否存在
    :param consumer: kafka consumer对象
    :param topic: topic的名称
    :param partition: partition编号
    :return: 验证通过返回True，否则抛出异常
    """
    partitions = consumer.partitions_for_topic(topic)
    if not partitions or partition not in partitions:
        logger.error("Topic {} has partitions {}, not including partition {}!".format(topic, partitions, partition))
        return False

    return True


def _validate_offset(consumer, tp, offset):
    """
    验证在kafka集群上topic partition是否存在
    :param consumer: kafka consumer对象
    :param tp: topic-partition对象
    :param offset: 消息的offset
    :return: 验证通过返回True，否则抛出异常
    """
    head = consumer.beginning_offsets([tp])[tp]
    tail = consumer.end_offsets([tp])[tp]
    if offset >= tail or offset < head:
        logger.error(
            "Topic-partition {} offset range is [{}, {}], offset {} is out of range!".format(tp, head, tail, offset)
        )
        raise KafkaTopicOffsetNotExistsError(message_kv={"tp": "{}[{},{}]".format(tp, head, tail), "offset": offset})

    return True


def _get_kafka_message(consumer, tp, offset, limit=20):
    """
    获取kafka中从offset开始的数条消息内容
    :param consumer: kafka consumer对象
    :param topic: topic名称
    :param partition: 分区编号
    :param offset: 消息的offset
    :param limit: 最多返回消息数量
    :return: kafka 消息
    """
    logger.debug("reading from kafka tp {}, starting offset {}, and message count {}".format(tp, offset, limit))
    # 检查offset的范围，是否在目前partition中存在
    if offset < 0:
        offset = 0
    consumer.seek(tp, offset)
    # 获取kafka中的消息内容
    messages = consumer.poll(CONSUMER_TIMEOUT, limit)
    result = []
    if messages and tp in messages:
        result = messages[tp]
    else:
        logger.warning("failed to consumer records from {}-{}, return empty result!".format(tp, offset))
    consumer.close()

    return result


def _decode_avro_record(value):
    """
    将avro格式的文件解析为avro记录并返回
    :param value: avro格式文件的字节流
    :return: avro记录对象
    """
    if value:
        utf8_msg_val = value.decode("utf8")
        raw_file = io.BytesIO(utf8_msg_val.encode("ISO-8859-1"))
        raw_records = fastavro.reader(raw_file)
        return raw_records
    else:
        logger.warning("failed to decode avro string!")
        return []


def _decode_raw_msg(value):
    """
    首先尝试用utf8编码解码，失败后尝试用gbk解码
    :param value: 接入层kafka的消息内容
    :return: 解码后的字符串
    """
    try:
        return value.decode("utf8")
    except Exception:
        try:
            return value.decode("gbk")
        except Exception:
            # 两种编码decode都失败时，返回空值
            logger.warning("decode failed, Not utf8 nor gbk")
            return None


def register_channel_to_gse(params, username):
    stream_to_params = {
        METADATA: {
            LABEL: {BK_BIZ_ID: BK_BIZ_ID_BKDATA, BK_BIZ_NAME: BK_BIZ_NAME_BKDATA},
            PLAT_NAME: BK_BKDATA_NAME,
        },
        OPERATION: {OPERATOR_NAME: username},
        STREAM_TO: {
            NAME: DATA_ROUTE_NAME_TEMPLATE % (params.get(CLUSTER_TYPE, KAFKA), params[CLUSTER_NAME]),
            REPORT_MODE: params.get(CLUSTER_TYPE, KAFKA),
        },
    }
    if KAFKA == params.get(CLUSTER_TYPE, KAFKA):
        stream_to_params[STREAM_TO][KAFKA] = {
            STORAGE_ADDRESS: [
                {
                    IP: params[CLUSTER_DOMAIN],
                    PORT: params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT),
                }
            ]
        }
        # 为需要鉴权的kafka设置鉴权信息
        set_kafka_auth_info(params.get("storage_name", None), stream_to_params[STREAM_TO][KAFKA])

    elif PULSAR == params.get(CLUSTER_TYPE, KAFKA):
        stream_to_params[STREAM_TO][PULSAR] = {
            STORAGE_ADDRESS: [
                {
                    IP: params[CLUSTER_DOMAIN],
                    PORT: params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT),
                }
            ]
        }
        token = params.get(ZK_DOMAIN, None)
        if token and token != EMPTY_STRING:
            stream_to_params[STREAM_TO][PULSAR][TOKEN] = params.get(ZK_DOMAIN)

    ret = DataRouteApi.config_add_streamto(stream_to_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed, params:{}, ret:{}".format(stream_to_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )
    stream_to_id = ret.data[STREAM_TO_ID]
    return stream_to_id


def update_channel_to_gse(params, username, stream_to_id):
    stream_to_update_params = {
        CONDITION: {STREAM_TO_ID: stream_to_id, PLAT_NAME: BK_BKDATA_NAME},
        OPERATION: {OPERATOR_NAME: username},
        SPECIFICATION: {
            STREAM_TO: {
                NAME: DATA_ROUTE_NAME_TEMPLATE % (params.get(CLUSTER_TYPE, KAFKA), params[CLUSTER_NAME]),
                REPORT_MODE: params.get(CLUSTER_TYPE, KAFKA),
            }
        },
    }

    if KAFKA == params.get(CLUSTER_TYPE, KAFKA):
        stream_to_update_params[SPECIFICATION][STREAM_TO][KAFKA] = {
            STORAGE_ADDRESS: [
                {
                    IP: params[CLUSTER_DOMAIN],
                    PORT: params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT),
                }
            ]
        }
        # 为需要鉴权的kafka设置鉴权信息
        set_kafka_auth_info(
            params.get("storage_name", None),
            stream_to_update_params[SPECIFICATION][STREAM_TO][KAFKA],
        )

    elif PULSAR == params.get(CLUSTER_TYPE, KAFKA):
        stream_to_update_params[SPECIFICATION][STREAM_TO][PULSAR] = {
            STORAGE_ADDRESS: [
                {
                    IP: params[CLUSTER_DOMAIN],
                    PORT: params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT),
                }
            ]
        }
        token = params.get(ZK_DOMAIN, None)
        if token and token != EMPTY_STRING:
            stream_to_update_params[SPECIFICATION][STREAM_TO][PULSAR][TOKEN] = params.get(ZK_DOMAIN)

    ret = DataRouteApi.config_update_streamto(stream_to_update_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed, params: {}, ret: {}".format(stream_to_update_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )


def get_operator_username(username):
    """
    若接口未开启鉴权，从get_request_username可能会获取不到username或者为空，需要提供默认的username，避免gse接口报错
    """
    if username and username != EMPTY_STRING:
        return username
    else:
        return ADMIN


def check_channel_auth_info(storage_name):
    """
    :param storage_name: storekit中存储集群名
    :return: need_auth(Boolean), connection_info集群具体信息
    """
    from datahub.databus.rt import get_storage_cluster

    queue_cluster = get_storage_cluster(QUEUE, storage_name)
    connection = queue_cluster[CONNECTION]
    if connection.get(USE__SASL, False):
        connection[SASL__PASS] = BaseCrypt.bk_crypt().decrypt(connection[SASL__PASS])
        return True, connection
    else:
        return False, connection


def set_kafka_auth_info(storage_name, kafka):
    """
    查询storekit，判断该集群是否需要鉴权，并设定鉴权信息
    """
    if storage_name:
        need_auth, connection_info = check_channel_auth_info(storage_name)
        if need_auth:
            kafka[SASL_USERNAME] = connection_info[SASL__USER]
            kafka[SASL_PASSWD] = connection_info[SASL__PASS]
            kafka[SASL_MECHANISMS] = SCRAM_SHA_512
            kafka[SECURITY_PROTOCOL] = SASL_PLAINTEXT
    return kafka
