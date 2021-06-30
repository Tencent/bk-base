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

from common.exceptions import ApiResultError
from common.log import logger
from datahub.access.utils import kafka_tool
from datahub.common.const import (
    CHANNEL_CLUSTER_NAME,
    CHANNEL_TYPE,
    ID,
    KAFKA,
    PHYSICAL_TABLE_NAME,
    PROCESSING_TYPE,
    PULSAR,
    RESULT_TABLE_ID,
    STORAGE_CHANNEL,
    STORAGES,
)
from datahub.databus.api import MetaApi, StoreKitApi
from datahub.databus.channel import create_multi_partition_kafka_topic
from datahub.databus.exceptions import (
    AlterKafkaTopicError,
    GetRtDataProcessingError,
    GetRtStorageError,
    KafkaStorageNotExistsError,
    MetaResponseError,
    RtKafkaStorageNotExistError,
    StorageClusterNotExistsError,
)
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.pulsar.topic import get_pulsar_channel_token
from datahub.databus.utils import ExtendsHandler

from datahub.databus import (
    channel,
    cmd_helper,
    exceptions,
    model_manager,
    rawdata,
    settings,
    tag,
    zk_helper,
)


def get_databus_rt_info(rt_id):
    """
    获取rt_id相关的配置信息，用于总线模块启动databus task
    """
    rt_info = {}
    # 获取单个result_table信息
    data = get_rt_fields_storages(rt_id)
    if not data:
        return rt_info

    rt_info["rt.id"] = rt_id
    rt_info["rt.type"] = data["processing_type"]
    rt_info["msg.type"] = "avro"  # 默认数据在中间层存储的格式
    rt_info["geog_area"] = data["tags"]["manage"]["geog_area"][0]["code"]

    # 获取rt_id的字段信息
    fields, cols, dims = [], [], []
    for field in data["fields"]:
        fields.append(field["field_name"])
        cols.append("{}={}".format(field["field_name"], field["field_type"]))
        if field["is_dimension"]:
            dims.append(field["field_name"])
    # 将字段列表按照字母序排序
    rt_info["fields"] = fields
    cols.sort()
    rt_info["columns"] = ",".join(cols)
    rt_info["dimensions"] = ",".join(dims)
    rt_info["channel_type"] = settings.TYPE_KAFKA
    rt_info["project_id"] = data["project_id"]
    rt_info["bk_biz_id"] = data["bk_biz_id"]
    rt_info["platform"] = data["platform"]
    rt_info["table_name"] = data["result_table_name"]
    try:
        rt_info["geog_area"] = data["tags"]["manage"]["geog_area"][0]["code"]
    except Exception:
        rt_info["geog_area"] = ""
    rt_info["storages.list"] = data["storages"].keys()

    rt_info = __fill_storage_info(rt_info, data["storages"])
    rt_info = ExtendsHandler.fill_inner_storages_info(rt_info, data["storages"])

    # 获取清洗相关的配置
    if data["processing_type"] == settings.CLEAN_TRT_CODE:  # 清洗rt
        databus_clean = model_manager.get_clean_by_processing_id(rt_id)
        rt_info["etl.id"] = databus_clean.id
        rt_info["data.id"] = databus_clean.raw_data_id
        rt_info["etl.conf"] = databus_clean.json_config
    else:
        rt_info["etl.id"] = ""
        rt_info["data.id"] = ""
        rt_info["etl.conf"] = ""

    return rt_info


def __fill_storage_info(rt_info, storages):
    for storage in storages:
        rt_info[storage] = {}
        if "id" in storages[storage]["storage_cluster"]:
            rt_info[storage]["id"] = storages[storage]["storage_cluster"]["id"]
        if "cluster_name" in storages[storage]["storage_cluster"]:
            rt_info[storage]["cluster_name"] = storages[storage]["storage_cluster"]["cluster_name"]
        if "connection_info" in storages[storage]["storage_cluster"]:
            rt_info[storage]["connection_info"] = storages[storage]["storage_cluster"]["connection_info"]
        if "version" in storages[storage]["storage_cluster"]:
            rt_info[storage]["version"] = storages[storage]["storage_cluster"]["version"]
        if "physical_table_name" in storages[storage]:
            rt_info[storage]["physical_table_name"] = storages[storage]["physical_table_name"]
        if "storage_config" in storages[storage]:
            rt_info[storage]["storage_config"] = storages[storage]["storage_config"]
        if "expires" in storages[storage]:
            rt_info[storage]["expires"] = storages[storage]["expires"]

    if "kafka" in storages:
        rt_info["channel_cluster_index"] = storages["kafka"]["storage_channel"]["id"]
        databus_channel = model_manager.get_channel_by_id(rt_info["channel_cluster_index"])
        # 需要考虑下databus_channel为空的情况
        if databus_channel is None:
            raise exceptions.ChannelNotFound()
        rt_info["bootstrap.servers"] = "{}:{}".format(
            databus_channel.cluster_domain,
            databus_channel.cluster_port,
        )
        rt_info["zookeeper.addr"] = "{}:{}{}".format(
            databus_channel.zk_domain,
            databus_channel.zk_port,
            databus_channel.zk_root_path,
        )
        rt_info["channel_cluster_name"] = databus_channel.cluster_name
        rt_info["channel_type"] = settings.TYPE_KAFKA

    if "pulsar" in storages:
        rt_info["channel_cluster_index"] = storages["pulsar"]["storage_channel"]["id"]
        databus_channel = model_manager.get_channel_by_id(rt_info["channel_cluster_index"])
        # 需要考虑下databus_channel为空的情况
        if databus_channel is None:
            raise exceptions.ChannelNotFound()
        rt_info["bootstrap.servers"] = "{}:{}".format(
            databus_channel.cluster_domain,
            databus_channel.cluster_port,
        )
        rt_info["brokerServiceUrl"] = "{}:{}".format(
            databus_channel.cluster_domain,
            databus_channel.zk_port,
        )
        rt_info["channel_cluster_name"] = databus_channel.cluster_name
        rt_info["channel_type"] = settings.TYPE_PULSAR
        rt_info["pulsar_channel_token"] = get_pulsar_channel_token(databus_channel)

    if "hdfs" in storages:
        rt_info["hdfs.data_type"] = "unknown"
        if "data_type" in storages["hdfs"]:
            rt_info["hdfs.data_type"] = storages["hdfs"]["data_type"]

    return rt_info


def get_rt_fields_storages(rt_id):
    """
    从Meta接口中获取rt的字段和存储信息, 地理字段填充到geog_area字段
    :param rt_id: result table的id
    :return: rt的字段和存储信息
    """
    res = MetaApi.result_tables.retrieve({"result_table_id": rt_id, "related": ["fields", "storages"]})
    if res.is_success():
        try:
            res.data["geog_area"] = res.data["tags"]["manage"]["geog_area"][0]["code"]
        except Exception as e:
            logger.error("meta response error: {}, data={}".format(e, res.data))
            raise MetaResponseError(message_kv={"field": "geog_area"})
        return res.data
    else:
        logger.warning(
            "failed to get rt {} info from meta! response: {} {} {}".format(rt_id, res.code, res.message, res.data)
        )
        return None


def get_rt_data_processing(rt_id):
    """
    从Meta接口中获取rt的处理节点信息, 地理字段填充到geog_area字段
    :param rt_id: result table的id
    :return: rt的字段和存储信息
    """
    res = MetaApi.result_tables.retrieve(
        {"result_table_id": rt_id, "related": ["data_processing"]}, raise_exception=True
    )
    return res.data


def get_batch_rt_fields_storages(rt_ids):
    """
    批量从Meta接口中获取rt的字段和存储信息
    :param rt_ids: result table的id列表
    :return: rt的字段和存储信息
    """
    rt_info_dict = dict()
    if rt_ids:
        res = MetaApi.result_tables.list({"result_table_ids": rt_ids, "related": ["fields", "storages"]})
        if res.is_success():
            rt_infos = res.data
            if rt_infos:
                for cur_rt_info in rt_infos:
                    rt_info_dict[cur_rt_info["result_table_id"]] = cur_rt_info

        else:
            logger.warning(
                "failed to get rts {} info from meta! response: {} {} {}".format(
                    rt_ids, res.code, res.message, res.data
                )
            )

    return rt_info_dict


def _check_meta_field(key, data):
    if key not in data or data[key] is None:
        raise MetaResponseError(message_kv={"field": key})


def alert_rt(result_table_id, result_table_name_alias, bk_username, fields):
    """
    从Meta接口中更新rt表别名
    :param result_table_id: result table的id
    :param result_table_name_alias: 别名
    :param bk_username 用户名
    :param fields 字段名
    :return:
    """
    if fields:
        for field in fields:
            field["is_dimension"] = 1 if field["is_dimension"] else 0

    res = MetaApi.result_tables.update(
        {
            "result_table_id": result_table_id,
            "result_table_name_alias": result_table_name_alias,
            "bk_username": bk_username,
            "fields": fields,
        }
    )
    if res.is_success():
        return res.data
    else:
        logger.warning(
            "failed to update rt %s info from meta! response: %s %s %s"
            % (result_table_id, res.code, res.message, res.data)
        )
        return None


def get_databus_rt_storage_info(rt_id, storage):
    """
    获取rt指定的存储的配置信息
    :param rt_id: result_table表的id
    :param storage: 存储类型
    :return: 存储的配置信息
    """
    res = MetaApi.result_tables.storages({"result_table_id": rt_id, "cluster_type": storage})
    if res.is_success():
        return res.data
    else:
        logger.warning(
            "failed to get rt %s storage %s info from meta! response: %s %s %s"
            % (rt_id, storage, res.code, res.message, res.data)
        )
        raise GetRtStorageError(message_kv={"message": res.message})


def get_rt_data_processing_input(rt_id):
    res = MetaApi.data_processings.retrieve({"processing_id": rt_id})
    if res.is_success():
        return res.data["inputs"]
    else:
        logger.warning(
            "failed to get rt %s processing info from meta! response: %s %s %s"
            % (rt_id, res.code, res.message, res.data)
        )
        raise GetRtDataProcessingError(message_kv={"message": res.message})


def get_storage_cluster(storage, cluster):
    """
    获取指定的存储集群的配置信息
    :param storage: 存储集群类型
    :param cluster: 存储集群名称
    :return: 存储集群的配置信息
    """
    res = StoreKitApi.cluster_config.retrieve({"cluster_name": cluster, "cluster_type": storage})
    if res.is_success():
        return res.data
    else:
        logger.warning(
            "failed to get %s storage config for %s! response: %s %s %s"
            % (cluster, storage, res.code, res.message, res.data)
        )
        raise StorageClusterNotExistsError(message_kv={"storage": storage, "cluster": cluster})


def get_channel_id_for_rt_list(rt_list):
    """
    获取指定的rt列表的kafka存储信息
    :param rt_list: result_table_id的列表
    :return:result_table_id到kafka channel id的映射
    """
    result = {}
    for rt_id in rt_list:
        res = get_databus_rt_storage_info(rt_id, "kafka")
        if res:
            result[rt_id] = res["kafka"]["storage_channel"]["id"]
        else:
            raise RtKafkaStorageNotExistError(message_kv={"result_table_id": rt_id})

    return result


def get_rt_kafka_bootstap_server(rt_id):
    """
    获取rt对应的kafka集群的地址
    :param rt_id: result_table表的id
    :return: rt对应的kafka集群的地址，或者None
    """
    info = get_databus_rt_storage_info(rt_id, "kafka")
    if info and "kafka" in info:
        return "{}:{}".format(
            info["kafka"]["storage_channel"]["cluster_domain"],
            info["kafka"]["storage_channel"]["cluster_port"],
        )
    else:
        return None


def get_daily_record_count(rt_id):
    """
    获取rt_id最近24小时的数据量，如果是清洗rt，则获取对应rawdata的数据量，
    否则通过rt对应的inner kafka中topic的消息数量和每条消息中包含的记录数量来计算。
    :param rt_id: result table的id
    :return: rt最近24小时的数据量
    """
    result = 0
    databus_clean = model_manager.get_clean_by_processing_id(rt_id)
    if databus_clean:
        return rawdata.get_daily_record_count(databus_clean.raw_data_id)
    else:
        # 非清洗的rt，计算inner kafka中消息数量
        kafka_bs = get_rt_kafka_bootstap_server(rt_id)
        if kafka_bs:
            # 最近24小时kafka中的消息数量
            result = channel.get_msg_count_in_passed_ms(kafka_bs, get_topic_name(rt_id), 24 * 3600 * 1000)
            # 获取最近一条消息中的记录数量，用记录数量 * 消息总数，得到实际的最近24小时的记录总数
            msg_tail = channel.inner_kafka_tail(kafka_bs, get_topic_name(rt_id), 0, 1)
            result = result * len(msg_tail) if len(msg_tail) > 0 else result

    return result


def rt_kafka_tail(rt_id, partition, limit):
    """
    获取rt_id对应的kafka topic中最近的几条记录
    :param rt_id: result_table的id
    :param partition: 分区编号
    :param limit: 最大返回记录条数
    :return: kafka上rt_id对应的topic上最近的记录内容
    """
    rt_info = get_databus_rt_info(rt_id)
    if rt_info[CHANNEL_TYPE] == PULSAR:
        return channel.inner_pulsar_tail(rt_info[CHANNEL_CLUSTER_NAME], get_topic_name(rt_id), partition, limit)

    kafka_bs = get_rt_kafka_bootstap_server(rt_id)
    if kafka_bs:
        return channel.inner_kafka_tail(kafka_bs, get_topic_name(rt_id), partition, limit)
    else:
        logger.warning("failed to find kafka storage for rt %s" % rt_id)
        return []


def rt_kafka_message(rt_id, partition, offset, msg_count=1):
    """
    获取rt_id对应的kafka的topic上指定分区和offset的消息内容
    :param rt_id: result_table的id
    :param partition: 分区编号
    :param offset: 消息的offset
    :param msg_count: 拉取的消息数量
    :return: kafka上rt_id对应的topic上的消息内容
    """
    kafka_bs = get_rt_kafka_bootstap_server(rt_id)
    if kafka_bs:
        return channel.inner_kafka_message(kafka_bs, get_topic_name(rt_id), partition, offset, msg_count)
    else:
        return []


def rt_kafka_offsets(rt_id, group_id=None):
    """
    获取rt_id对应的kafka topic上每个分区里消息数量和offset信息
    :param rt_id: result_table表的id
    :param group_id: 消费者所属group
    :return: kafka上rt_id对应的topic的offset信息
    """
    kafka_bs = get_rt_kafka_bootstap_server(rt_id)
    if kafka_bs:
        topic = get_topic_name(rt_id)
        if group_id:
            return channel.get_topic_offsets_with_group(kafka_bs, topic, group_id)
        else:
            return channel.get_topic_offsets(kafka_bs, topic)
    else:
        return {}


def get_partitions(rt_id):
    """
    获取rt_id对应的kafka topic的partition数量
    @:param rt_id: result_table表的id
    :return: kafka上rt_id对应的topic的分区数量
    """
    kafka_bs = get_rt_kafka_bootstap_server(rt_id)
    if kafka_bs:
        # 当topic不存在时，会尝试发送一条空消息到kafka来自动创建topic
        return channel.get_topic_partition_num(kafka_bs, get_topic_name(rt_id))
    else:
        # 默认返回0，代表rt对于的kafka topic不存在
        return 0


def set_topic(rt_id, r_size, r_hours):
    result = {}
    # 获取单个result_table信息
    rt_info = get_rt_fields_storages(rt_id)
    if rt_info:
        if "kafka" in rt_info["storages"]:
            databus_channel = model_manager.get_channel_by_id(rt_info["storages"]["kafka"]["storage_channel"]["id"])
            zk_addr = "{}:{}{}".format(
                databus_channel.zk_domain,
                databus_channel.zk_port,
                databus_channel.zk_root_path,
            )
            topic = get_topic_name(rt_id)
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
        else:
            logger.warning(
                "no kafka storage for {}, unable to increase partitions. {}".format(rt_id, rt_info["storages"])
            )
            raise KafkaStorageNotExistsError(message_kv={"rt_id": rt_id})
    else:
        raise ApiResultError()


def do_set_partitions(rt_info, partitions):
    """
    设置RT输出分区
    :param rt_info: rt_info
    :param partitions: 分区数量
    :return: 执行结果
    """
    storage_dict = rt_info.get(STORAGES)
    channel_storage = storage_dict.get(settings.TYPE_KAFKA)
    if not channel_storage:
        channel_storage = storage_dict.get(settings.TYPE_PULSAR)

    channel_id = channel_storage[STORAGE_CHANNEL][ID]
    databus_channel = model_manager.get_channel_by_id(channel_id)
    topic = get_topic_name(rt_info[RESULT_TABLE_ID])
    channel_host = "{}:{}".format(
        databus_channel.cluster_domain,
        databus_channel.cluster_port,
    )

    if databus_channel.cluster_type == settings.TYPE_PULSAR:
        pulsar_topic.increment_partitions(channel_host, topic, partitions, get_pulsar_channel_token(databus_channel))
        output = ""
    else:
        create_multi_partition_kafka_topic(channel_host, topic, partitions)

    result = {
        "topic": topic,
        "partitions": partitions,
        "output": output,
    }
    # 扩容清洗RT需要扩容源数据的分区, 给出提示
    if rt_info[PROCESSING_TYPE] == settings.CLEAN_TRT_CODE:
        databus_clean = model_manager.get_clean_by_processing_id(rt_info[RESULT_TABLE_ID])
        result["warning"] = "need to alter kafka partitions for topic of rawdata %s" % databus_clean.raw_data_id
    return result


def set_partitions(rt_id, partitions):
    """
    对rt_id对应的kafka topic的partition进行扩容，增加partition数量
    """
    # 获取单个result_table信息
    rt_info = get_rt_fields_storages(rt_id)
    if not rt_info:
        raise ApiResultError()

    if settings.TYPE_KAFKA not in rt_info[STORAGES] and settings.TYPE_PULSAR not in rt_info[STORAGES]:
        logger.warning("no kafka storage for {}, unable to increase partitions. {}".format(rt_id, rt_info[STORAGES]))
        raise KafkaStorageNotExistsError(message_kv={"rt_id": rt_id})

    result = list()
    # 校验是否为清洗的rt，如果是清洗rt，则需要扩容rawdata的所有清洗RT
    if rt_info[PROCESSING_TYPE] == settings.CLEAN_TRT_CODE:
        databus_clean = model_manager.get_clean_by_processing_id(rt_id)
        raw_data_id = databus_clean.raw_data_id
        clean_infos = model_manager.get_clean_list_by_raw_data_id(raw_data_id)
        for clean_info in clean_infos:
            rt_info = get_rt_fields_storages(clean_info["processing_id"])
            result.append(do_set_partitions(rt_info, partitions))
    else:
        result.append(do_set_partitions(rt_info, partitions))

    return result


def get_rt_destinations(rt_id):
    """
    获取rt后面接的分发存储列表
    """
    result = []
    res = MetaApi.result_tables.storages({"result_table_id": rt_id})
    if res.is_success():
        result = res.data.keys()

    return result


def notify_change(rt_id):
    """
    标记rt的清洗配置发生变化
    """
    try:
        geog_area = tag.get_tag_by_rt_id(rt_id)
        # 将配置发生变化的rt_id写入zk中指定的节点
        real_path = zk_helper.add_sequence_node(
            settings.CONFIG_ZK_ADDR[geog_area], settings.ZK_RT_CONFIG_CHANGE_PATH, rt_id
        )
        logger.info("setting zk node {} {}, node {} added".format(settings.ZK_RT_CONFIG_CHANGE_PATH, rt_id, real_path))
        return True
    except Exception as e:
        logger.exception(
            e,
            "failed to mark clean conf changed for result_table_id %s on zookeeper!" % rt_id,
        )
        return False


def is_batch_data(rt_id):
    """
    是否清洗rt_id对应的数据源为批量导入的数据
    @:param result_table的id
    @:return True/False，标记是否数据源为TDW这种批量导入的数据
    """
    databus_clean = model_manager.get_clean_by_processing_id(rt_id)
    if databus_clean:
        raw_data = model_manager.get_raw_data_by_id(databus_clean.raw_data_id)
        if raw_data:
            return raw_data.data_scenario == "tdw"

    return False


def get_topic_name(rt_id):
    """
    根据result_table的id获取对应的kafka topic的名称
    :param rt_id: result_table的id
    :return: rt_id对应的kafka topic名称
    """
    return "table_%s" % rt_id


def has_next_dt_or_dp(rt_id):
    """
    是否存在下游的data transform或者data processing
    :param rt_id: rt
    :return: 是否存在
    """
    dp_list, dt_list = get_rt_dt_and_dp(rt_id)

    has_next = False  # 是否有下游
    # clean 自身存在一个DP, 当存在多个DP时, 下游有flow中的节点, 有DT时,下游有存储
    if len(dp_list) > 1 or len(dt_list) > 0:
        has_next = True

    # TODO 旧的数据没有创建DT,暂时从其他接口获取存储信息, 等后面补录DT后再移除
    if not has_next:
        res = MetaApi.result_tables.storages({"result_table_id": rt_id}, raise_exception=True)
        """
        {
            "tspider": {
              ...
            },
            "kafka": {
                ...
            }
          }
        """
        if res.data:
            for storage in res.data.keys():
                # 若存在非kafka的
                if storage != "kafka":
                    has_next = True
                    break
    return has_next


def get_rt_dt_and_dp(rt_id):
    """
    获取RT的DT和DP
    :param rt_id: rt
    :return: ([DT], [DP])
    """
    retrieve_rt_param = {
        "~DataProcessingRelation.data_set": {"processing_id": True},
        "~DataTransferringRelation.data_set": {"transferring_id": True},
    }
    res = MetaApi.result_tables.retrieve(
        {
            "result_table_id": rt_id,
            "erp": json.dumps(retrieve_rt_param),
        },
        raise_exception=True,
    )
    """
    {
        "~DataProcessingRelation.data_set": [
          {
            "processing_id": "rt_id",
            "identifier_value": 123
          }
        ],
        "identifier_value": "rt_id"
    }
    """
    dp_list = list()
    dt_list = list()
    dp_set = res.data.get("~DataProcessingRelation.data_set")
    dt_set = res.data.get("~DataTransferringRelation.data_set")
    if dp_set:
        dp_list = [x["processing_id"] for x in dp_set]
    if dt_set:
        dt_list = [x["transferring_id"] for x in dt_set]

    return dp_list, dt_list


def is_single_rt(rt_id):
    """
    是否是孤立的RT, 如果没有任务DT和DP, 则是孤立的
    :param rt_id: rt
    :return:
    """
    dp_list, dt_list = get_rt_dt_and_dp(rt_id)
    if not dp_list and not dt_list:
        return True
    else:
        return False


def delete_topic_by_rt_info(rt_info):
    """
    删除rt的topic
    :param rt_info: rt信息
    """
    if KAFKA in rt_info.keys():
        topic = rt_info[KAFKA][PHYSICAL_TABLE_NAME]
        bootstrap_server = rt_info["bootstrap.servers"]
        logger.info("delete topic {} from {}".format(topic, bootstrap_server))
        kafka_tool.delete_topic(bootstrap_server, topic)
    else:
        logger.warning("not exist kafka in this rt:%s" % str(rt_info))


def delete_clean_topic_by_rt(result_table_id):
    """
    删除rt的topic
    :param result_table_id: rt
    """
    rt_info = get_databus_rt_info(result_table_id)
    delete_topic_by_rt_info(rt_info)
