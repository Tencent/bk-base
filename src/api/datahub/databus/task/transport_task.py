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

from clickhouse_driver import Client
from common.log import logger
from datahub.databus.exceptions import (
    TaskStartErr,
    TaskStorageConfigErr,
    TaskStorageConnConfigErr,
    TaskTransportRtError,
    TaskTransportStatusError,
    TaskTransportStorageError,
)
from datahub.databus.settings import (
    COMPONENT_TRANSPORT,
    DEFAULT_THIN_CLIENT_THRESHOLD,
    MODULE_TRANSPORT,
    QUERY_FIELDS_SQL,
    TRANSPORT_STORAGE_SINK_SET,
    TRANSPORT_STORAGE_SOURCE_SET,
    TYPE_PULSAR,
)
from datahub.databus.task.pulsar.config import (
    build_transport_hdfs_ck_param,
    build_transport_hdfs_iceberg_param,
    build_transport_hdfs_ignite_param,
)

from datahub.databus import exceptions, model_manager, models, rt, settings

from ...common.const import (
    CLICKHOUSE,
    CLUSTER_NAME,
    CONNECTION_INFO,
    DATA_TYPE,
    DEFAULT,
    DESCRIPTION,
    EMPTY_STRING,
    EXECUTE,
    FINISHED,
    GEOG_AREA,
    HDFS,
    HOST,
    ICEBERG,
    IGNITE,
    KEY_SEPARATOR,
    MAX_RECORDS,
    MESSAGE,
    PARALLELISM,
    PASSWORD,
    PERCENTAGE,
    PHYSICAL_TABLE_NAME,
    PORT,
    SINK_RT_ID,
    SINK_TYPE,
    SOURCE_RT_ID,
    SOURCE_TYPE,
    STAGE_SEQ,
    STAGE_STATUS,
    STAGE_TYPE,
    STAGES,
    STATUS,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGE_KEYS,
    STORAGES,
    SUCCESS,
    TCP_TGW,
    THIN_CLIENT_THRESHOLD,
    TIME_TAKEN,
    TIME_TAKEN_JAVA,
    UNKNOWN,
    USER,
    VALUE,
)
from ...storekit.iceberg import construct_hdfs_conf
from . import status
from .pulsar import task as pulsar_task
from .pulsar import topic as pulsar_topic
from .pulsar.topic import generate_transport_pulsar_topic, get_pulsar_channel_token
from .task_utils import (
    _stop_task_without_exception,
    generate_transport_connector_name,
    get_pulsar_task_message,
    get_transport_cluster_name,
    is_databus_task_exists,
    query_status_and_task,
)


def get_hdfs_transport_task(message, connector_name):
    status = message.get(STATUS, UNKNOWN).lower()
    # 对于已经成功执行的任务，可以删除在transport集群中的记录
    if status == SUCCESS.lower() and is_databus_task_exists(
        connector_name, TYPE_PULSAR, MODULE_TRANSPORT, COMPONENT_TRANSPORT, None, True
    ):
        stop_transport(connector_name)
    stage = {
        FINISHED: message.get(FINISHED, EMPTY_STRING),
        TIME_TAKEN: message.get(TIME_TAKEN_JAVA, EMPTY_STRING),
        STAGE_STATUS: message.get(STATUS, EMPTY_STRING),
        STAGE_TYPE: EXECUTE,
        DESCRIPTION: u"执行数据迁移",
        STAGE_SEQ: 1,
    }
    result = {
        STATUS: status,
        MESSAGE: message.get(MESSAGE, "get status failed"),
        PERCENTAGE: message.get(FINISHED, 0) / 100,
        STAGES: [stage],
    }
    return result


def process_transport_task(params):
    """
    校验参数，并启动迁移任务
    :param params: 启动迁移任务的全部参数集合
    """
    source_rt_id, source_type = params[SOURCE_RT_ID], params[SOURCE_TYPE]
    sink_rt_id, sink_type = params[SINK_RT_ID], params[SINK_TYPE]
    if source_type not in TRANSPORT_STORAGE_SOURCE_SET or sink_type not in TRANSPORT_STORAGE_SINK_SET:
        raise TaskTransportStorageError(message_kv={SOURCE_TYPE: source_type, SINK_TYPE: sink_type})

    if source_rt_id == sink_rt_id and source_type == sink_type:
        raise TaskTransportRtError(message_kv={"params": params})

    # 目前支持hdfs->clickhouse; hdfs->hdfs迁移
    logger.info(
        "create trasport task, transport data from {} {} to {} {} ".format(
            source_type, source_rt_id, sink_type, sink_rt_id
        )
    )
    return _start_transport(params)


def _start_transport(params):
    """
    启动hdfs到clickhouse的直接数据迁移任务
    :param params: 启动迁移任务的全部参数集合
    """
    component, module, cluster_type = (
        settings.COMPONENT_TRANSPORT,
        settings.MODULE_TRANSPORT,
        settings.TYPE_PULSAR,
    )
    connector = generate_transport_connector_name(params)
    obj = model_manager.get_connector_route(connector)
    if obj:
        # 若存在记录则使用上次的集群
        cluster_name = obj.cluster_name
    else:
        cluster_name = get_transport_cluster_name(settings.TYPE_PULSAR, component)

    # 添加databus任务信息
    add_databus_transport_task_info(cluster_name, connector, cluster_type, module, component, params)

    # 生成配置
    source_type, sink_type = params[SOURCE_TYPE], params[SINK_TYPE]
    if source_type == HDFS and sink_type == CLICKHOUSE:
        conf = _build_transport_hdfs_ck_param(connector, params)
    elif source_type == HDFS and sink_type == HDFS:
        conf = _build_transport_hdfs_hdfs_config(connector, params)
    elif source_type == HDFS and sink_type == IGNITE:
        conf = _build_transport_hdfs_ignite_param(connector, params)
    else:
        raise TaskTransportStorageError(message_kv={SOURCE_TYPE: source_type, SINK_TYPE: sink_type})

    # 启动任务
    ret = start_transport_task(cluster_type, cluster_name, connector, conf)
    return connector if ret else ""


def get_transport_task_status(connector_name, for_datalab=True):
    """
    获取指定transport任务的当前状态
    :param connector_name: transport任务名
    :param for_datalab: 返回结果是否用于datalab
    :return: 任务状态
    """
    stage = {
        FINISHED: EMPTY_STRING,
        TIME_TAKEN: EMPTY_STRING,
        STAGE_STATUS: EMPTY_STRING,
        STAGE_TYPE: EXECUTE,
        DESCRIPTION: u"执行数据迁移",
        STAGE_SEQ: 1,
    }

    task_status, task = query_status_and_task(connector_name)

    # 根据表中状态判断是否已经完成
    if task_status == models.DataBusTaskStatus.STOPPED:
        stop_transport(connector_name)
        result = {
            STATUS: "success",
            PERCENTAGE: 1.0,
            MESSAGE: "transport success",
            STAGES: [stage],
        }
        return result

    messages = get_pulsar_task_message(connector_name, 1)
    # 根据topic中的数据情况，判断是否开始写入任务了
    if not messages:
        result = {
            STATUS: "pending",
            PERCENTAGE: 0.0,
            MESSAGE: "waiting for transport",
            STAGES: [stage],
        }
        return result

    try:
        # 通过解析messages信息，展示具体迁移中的状态
        message = json.loads(messages[0][VALUE])
        if not for_datalab:
            return message

        return get_hdfs_transport_task(message, connector_name)
    except Exception as e:
        logger.warning("{}: failed to get topic message, exception: {}".format(connector_name, str(e)))
        raise TaskTransportStatusError(message=u"状态解析异常")


def delete_transport_task_status(connector_name):
    """
    清楚connector在pulsar中保存的状态
    :param connector_name: 任务名
    """
    connector = model_manager.get_connector_route(connector_name)
    transport_cluster = model_manager.get_cluster_by_name(connector.cluster_name)
    topic = generate_transport_pulsar_topic(
        connector.data_source,
        connector.source_type,
        connector.data_sink,
        connector.sink_type,
    )
    channel = model_manager.get_channel_by_name(transport_cluster.channel_name)
    channel_host = "{}:{}".format(channel.cluster_domain, channel.cluster_port)
    pulsar_topic.delete_topic(
        channel_host,
        topic,
        get_pulsar_channel_token(channel),
        settings.PULSAR_TASK_TENANT,
        settings.PULSAR_TASK_STATUS_NAMESPACE,
    )
    pulsar_topic.delete_topic(
        channel_host,
        topic,
        get_pulsar_channel_token(channel),
        settings.PULSAR_OUTER_TENANT,
        settings.PULSAR_OUTER_NAMESPACE,
    )
    topic = "%s_instance" % topic
    pulsar_topic.delete_topic(
        channel_host,
        topic,
        get_pulsar_channel_token(channel),
        settings.PULSAR_TASK_TENANT,
        settings.PULSAR_TASK_STATUS_NAMESPACE,
    )


def stop_transport(connector_name):
    """
    通用停止transport
    :param connector_name: 任务名
    """
    obj = model_manager.get_connector_route(connector_name)
    if not obj:
        return
    _stop_task_without_exception(connector_name, obj.cluster_name, True)


def add_databus_transport_task_info(cluster_name, connector_name, cluster_type, module, component, params):
    """
    新增transport任务
    :param params: 用户传入的原始参数集合
    :param cluster_name: 集群名
    :param connector_name: connector名称
    :param cluster_type: 集群类型 kafka/pulsar
    :param module: module. ex. puller/clean/shipper
    :param component: component. ex. datanode/clean/es
    """
    source_rt_id, source_type = params[SOURCE_RT_ID], params[SOURCE_TYPE]
    sink_rt_id, sink_type = params[SINK_RT_ID], params[SINK_TYPE]
    logger.info("add {} task: component={} with params {}".format(connector_name, component, params))
    if not is_databus_task_exists(connector_name, cluster_type, module, component, None, True):
        processing_id = "{}_{}_{}_{}_{}".format(
            settings.MODULE_TRANSPORT,
            source_rt_id,
            source_type,
            sink_rt_id,
            sink_type,
        )
        model_manager.add_databus_task(
            processing_id,
            connector_name,
            cluster_name,
            source_rt_id,
            source_type,
            sink_rt_id,
            sink_type,
            settings.MODULE_TRANSPORT,
        )


def _build_transport_hdfs_ck_param(connector, params):
    """
    构建hdfs迁移clickhouse任务的配置集合
    :param connector: 任务名
    :param params: 用户传入的原始参数集合
    :return: 配置内容
    """
    source_rt_id, sink_rt_id, parallelism = (
        params[SOURCE_RT_ID],
        params[SINK_RT_ID],
        params[PARALLELISM],
    )
    source_rt_info, sink_rt_info = rt.get_rt_fields_storages(source_rt_id), rt.get_rt_fields_storages(sink_rt_id)
    # hdfs 相关参数
    hdfs = source_rt_info[STORAGES][HDFS]
    hdfs_conn_info = hdfs[STORAGE_CLUSTER][CONNECTION_INFO]
    geog_area = source_rt_info[GEOG_AREA]
    data_type = hdfs[DATA_TYPE]
    physical_tn = hdfs[PHYSICAL_TABLE_NAME]
    data_dir = EMPTY_STRING
    iceberg_db = EMPTY_STRING
    iceberg_table = EMPTY_STRING
    if data_type == ICEBERG:
        iceberg_db, iceberg_table = physical_tn.split(".")[0], physical_tn.split(".")[1]
        hdfs_conf = construct_hdfs_conf(hdfs_conn_info, geog_area, ICEBERG)
    else:
        data_dir = physical_tn
        hdfs_conf = build_hdfs_custom_conf_from_conn(json.loads(hdfs_conn_info))

    # clickhouse相关参数
    ck = sink_rt_info[STORAGES][CLICKHOUSE]
    ck_conn_info = ck[STORAGE_CLUSTER][CONNECTION_INFO]
    ck_conn = json.loads(ck_conn_info)
    host, port = ck_conn[TCP_TGW].split(":")[0], int(ck_conn[TCP_TGW].split(":")[1])
    client = Client(host=host, port=port, database=DEFAULT, user=DEFAULT)
    physical_tn = ck[PHYSICAL_TABLE_NAME]
    db_name, replicated_table = (
        physical_tn.split(".")[0],
        "%s_local" % physical_tn.split(".")[1],
    )

    # 表字段信息
    sql = QUERY_FIELDS_SQL.format(db_name, replicated_table)
    clickhouse_col_order = ",".join(["{}:{}".format(cell[0], cell[1]) for cell in client.execute(sql)])

    return build_transport_hdfs_ck_param(
        connector,
        source_rt_id,
        hdfs_conf,
        data_dir,
        iceberg_db,
        iceberg_table,
        data_type,
        sink_rt_id,
        ck_conn_info,
        db_name,
        replicated_table,
        clickhouse_col_order,
        parallelism,
    )


def _build_transport_hdfs_hdfs_config(connector, params):
    """
    构建hdfs迁移hdfs任务的配置集合
    :param connector: 任务名
    :param params: 用户传入的原始参数集合
    :return: 配置内容
    """
    source_rt_id, sink_rt_id, parallelism = (
        params[SOURCE_RT_ID],
        params[SINK_RT_ID],
        params[PARALLELISM],
    )
    source_rt_info, sink_rt_info = rt.get_rt_fields_storages(source_rt_id), rt.get_rt_fields_storages(sink_rt_id)
    # source hdfs 相关参数
    source_hdfs = source_rt_info[STORAGES][HDFS]
    source_hdfs_conn_info = source_hdfs[STORAGE_CLUSTER][CONNECTION_INFO]
    source_geog_area = source_rt_info[GEOG_AREA]
    source_data_type = source_hdfs[DATA_TYPE]
    source_physical_tn = source_hdfs[PHYSICAL_TABLE_NAME]
    source_data_dir = EMPTY_STRING
    source_iceberg_db = EMPTY_STRING
    source_iceberg_table = EMPTY_STRING
    if source_data_type == ICEBERG:
        source_iceberg_db, source_iceberg_table = (
            source_physical_tn.split(".")[0],
            source_physical_tn.split(".")[1],
        )
        source_hdfs_conf = construct_hdfs_conf(source_hdfs_conn_info, source_geog_area, ICEBERG)
    else:
        source_data_dir = source_physical_tn
        source_hdfs_conf = build_hdfs_custom_conf_from_conn(json.loads(source_hdfs_conn_info))

        # sink hdfs 相关参数
    sink_hdfs = sink_rt_info[STORAGES][HDFS]
    sink_hdfs_conn_info = sink_hdfs[STORAGE_CLUSTER][CONNECTION_INFO]
    sink_geog_area = sink_rt_info[GEOG_AREA]
    sink_data_type = sink_hdfs[DATA_TYPE]
    sink_physical_tn = sink_hdfs[PHYSICAL_TABLE_NAME]
    sink_iceberg_db = EMPTY_STRING
    sink_iceberg_table = EMPTY_STRING
    if sink_data_type == ICEBERG:
        sink_iceberg_db, sink_iceberg_table = (
            sink_physical_tn.split(".")[0],
            sink_physical_tn.split(".")[1],
        )
        sink_hdfs_conf = construct_hdfs_conf(sink_hdfs_conn_info, sink_geog_area, ICEBERG)
    else:
        sink_hdfs_conf = build_hdfs_custom_conf_from_conn(json.loads(sink_hdfs_conn_info))

    return build_transport_hdfs_iceberg_param(
        connector,
        source_rt_id,
        source_hdfs_conf,
        source_data_dir,
        source_iceberg_db,
        source_iceberg_table,
        source_data_type,
        sink_rt_id,
        sink_iceberg_db,
        sink_iceberg_table,
        sink_hdfs_conf,
        parallelism,
    )


def _build_transport_hdfs_ignite_param(connector, params):
    """
    构建hdfs迁移hdfs任务的配置集合
    :param connector: 任务名
    :param params: 用户传入的原始参数集合
    :return: 配置内容
    """
    source_rt_id, sink_rt_id, parallelism = (
        params[SOURCE_RT_ID],
        params[SINK_RT_ID],
        params[PARALLELISM],
    )
    source_rt_info, _ = rt.get_rt_fields_storages(source_rt_id), rt.get_rt_fields_storages(sink_rt_id)
    # source hdfs 相关参数
    source_hdfs = source_rt_info[STORAGES][HDFS]
    source_hdfs_conn_info = source_hdfs[STORAGE_CLUSTER][CONNECTION_INFO]
    source_geog_area = source_rt_info[GEOG_AREA]
    source_data_type = source_hdfs[DATA_TYPE]
    source_physical_tn = source_hdfs[PHYSICAL_TABLE_NAME]
    source_data_dir = EMPTY_STRING
    source_iceberg_db = EMPTY_STRING
    source_iceberg_table = EMPTY_STRING
    if source_data_type == ICEBERG:
        source_iceberg_db, source_iceberg_table = (
            source_physical_tn.split(".")[0],
            source_physical_tn.split(".")[1],
        )
        source_hdfs_conf = construct_hdfs_conf(source_hdfs_conn_info, source_geog_area, ICEBERG)
    else:
        source_data_dir = source_physical_tn
        source_hdfs_conf = build_hdfs_custom_conf_from_conn(json.loads(source_hdfs_conn_info))

    # sink hdfs 相关参数
    rt_info = rt.get_databus_rt_info(sink_rt_id)
    try:
        conn_info = rt_info[IGNITE][CONNECTION_INFO]
        storage_conn = json.loads(conn_info)
    except Exception:
        raise TaskStorageConnConfigErr()
    storage_config = json.loads(rt_info[IGNITE][STORAGE_CONFIG])
    key_fields = ",".join(storage_config[STORAGE_KEYS])
    key_separator = storage_config.get(KEY_SEPARATOR, "")
    max_records = storage_config.get(MAX_RECORDS, 100000)
    threshold = storage_conn.get(THIN_CLIENT_THRESHOLD, DEFAULT_THIN_CLIENT_THRESHOLD)
    use_thin_client = int(max_records) < int(threshold)
    cache_name = rt_info[IGNITE][PHYSICAL_TABLE_NAME].split(".")[1]
    cluster_name = rt_info[IGNITE][CLUSTER_NAME]
    host = storage_conn[HOST]
    password = storage_conn[PASSWORD]
    port = storage_conn[PORT]
    user = storage_conn[USER]

    return build_transport_hdfs_ignite_param(
        connector,
        source_rt_id,
        source_hdfs_conf,
        source_data_dir,
        source_iceberg_db,
        source_iceberg_table,
        source_data_type,
        sink_rt_id,
        key_fields,
        key_separator,
        cache_name,
        max_records,
        cluster_name,
        host,
        password,
        port,
        user,
        use_thin_client,
        parallelism,
    )


def start_transport_task(cluster_type, cluster_name, task_name, conf):
    """
    通用的启动任务功能
    :param cluster_type: kafka/pulsar
    :param cluster_name: kafka/pulsar集群名
    :param task_name: 迁移任务名
    :param conf: 迁移任务配置
    :return: 是否启动成功
    """
    # 启动任务
    if cluster_type == settings.TYPE_PULSAR:
        ret = pulsar_task.start_or_create_task(cluster_name, settings.TYPE_TRANSPORT, task_name, conf)
        # 若存在则更新配置
        if ret:
            ret = pulsar_task.update_task(cluster_name, settings.TYPE_TRANSPORT, task_name, conf)
    else:
        # 迁移任务只能在pulsar集群上执行
        raise exceptions.NotSupportErr()

    if ret:
        logger.info("task {} is started and running in {}".format(task_name, cluster_name))
        status.set_databus_task_status(task_name, cluster_name, models.DataBusTaskStatus.RUNNING)
        return ret
    else:
        logger.error("failed to start task {} in {}".format(task_name, cluster_name))
        raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster_name})


def build_hdfs_custom_conf_from_conn(storage_conn):
    """
    根据hdfs集群的connection info构建hdfs集群的连接参数
    :param storage_conn: hdfs集群的链接串
    :return: hdfs集群的连接参数，dict类型
    """
    custom_conf = storage_conn["hdfs_default_params"] if "hdfs_default_params" in storage_conn else {}
    hdfs_cluster_name = storage_conn["hdfs_cluster_name"]
    hosts_arr = storage_conn["hosts"].split(",")
    port = storage_conn["port"]
    rpc_port = storage_conn["rpc_port"]
    servicerpc_port = storage_conn["servicerpc_port"]
    namenode_arr = storage_conn["ids"].split(",")

    if len(namenode_arr) != len(hosts_arr):
        raise TaskStorageConfigErr()

    # 设置参数值，其中namenode有多个，需要拆分为list
    custom_conf["fs.defaultFS"] = storage_conn["hdfs_url"]
    custom_conf["dfs.nameservices"] = hdfs_cluster_name
    custom_conf["dfs.ha.namenodes.%s" % hdfs_cluster_name] = storage_conn["ids"]
    custom_conf[
        "dfs.client.failover.proxy.provider.%s" % hdfs_cluster_name
    ] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    for i in range(0, len(namenode_arr)):
        custom_conf["dfs.namenode.rpc-address.{}.{}".format(hdfs_cluster_name, namenode_arr[i])] = "{}:{}".format(
            hosts_arr[i],
            rpc_port,
        )
        custom_conf[
            "dfs.namenode.servicerpc-address.{}.{}".format(hdfs_cluster_name, namenode_arr[i])
        ] = "{}:{}".format(
            hosts_arr[i],
            servicerpc_port,
        )
        custom_conf["dfs.namenode.http-address.{}.{}".format(hdfs_cluster_name, namenode_arr[i])] = "{}:{}".format(
            hosts_arr[i],
            port,
        )

    return custom_conf
