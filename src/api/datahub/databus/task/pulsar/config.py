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

from conf import dataapi_settings
from datahub.common.const import (
    ARCHIVE,
    AVRO,
    CLICKHOUSE_COLUMN_ORDER_JAVA,
    CLICKHOUSE_PROPERTIES_JAVA,
    CONFIGS,
    CONNECTOR,
    DATA_DIR_JAVA,
    DATA_TYPE_JAVA,
    DB_NAME_JAVA,
    FLUSH_SIZE,
    FLUSH_SIZE_JAVA,
    HDFS_CUSTOM_PROPERTY_JAVA,
    HDFS_PROPERTY_JAVA,
    ICEBERG_DATABASE_JAVA,
    ICEBERG_TABLE_JAVA,
    IGNITE_CACHE_JAVA,
    IGNITE_CLUSTER_JAVA,
    IGNITE_HOST_JAVA,
    IGNITE_MAX_RECORDS_JAVA,
    IGNITE_PASS_JAVA,
    IGNITE_PORT_JAVA,
    IGNITE_USER_JAVA,
    INTERVAL,
    KEY_FIELDS_JAVA,
    KEY_SEPARATOR_JAVA,
    LATEST,
    MSG_TYPE_JAVA,
    PARALLELISM,
    REPLICATED_TABLE_JAVA,
    RT_ID_JAVA,
    SINK_CONFIG_JAVA,
    SOURCE_CONFIG_JAVA,
    TABLENAME,
    USE_THIN_CLIENT_JAVA,
)

from datahub.databus import settings

from ..jdbc import get_mode


def build_puller_tdwhdfs_config_param(
    cluster_name,
    connector,
    data_id,
    topic,
    kafka_bs,
    fs_default_name,
    hadoop_job_ugi,
    hdfs_data_dir,
    username,
    secure_id,
    secure_key,
):
    """
    tdw 数据拉取任务配置
    :param cluster_name: 集群名
    :param connector: 任务名
    :param data_id: dataid
    :param topic: 目标topic
    :param kafka_bs: 目标kafka地址
    :param fs_default_name: hdfs地址
    :param hadoop_job_ugi: 内部版tdw的ugi
    :param hdfs_data_dir: 数据目录
    :param username: tdw提供的用户名
    :param secure_id: tdw提供的secure_id
    :param secure_key: tdw提供的secure_key
    :return: config
    """
    task_config = {
        "connector": connector,
        "dataId": "%s" % data_id,
        "fsDefaultName": fs_default_name,
        "hadoopJobUgi": hadoop_job_ugi,
        "hdfsDataDir": hdfs_data_dir,
        "hdfsConfDir": dataapi_settings.HDFS_DEFAULT_PULSAR_CONF_DIR,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    config = {
        "topicName": pulsar_topic,
        "parallelism": 1,
        "archive": "builtin://databus_tdw_puller",
        "schemaType": "STRING",
        "configs": task_config,
    }

    return config


def build_puller_tdbank_config_param(
    group_id,
    task_name,
    data_id,
    topic,
    zk_node,
    tube_group,
    tube_master,
    tube_topic,
    tube_mixed,
    tube_splitter,
    max_tasks,
    tid_key,
):
    """
    tdbank 数据拉取任务配置
    """
    task_config = {
        "connector": task_name,
        "dataId": "%s" % data_id,
        "confPath": zk_node,
        "tubeMaster": tube_master,
        "tubeTopic": tube_topic,
        "tubeGroup": tube_group,
        "mixed": tube_mixed,
        "splitter": tube_splitter,
        "tidKey": tid_key,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "topicName": pulsar_topic,
        "parallelism": max_tasks,
        "archive": "builtin://databus_tdbank_puller",
        "schemaType": "NONE",
        "configs": task_config,
    }


def build_clean_kafka_conf(task_name, source_topic, rt_id, data_id, task_num, target_kafka):
    """
    生成kafka清洗任务配置
    :param task_name: 任务名
    :param source_topic: 源topic
    :param rt_id: result_table_id
    :param data_id: 对应access_raw_data中id
    :param task_num: 并发度
    :param target_kafka: target kafka address
    :return: {dict} 配置信息
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "dataId": "%s" % data_id,
        "msgType": "etl",
        "bootstrapServers": target_kafka,
        "acks": "1",
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, source_topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": task_num,
        "archive": "builtin://databus_clean",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_puller_kafka_config_param(
    data_id,
    task_name,
    source_master,
    source_group,
    source_topic,
    dest_topic,
    max_tasks,
    auto_offset_reset=LATEST,
):
    """
    生成kafka任务配置
    :param data_id: dataid
    :param task_name: 任务名
    :param source_master: 源管道
    :param source_group: 消费组
    :param source_topic: 源topic
    :param dest_topic: 目标topic
    :param max_tasks: 最大并发
    :param auto_offset_reset: auto_offset_reset参数
    :return:
    """
    task_config = {
        "connector": task_name,
        "rtId": data_id,
        "dataId": "%s" % data_id,
        "bootstrapServers": source_master,
        "groupId": source_group,
        "topic": source_topic,
        "autoOffsetReset": auto_offset_reset,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, dest_topic)
    return {
        "topicName": pulsar_topic,
        "parallelism": max_tasks,
        "archive": "builtin://databus_kafka_puller",
        "schemaType": "none",
        "configs": task_config,
    }


def build_puller_kafka_config_with_sasl(
    data_id,
    task_name,
    source_master,
    source_group,
    source_topic,
    dest_topic,
    max_tasks,
    security_protocol,
    sasl_mechanism,
    user,
    password,
    auto_offset_reset=LATEST,
):
    """
    生成kafka任务配置, 带安全协议
    :param data_id: dataid
    :param task_name: 任务名
    :param source_master: 源管道
    :param source_group: 消费组
    :param source_topic: 源topic
    :param dest_topic: 目标topic
    :param max_tasks: 最大并发
    :param security_protocol: 安全协议
    :param sasl_mechanism: 安全机制
    :param user: 用户名
    :param password: 密码
    :param auto_offset_reset: auto_offset_reset参数
    :return: 配置内容
    """
    config = build_puller_kafka_config_param(
        data_id,
        task_name,
        source_master,
        source_group,
        source_topic,
        dest_topic,
        max_tasks,
        auto_offset_reset,
    )
    config["configs"]["useSasl"] = True
    config["configs"]["securityProtocol"] = security_protocol
    config["configs"]["mechanism"] = sasl_mechanism
    config["configs"]["user"] = user
    config["configs"]["password"] = password
    return config


"""
shipper config
"""


def build_es_config_param(
    cluster_name,
    task_name,
    rt_id,
    task_num,
    channel_topic,
    table_name,
    index_prefix,
    hosts,
    http_port,
    transport_port,
    es_cluster_name,
    es_version,
    enable_auth,
    user,
    password,
):
    """
    es参数构建
    :param cluster_name: 集群名
    :param task_name: 任务名
    :param rt_id: rt_id
    :param task_num: 任务数
    :param channel_topic: 来源topic
    :param table_name: 表名
    :param hosts: es的host
    :param http_port: es的port
    :param transport_port: es transport的port
    :param es_cluster_name: es集群名称
    :param es_version es集群版本
    :param enable_auth 是否启用验证
    :param user: 用户名
    :param password: 密码, 加密过的
    :return: 参数
    """
    return build_eslog_config_param(
        cluster_name,
        task_name,
        rt_id,
        task_num,
        channel_topic,
        table_name,
        hosts,
        http_port,
        transport_port,
        es_cluster_name,
        es_version,
        enable_auth,
        user,
        password,
        "avro",
    )


def build_hdfs_config_param(
    group_id,
    task_name,
    rt_id,
    tasks,
    input_topic,
    flush_size,
    rotate_interval_ms,
    topic_dir,
    hdfs_custom_conf,
    hdfs_url,
    log_dir,
    biz_id,
    table_name,
):
    """
    构建hdfs配置参数
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "cluster": group_id,
        "hdfsUrl": hdfs_url,
        "hdfsConfDir": "",
        "topicsDir": topic_dir,
        "logsDir": "%s/hdfs/" % log_dir,
        "hdfsCustomProperty": hdfs_custom_conf,
        "bizId": biz_id,
        "tableName": table_name,
        "flushSize": flush_size,
        "rotateIntervalMs": rotate_interval_ms,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, input_topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_hdfs_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_tspider_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    connection_url,
    connection_user,
    connection_pass,
    table_name,
):
    """
    tspider connector的配置参数
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "connUrl": connection_url,
        "connUser": connection_user,
        "connPass": connection_pass,
        "tableName": table_name,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_tspider_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_postgresql_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    connection_url,
    connection_user,
    connection_pass,
    schema,
    table_name,
):
    """
    postgresql connector的配置参数
    :param group_id:
    :param rt_id: rt_id
    :param topic: 来源topic
    :param tasks: 任务数
    :param connection_url: postgresql url
    :param connection_user: 用户名
    :param connection_pass: 密码
    :param schema: 表结构
    :param table_name: 物理表名
    :return:
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "connUrl": connection_url,
        "connUser": connection_user,
        "connPass": connection_pass,
        "tableName": table_name,
        "schemaName": schema,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_postgresql_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_hermes_config_param(group_id, task_name, rt_id, topic, tasks, tdbank_cluster_id, bid, table_name):
    """
    生成tdbank任务配置
    :param group_id: 集群名
    :param rt_id: rt_id
    :param topic: 数据源topic
    :param tasks: 消费并发数
    :param master: tdbank数据接收地址
    :param bid: tdbank bid
    :param table_name: tdbank配置接口名
    :param kv_splitter: kv对分割符，如get请求中的 &
    :param field_splliter: 域分隔符，如get请求中的 =
    :return: tdbank任务配置
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "bidId": bid,
        "tableName": table_name,
        "tdbankType": "HERMES",
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_tdbank_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_tredis_config_param(topic, tasks, task_config):
    """
    tredis connector的配置信息
    """
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_tredis_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_tredis_list_config_param(group_id, task_name, rt_id, topic, tasks, redis_dns, redis_port, redis_auth):
    """
    tredis connector的配置信息
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "redisDns": redis_dns,
        "redisPort": redis_port,
        "redisAuth": redis_auth,
        "storageType": "list",
        "topic": topic,
    }
    return build_tredis_config_param(topic, tasks, task_config)


def build_tredis_publish_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    redis_dns,
    redis_port,
    redis_auth,
    storage_channel,
):
    """
    tredis connector的配置信息
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "redisDns": redis_dns,
        "redisPort": redis_port,
        "redisAuth": redis_auth,
        "storageType": "publish",
        "channel": storage_channel,
        "topic": topic,
    }
    return build_tredis_config_param(topic, tasks, task_config)


def build_tredis_join_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    redis_dns,
    redis_port,
    redis_auth,
    storage_keys,
    storage_separator,
    storage_key_separator,
    storage_expire_days,
):
    """
    tredis connector的配置信息
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "redisDns": redis_dns,
        "redisPort": redis_port,
        "redisAuth": redis_auth,
        "storageType": "join",
        "storageKeys": storage_keys,
        "storageSeparator": storage_separator,
        "storageKeySeparator": storage_key_separator,
        "expireDays": storage_expire_days,
        "topic": topic,
    }
    return build_tredis_config_param(topic, tasks, task_config)


def build_tredis_kv_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    redis_dns,
    redis_port,
    redis_auth,
    storage_keys,
    storage_values,
    storage_separator,
    storage_prefix,
    storage_expire_days,
    setnx,
):
    """
    tredis connector的配置信息
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "redisDns": redis_dns,
        "redisPort": redis_port,
        "redisAuth": redis_auth,
        "storageType": "kv",
        "storageKeys": storage_keys,
        "storageValues": storage_values,
        "storageSeparator": storage_separator,
        "expireDays": storage_expire_days,
        "storageKeyPrefix": storage_prefix,
        "storageValueSetnx": setnx,
        "topic": topic,
    }
    return build_tredis_config_param(topic, tasks, task_config)


def build_tsdb_config_param(group_id, rt_id, topic, tasks, db_name, table_name, tsdb_url, tsdb_user, tsdb_pass):
    pass


def build_druid_config_param(group_id, task_name, rt_id, topic, tasks, zk_addr, ts_strategy, version, table_name):
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "tableName": table_name,
        "druidVersion": version,
        "zookeeperConnect": zk_addr,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_druid_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_clickhouse_config_param(
    group_id,
    task_name,
    topic,
    task_num,
    rt_id,
    db_name,
    replicated_table,
    clickhouse_properties,
    clickhouse_col_order,
):
    """
    clickhouse pulsar connector的配置
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "dbName": db_name,
        "replicatedTable": replicated_table,
        "clickhouseProperties": clickhouse_properties,
        "clickhouseColumnOrder": clickhouse_col_order,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": task_num,
        "archive": "builtin://databus_clickhouse_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_queue_config_param(group_id, task_name, rt_id, topic, tasks, dest_kafka, topic_prefix, sasl_config):
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "bootstrapServers": dest_kafka,
        "topicPrefix": topic_prefix,
        "queueType": "KAFKA",
        "useSasl": sasl_config["use.sasl"],
        "saslUser": sasl_config["sasl.user"],
        "saslPass": sasl_config["sasl.pass"],
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_queue_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_queue_pulsar_config_param(group_id, task_name, rt_id, topic, tasks, dest_pulsar, dest_topic):
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "bootstrapServers": dest_pulsar,
        "destTopic": dest_topic,
        "queueType": "PULSAR",
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_queue_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_tdbank_config_param(
    group_id,
    task_name,
    rt_id,
    topic,
    tasks,
    master,
    bid,
    table_name,
    kv_splitter,
    field_splliter,
):
    """
    生成tdbank任务配置
    :param group_id: 集群名
    :param rt_id: rt_id
    :param topic: 数据源topic
    :param tasks: 消费并发数
    :param master: tdbank数据接收地址
    :param bid: tdbank bid
    :param table_name: tdbank配置接口名
    :param kv_splitter: kv对分割符，如get请求中的 &
    :param field_splliter: 域分隔符，如get请求中的 =
    :return: tdbank任务配置
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "tdManageIp": master,
        "bidId": bid,
        "tableName": table_name,
        "fieldSplitter": field_splliter,
        "kvSplitter": kv_splitter,
        "tdbankType": "TDBANK",
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_tdbank_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_ignite_config_param(
    group_id,
    task_name,
    rt_id,
    key_fields,
    key_separator,
    max_records,
    cache_name,
    cluster_name,
    host,
    password,
    port,
    user,
    use_thin_client,
    task_num,
):
    """
    生成ignite任务配置
    :param group_id: 集群名
    :param rt_id: rt_id
    :param key_fields: 构成主键的字段列表，按照顺序，逗号分隔。即storage_config中的key_fields的内容
    :param key_separator: 串联字段成为主键的分隔符。即storage_config中的key_separator部分，默认值为空字符串
    :param max_records: 缓存最大允许数据量
    :param cache_name: 写入ignite中缓存的名称， 即rt的ignite存储的物理表名中<schema>.<table_name>的table_name部分
    :param cluster_name: ignite集群名
    :param host: ignite集群的host
    :param password: 密码
    :param port: 端口
    :param user: 用户名
    :return: tdbank任务配置
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": "avro",
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "keyFields": key_fields,
        "keySeparator": key_separator,
        "igniteCache": cache_name,
        "igniteMaxRecords": max_records,
        "igniteCluster": cluster_name,
        "igniteHost": host,
        "ignitePass": password,
        "ignitePort": port,
        "igniteUser": user,
        "useThinClient": use_thin_client,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, "table_%s" % rt_id)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": task_num,
        "archive": "builtin://databus_ignite_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_eslog_config_param(
    cluster_name,
    task_name,
    rt_id,
    tasks,
    topic,
    table_name,
    hosts,
    http_port,
    transport,
    es_cluster_name,
    es_version,
    enable_auth,
    user,
    password,
    msg_type="etl",
):
    """
    es参数构建
    :param cluster_name: 集群名
    :param task_name: 任务名
    :param rt_id: rt_id
    :param tasks: 任务数
    :param topic: 来源topic
    :param table_name: 表名
    :param hosts: es的host
    :param http_port: es的port
    :param transport: es transport的port
    :param es_cluster_name: es集群名称
    :param es_version es集群版本
    :param enable_auth 是否启用验证
    :param user: 用户名
    :param password: 密码, 加密过的
    :param msg_type: 消息的格式
    :return: 参数
    """
    task_config = {
        "connector": task_name,
        "rtId": rt_id,
        "msgType": msg_type,
        "converterClass": "org.apache.kafka.connect.storage.StringConverter",
        "typeName": table_name,
        "esClusterName": es_cluster_name,
        "esHost": hosts,
        "esHttpPort": http_port,
        "version": es_version,
        "enableAuth": enable_auth,
        "enablePlaintextPwd": False,  # 当前都是加密后的密码
        "authUser": user,
        "authPassword": password,
    }
    consumer_config = {
        "schemaType": None,
        "serdeClassName": None,
        "regexPattern": False,
        "receiverQueueSize": 500,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "sourceSubscriptionPosition": "Earliest",
        "inputs": [pulsar_topic],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": tasks,
        "archive": "builtin://databus_es_shipper",
        "configs": task_config,
        "inputSpecs": {pulsar_topic: consumer_config},
    }


def build_puller_datanode_config_param(group_id, rt_id, tasks, source_rt_ids, node_type, config):
    pass


def build_puller_jdbc_config_param(
    data_id,
    cluster_name,
    task_name,
    resource_info,
    topic,
    channel_host,
    encoding,
    max_tasks=1,
):
    """
    构建jdbc采集配置
    :param data_id: dataid
    :param cluster_name: 集群名，作为group.id参数
    :param task_name: 任务名
    :param resource_info: access_resource model
    :param topic: 目标topic
    :param channel_host: 目标存储地址
    :param encoding: 字符编码
    :param max_tasks: 并发
    :return: 配置
    """
    column = resource_info.increment_field
    collection_type = resource_info.collection_type  # pri, time, all
    # DB时间类型 和 Unix Time Stamp(mins), Unix Time Stamp(seconds), Unix Time Stamp(mins)
    time_format = resource_info.time_format
    # 根据start_at，collection_type 判断 mode 类型
    start_at = resource_info.start_at
    mode = get_mode(collection_type, time_format, start_at)
    period = resource_info.period  # 单位是min
    poll_period_ms = period * 60 * 1000  # 转为ms
    before_time = resource_info.before_time  # 单位 min
    delay_interval_ms = before_time * 60 * 1000  # 转为ms
    resource = json.loads(resource_info.resource)
    url = "jdbc:mysql://{}:{}/{}".format(
        resource["db_host"],
        resource["db_port"],
        resource["db_name"],
    )
    table_name = str(resource["table_name"])

    task_config = {
        "connector": task_name,
        "dataId": "%s" % data_id,
        "rtId": "%s" % data_id,
        "connectionUrl": url,
        "connectionUser": str(resource["db_user"]),
        "connectionPassword": str(resource["db_pass"]),
        "mode": mode,
        "incrementColumnName": column,  # 同时设置自增和时间字段, puller会根据模式自动选择正确的字段
        "timestampColumnName": column,
        "pollInterval": poll_period_ms,
        "tableName": table_name,
        "timestampDelayInterval": delay_interval_ms,
        "timestampTimeFormat": time_format,
        "conditions": resource_info.conditions,
    }
    if "batch_max_rows" in resource:
        task_config["batchMaxRows"] = resource["batch_max_rows"]
    if "record_package_size" in resource:
        task_config["recordPackageSize"] = resource["record_package_size"]
    if encoding == "GBK":
        task_config["charsetName"] = "GB18030"
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "topicName": pulsar_topic,
        "parallelism": max_tasks,
        "archive": "builtin://databus_jdbc_puller",
        "schemaType": "STRING",
        "configs": task_config,
    }


def build_puller_http_config_param(
    task_name, topic, data_id, method, url, time_format, start, end, period, max_tasks=1
):
    """
    http 数据拉取任务配置
    :param task_name: 任务名
    :param topic: 目标topic
    :param data_id: data id
    :param method: GET/POST
    :param url: URL
    :param time_format: 时间格式
    :param start: start字段
    :param end: end字段
    :param period: 拉取周期
    :param max_tasks: 并发
    :return: 配置内容
    """
    task_config = {
        "connector": task_name,
        "rtId": data_id,
        "dataId": "%s" % data_id,
        "httpMethod": method,
        "httpUrl": url,
        "timeFormat": time_format,
        "startTimeField": start,
        "endTimeField": end,
        "periodSecond": period,
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, topic)
    return {
        "topicName": pulsar_topic,
        "parallelism": max_tasks,
        "archive": "builtin://databus_http_puller",
        "schemaType": "STRING",
        "configs": task_config,
    }


def build_transport_hdfs_ck_param(
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
):
    """
    :param parallelism: 迁移线程的并发度
    :param data_type: hdfs物理存储的数据格式
    :param iceberg_table: iceberg表名
    :param iceberg_db: iceberg表的库名
    :param connector: 任务名
    :param source_rt_id: 源rt_id
    :param hdfs_conf: hdfs集群配置
    :param data_dir: hdfs文件数据目录
    :param geog_area: hdfs标签信息
    :param sink_rt_id: 目标rt_id
    :param ck_conn_info: ck连接信息
    :param db_name: ck物理库名
    :param replicated_table: ck本地复制表名
    :param clickhouse_col_order: replicated_table的字段顺序与类型信息
    :return: transport任务配置
    """

    source_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        DATA_DIR_JAVA: data_dir,
        HDFS_PROPERTY_JAVA: json.dumps(hdfs_conf),
        DATA_TYPE_JAVA: data_type,
        ICEBERG_DATABASE_JAVA: iceberg_db,
        ICEBERG_TABLE_JAVA: iceberg_table,
    }

    sink_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: sink_rt_id,
        CLICKHOUSE_PROPERTIES_JAVA: ck_conn_info,
        DB_NAME_JAVA: db_name,
        REPLICATED_TABLE_JAVA: replicated_table,
        CLICKHOUSE_COLUMN_ORDER_JAVA: clickhouse_col_order,
    }

    task_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        MSG_TYPE_JAVA: AVRO,
        SINK_CONFIG_JAVA: sink_config,
        SOURCE_CONFIG_JAVA: source_config,
    }
    return {
        PARALLELISM: parallelism,
        ARCHIVE: "builtin://databus_transport_hdfs_clickhouse",
        CONFIGS: task_config,
    }


def build_transport_hdfs_iceberg_param(
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
    sink_hdfs_custom_property,
    parallelism,
):
    """
    :param parallelism: 迁移线程的并发度
    :param source_data_type: hdfs物理存储的数据格式
    :param source_iceberg_table: iceberg表名
    :param source_iceberg_db: iceberg表的库名
    :param sink_iceberg_table: iceberg表名
    :param sink_iceberg_db: iceberg表的库名
    :param connector: 任务名
    :param source_rt_id: 源rt_id
    :param source_hdfs_conf: hdfs集群配置
    :param source_data_dir: hdfs文件数据目录
    :param sink_rt_id: 目标rt_id
    :return: transport任务配置
    """

    source_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        DATA_DIR_JAVA: source_data_dir,
        HDFS_PROPERTY_JAVA: json.dumps(source_hdfs_conf),
        DATA_TYPE_JAVA: source_data_type,
        ICEBERG_DATABASE_JAVA: source_iceberg_db,
        ICEBERG_TABLE_JAVA: source_iceberg_table,
    }

    sink_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: sink_rt_id,
        DB_NAME_JAVA: sink_iceberg_db,
        TABLENAME: sink_iceberg_table,
        FLUSH_SIZE_JAVA: sink_hdfs_custom_property[FLUSH_SIZE],
        INTERVAL: sink_hdfs_custom_property[INTERVAL],
        HDFS_CUSTOM_PROPERTY_JAVA: json.dumps(sink_hdfs_custom_property),
    }

    task_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        MSG_TYPE_JAVA: AVRO,
        SINK_CONFIG_JAVA: sink_config,
        SOURCE_CONFIG_JAVA: source_config,
    }

    return {
        PARALLELISM: parallelism,
        ARCHIVE: "builtin://databus_transport_hdfs_iceberg",
        CONFIGS: task_config,
    }


def build_transport_hdfs_ignite_param(
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
):
    """
    :param parallelism: 迁移线程的并发度
    :param source_data_type: hdfs物理存储的数据格式
    :param source_iceberg_table: iceberg表名
    :param source_iceberg_db: iceberg表的库名
    :param sink_iceberg_table: iceberg表名
    :param sink_iceberg_db: iceberg表的库名
    :param connector: 任务名
    :param source_rt_id: 源rt_id
    :param source_hdfs_conf: hdfs集群配置
    :param source_data_dir: hdfs文件数据目录
    :param sink_rt_id: 目标rt_id
    :return: transport任务配置
    """

    source_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        DATA_DIR_JAVA: source_data_dir,
        HDFS_PROPERTY_JAVA: json.dumps(source_hdfs_conf),
        DATA_TYPE_JAVA: source_data_type,
        ICEBERG_DATABASE_JAVA: source_iceberg_db,
        ICEBERG_TABLE_JAVA: source_iceberg_table,
    }

    sink_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: sink_rt_id,
        KEY_FIELDS_JAVA: key_fields,
        KEY_SEPARATOR_JAVA: key_separator,
        IGNITE_CACHE_JAVA: cache_name,
        IGNITE_MAX_RECORDS_JAVA: max_records,
        IGNITE_CLUSTER_JAVA: cluster_name,
        IGNITE_HOST_JAVA: host,
        IGNITE_PASS_JAVA: password,
        IGNITE_PORT_JAVA: port,
        IGNITE_USER_JAVA: user,
        USE_THIN_CLIENT_JAVA: use_thin_client,
    }

    task_config = {
        CONNECTOR: connector,
        RT_ID_JAVA: source_rt_id,
        MSG_TYPE_JAVA: AVRO,
        SINK_CONFIG_JAVA: sink_config,
        SOURCE_CONFIG_JAVA: source_config,
    }

    return {
        PARALLELISM: parallelism,
        ARCHIVE: "builtin://databus_transport_hdfs_ignite",
        CONFIGS: task_config,
    }


def build_puller_bkhdfs_config_param(parallelism=1):
    """
    http 数据拉取任务配置
    :param parallelism: 并发
    :return: 配置内容
    """
    task_config = {
        "connector": "puller-bkhdfs",
        "msgBatchRecords": "200",
    }
    tenant = settings.PULSAR_OUTER_TENANT
    namespace = settings.PULSAR_OUTER_NAMESPACE
    pulsar_topic = "persistent://{}/{}/{}".format(tenant, namespace, "bkhdfs_puller_temp")
    return {
        "topicName": pulsar_topic,
        "parallelism": parallelism,
        "archive": "builtin://databus_bkhdfs_puller",
        "schemaType": "STRING",
        "configs": task_config,
    }


def build_shipper_tcaplus_config_param(
    cluster,
    rt_id,
    task_num,
    tcaplus_app_id,
    tcaplus_zone_id,
    tcaplus_token_id,
    tcaplus_address_list,
    tcaplus_storage_keys,
    tcaplus_table_name,
    topic,
):
    """
    构造jdbc采集任务参数
    :param cluster: 集群名，作为group.id参数
    :param channel_topic: 上游topic name
    :param rt_id: rt_id
    :param task_num: 任务并行度
    :param tcaplus_app_id: tcaplus_app_id
    :param tcaplus_zone_id: tcaplus分区地址
    :param tcaplus_token_id: tcaplus读写token
    :param tcaplus_address_list: tcaplus地址列表
    :param tcaplus_storage_keys: 用逗号分隔的tcaplus key
    :param tcaplus_table_name: tcaplus表名
    """
    # TODO pulsar侧的tcaplus暂不支持
    config = {
        "group.id": cluster,
        "connector.class": "com.tencent.bkdata.connect.tcaplus.sink.TcaplusSinkConnector",
        "rt.id": rt_id,
        "topics": topic,
        "tasks.max": "%d" % task_num,
        "tcaplus.app.id": tcaplus_app_id,
        "tcaplus.zone.id": tcaplus_zone_id,
        "tcaplus.token.id": tcaplus_token_id,
        "tcaplus.address.list": tcaplus_address_list,
        "tcaplus.storage.keys": tcaplus_storage_keys,
        "tcaplus.table.name": tcaplus_table_name,
    }
    return config
