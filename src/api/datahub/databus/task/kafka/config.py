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
    AUTO_OFFSET_RESET,
    FLUSH_SIZE,
    FS_DEFAULTFS,
    GROUP,
    INTERVAL,
    LATEST,
    LOG_DIR,
    TOPIC_DIR,
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
    if username:
        auth = {
            "auth.tbds.username": username,
            "auth.tbds.secure.id": secure_id,
            "auth.tbds.secure.key": secure_key,
        }
    else:
        auth = {"hadoop.job.ugi": hadoop_job_ugi}
    config = {
        "group.id": cluster_name,
        "data.id": "%s" % data_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.source.hdfs.TdwHdfsSourceConnector",
        "tasks.max": "1",
        "hdfs.conf.dir": dataapi_settings.HDFS_DEFAULT_KAFKA_CONF_DIR,
        "fs.default.name": fs_default_name,
        "hdfs.data.dir": hdfs_data_dir,
        "dest.kafka.bs": kafka_bs,
        "dest.topic": topic,
    }
    config.update(auth)
    return config


def build_puller_tdbank_config_param(
    group_id,
    connector,
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
    return {
        "group.id": group_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tdbank.source.TdbankSourceConnector",
        "tasks.max": "%d" % max_tasks,
        "msg.process.num": "%s" % settings.TUBE_TASK_PROCESSOR_NUM,
        "tube.group": tube_group,
        "tube.master": tube_master,
        "tube.topic": tube_topic,
        "tube.mixed": tube_mixed,
        "msg.splitter": tube_splitter,
        "msg.tid.key": tid_key,
    }


def build_clean_kafka_conf(rt_id, cluster, data_id, kafka_topic, task_num, target_kafka):
    """
    生成kafka清洗任务配置
    :param rt_id: result_table_id
    :param cluster: 执行集群名称
    :param data_id: 对应access_raw_data中id
    :param kafka_topic: 来源kafka topic
    :param task_num: 并发任务数
    :param target_kafka: 目标kafka集群名称
    :return: {dict} 配置信息
    """
    return {
        "group.id": cluster,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.clean.connect.TransformSinkConnector",
        "tasks.max": "%s" % task_num,
        "topics": kafka_topic,
        "db.dataid": "%s" % data_id,
        "producer.bootstrap.servers": target_kafka,
        "producer.request.timeout.ms": "60000",
        "producer.retries": "5",
        "producer.max.block.ms": "60000",
        "producer.acks": "1",
        "producer.max.in.flight.requests.per.connection": "5",
        "producer.records.in.msg": "100",
    }


def build_puller_kafka_config_param(param):
    """
    生成kafka任务配置
    :param param: 配置参数
    :return: {dict} 配置信息
    """
    config = {
        "group.id": param["cluster_name"],
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.source.kafka.KafkaSourceConnector",
        "data.id": "%s" % param["data_id"],
        "source.group.id": param[GROUP],
        "source.topic": param["src_topic"],
        "source.bootstrap.servers": param["src_server"],
        "source.topic.whitelist": param["src_topic"],
        "source.auto.offset.reset": param.get(AUTO_OFFSET_RESET, LATEST),
        "destination.topic": param["dest_topic"],
        "dest.kafka.bs": param["dest_server"],
        "tasks.max": param["max_tasks"],
    }
    if "security_protocol" in param:
        config["source.security.protocol"] = param["security_protocol"]
        config["source.sasl.mechanism"] = param["sasl_mechanism"]
        config["source.sasl.jaas.user"] = param["user"]
        config["source.sasl.jaas.password"] = param["password"]
    return config


"""
shipper config
"""


def build_es_config_param(
    group_id,
    task_name,
    rt_id,
    tasks,
    topic,
    type_name,
    index_prefix,
    hosts,
    http_port,
    transport,
    es_cluster_name,
    es_version,
    enable_auth,
    user,
    password,
):
    """
    es参数构建
    :param group_id:
    :param rt_id: rt_id
    :param tasks: 任务数
    :param topic: 来源topic
    :param type_name: 表名
    :param index_prefix: 索引前缀
    :param hosts: es的host
    :param http_port: es的port
    :param transport: es transport的port
    :param es_cluster_name: es集群名称
    :param es_version es集群版本
    :return: 参数
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "topics": topic,
        "type.name": type_name,
        "tasks.max": "%s" % tasks,
        "es.index.prefix": index_prefix.lower(),
        "es.cluster.name": es_cluster_name,
        "es.cluster.version": es_version,
        "es.hosts": hosts,
        "es.transport.port": transport,
        "es.host": hosts,
        "es.http.port": http_port,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.sink.es.EsSinkConnector",
        "flush.timeout.ms": "10000",
        "batch.size": "10000",
        "max.in.flight.requests": "5",
        "retry.backoff.ms": "5000",
        "max.retry": "5",
        "es.cluster.enable.auth": enable_auth,
        "es.cluster.enable.PlaintextPwd": False,  # 当前都是加密后的密码
        "es.cluster.username": user,
        "es.cluster.password": password,
    }


def build_hdfs_config_param(group_id, rt_id, tasks, input_topic, conf, biz_id, table_name):
    """
    构建hdfs配置参数
    """
    return {
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.hdfs.BizHdfsSinkConnector",
        "rt.id": rt_id,
        "tasks.max": "%s" % tasks,
        "topics": input_topic,
        "topics.dir": conf[TOPIC_DIR],
        "group.id": group_id,
        "logs.dir": "%s/hdfs/" % conf[LOG_DIR],
        "partitioner.class": "com.tencent.bk.base.datahub.databus.connect.hdfs.BizDataPartitioner",
        "hdfs.url": conf[FS_DEFAULTFS],
        "hadoop.conf.dir": "",
        "hdfs.custom.property": json.dumps(conf),
        "flush.size": "%s" % conf[FLUSH_SIZE],
        "biz.id": "%s" % biz_id,
        "table.name": table_name,
        "rotate.interval.ms": conf[INTERVAL],
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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tspider.TspiderSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "connection.url": connection_url,
        "connection.user": connection_user,
        "connection.password": connection_pass,
        "table.name": table_name,
        "max.delay.days": 7,
        "flush.interval": "10000",
        "batch.size": "1000",
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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.postgresql.PostgresqlSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "connection.url": connection_url,
        "connection.user": connection_user,
        "connection.password": connection_pass,
        "schema.name": schema,
        "table.name": table_name,
        "max.delay.days": 7,
        "flush.interval": "10000",
        "batch.size": "1000",
    }


def build_hermes_config_param(group_id, task_name, rt_id, topic, tasks, tdbank_cluster_id, bid, table_name):
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.hermes.HermesSinkConnector",
        "tasks.max": "%s" % tasks,
        "tdbank.cluster.id": "%s" % tdbank_cluster_id,
        "tdbank.bid": bid,
        "table.name": table_name,
        "topics": topic,
    }


def build_tredis_list_config_param(group_id, task_name, rt_id, topic, tasks, redis_dns, redis_port, redis_auth):
    """
    tredis connector的配置信息
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tredis.TredisSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "redis.dns": redis_dns,
        "redis.port": redis_port,
        "redis.auth": redis_auth,
        "storage.type": "list",
    }


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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tredis.TredisSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "redis.dns": redis_dns,
        "redis.port": redis_port,
        "redis.auth": redis_auth,
        "storage.type": "publish",
        "storage.channel": storage_channel,
    }


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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tredis.TredisSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "redis.dns": redis_dns,
        "redis.port": redis_port,
        "redis.auth": redis_auth,
        "storage.type": "join",
        "storage.keys": storage_keys,
        "storage.separator": storage_separator,
        "storage.key.separator": storage_key_separator,
        "storage.expire.days": storage_expire_days,
    }


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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tredis.TredisSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "redis.dns": redis_dns,
        "redis.port": redis_port,
        "redis.auth": redis_auth,
        "storage.type": "kv",
        "storage.keys": storage_keys,
        "storage.values": storage_values,
        "storage.separator": storage_separator,
        "storage.expire.days": storage_expire_days,
        "storage.key.prefix": storage_prefix,
        "storage.value.setnx": setnx,
    }


def build_tsdb_config_param(group_id, rt_id, topic, tasks, db_name, table_name, tsdb_url, tsdb_user, tsdb_pass):
    """
    tsdb connector的配置参数
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.influxdb.InfluxdbSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "influx.db": db_name,
        "influx.table": table_name,
        "tsdb.url": tsdb_url,
        "tsdb.user": tsdb_user,
        "tsdb.pass": tsdb_pass,
        "is.system.metric": "false",
    }


def build_druid_config_param(group_id, task_name, rt_id, topic, tasks, zk_addr, ts_strategy, version, table_name):
    """
    druid connector的配置
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.druid.DruidSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "table.name": table_name,
        "druid.cluster.version": version,
        "zookeeper.connect": "%s" % zk_addr,
        "timestamp.strategy": "%s" % ts_strategy,
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
    clickhouse connector的配置
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.sink.clickhouse.ClickHouseSinkConnector",
        "tasks.max": "%s" % task_num,
        "topics": topic,
        "connector": task_name,
        "db.name": db_name,
        "replicated.table": replicated_table,
        "clickhouse.properties": clickhouse_properties,
        "clickhouse.column.order": clickhouse_col_order,
    }


def build_queue_config_param(group_id, task_name, rt_id, topic, tasks, dest_kafka, topic_prefix, sasl_config):
    """
    queue connector的配置参数
    :param group_id: 集群名
    :param rt_id: rt_id
    :param topic: 数据源topic
    :param tasks: 消费并发数
    :param dest_kafka: 目标kafka
    :param topic_prefix: topic前缀
    :param sasl_config:
        {
            "use.sasl": True/False,
            "sasl.user": "",
            "sasl.pass": "",
        }
    :return: 配置
    """
    config = {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.queue.QueueSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "dest.topic.prefix": topic_prefix,
        "producer.bootstrap.servers": dest_kafka,
    }
    config.update(sasl_config)
    return config


def build_queue_pulsar_config_param(group_id, task_name, rt_id, topic, tasks, dest_pulsar, dest_topic):
    """
    pulsar connector的配置参数
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.pulsar.PulsarSinkConnector",
        "tasks.max": "%s" % tasks,
        "topics": topic,
        "pulsar.url": dest_pulsar,
        "dest.topic": dest_topic,
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
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tdbank.sink.TdbankSinkConnector",
        "tasks.max": "%s" % tasks,
        "tdbank.td_manage.ip": master,
        "tdbank.bid": bid,
        "table.name": table_name,
        "topics": topic,
        "msg.field.splitter": field_splliter,
        "msg.kv.splitter": kv_splitter,
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
    :param task_num: 任务数
    :param group_id: 集群名
    :param rt_id: rt_id
    :param key_fields: 构成主键的字段列表，按照顺序，逗号分隔。即storage_config中的key_fields的内容
    :param key_separator: 串联字段成为主键的分隔符。即storage_config中的key_separator部分，默认值为空字符串
    :param max_records: 缓存最大允许数据量
    :param use_thin_client: 是否采用thin_client模式
    :param cache_name: 写入ignite中缓存的名称， 即rt的ignite存储的物理表名中<schema>.<table_name>的table_name部分
    :param cluster_name: ignite集群名
    :param host: ignite集群的host
    :param password: 密码
    :param port: 端口
    :param user: 用户名
    :return: tdbank任务配置
    """
    return {
        "cache.key.fields": key_fields,
        "cache.key.separator": key_separator,
        "cache.name": cache_name,
        "cache.max.records": max_records,
        "use.thin.client": use_thin_client,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.sink.ignite.IgniteSinkConnector",
        "group.id": group_id,
        "ignite.cluster": cluster_name,
        "ignite.host": host,
        "ignite.pass": password,
        "ignite.port": port,
        "ignite.user": user,
        "name": "ignite-table_%s" % rt_id,
        "rt.id": rt_id,
        "tasks.max": "%s" % task_num,
        "topics": "table_%s" % rt_id,
    }


def build_hdfs_iceberg_config_param(group_id, rt_id, topic, task_num, physical_table_name, conf):
    """
    生成hdfs存储的iceberg任务配置
    :param group_id: 集群名
    :param rt_id: 结果表id
    :param topic: 来源topic
    :param task_num: 任务数
    :param physical_table_name: iceberg物理表名
    :param conf: hdfs配置
    :return: iceberg任务配置
    """
    db_name, table_name = (
        physical_table_name.split(".")[0],
        physical_table_name.split(".")[1],
    )
    return {
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.sink.iceberg",
        "group.id": group_id,
        "name": "iceberg-table_%s" % rt_id,
        "rt.id": rt_id,
        "tasks.max": "%s" % task_num,
        "topics": topic,
        "db.name": db_name,
        "table.name": table_name,
        "hdfs.custom.property": json.dumps(conf),
        "cluster.type": "hdfs",
        "flush.size": conf[FLUSH_SIZE],
        "interval": conf[INTERVAL],
    }


def build_eslog_config_param(
    group_id,
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
):
    """
    es参数构建
    :param group_id: 集群名
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
    :return: 参数
    """
    return {
        "group.id": group_id,
        "rt.id": rt_id,
        "topics": topic,
        "type.name": table_name,
        "tasks.max": "%s" % tasks,
        "es.index.prefix": table_name.lower(),
        "es.cluster.name": es_cluster_name,
        "es.cluster.version": es_version,
        "es.hosts": hosts,
        "es.transport.port": transport,
        "es.host": hosts,
        "es.http.port": http_port,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.sink.es.EsSinkConnector",
        "flush.timeout.ms": "10000",
        "batch.size": "10000",
        "max.in.flight.requests": "5",
        "retry.backoff.ms": "5000",
        "max.retry": "5",
        "es.cluster.enable.auth": enable_auth,
        "es.cluster.enable.PlaintextPwd": False,  # 当前都是加密后的密码
        "es.cluster.username": user,
        "es.cluster.password": password,
    }


def build_puller_datanode_config_param(group_id, rt_id, tasks, source_rt_ids, node_type, config):
    """
    固化节点任务配置
    """
    connector_class = {
        "merge": "com.tencent.bk.base.datahub.databus.connect.source.datanode.MergeSourceConnector",
        "split": "com.tencent.bk.base.datahub.databus.connect.source.datanode.SplitBySourceConnector",
        "filter": "com.tencent.bk.base.datahub.databus.connect.source.datanode.FilterBySourceConnector",
    }
    return {
        "group.id": group_id,
        "rt.id": "%s" % rt_id,
        "connector.class": connector_class[node_type],
        "tasks.max": "%s" % tasks,
        "source.rt.list": "%s" % source_rt_ids,
        "config": "%s" % config,
    }


def build_puller_jdbc_config_param(
    data_id,
    cluster_name,
    connector,
    resource_info,
    topic,
    channel_host,
    encoding,
    max_tasks=1,
):
    """
    构造jdbc采集任务参数
    :param data_id: data_id
    :param cluster_name: 集群名，作为group.id参数
    :param connector: 任务名
    :param resource_info: access_resource model
    :param topic: 目标topic
    :param channel_host: 目标存储地址
    :param encoding: 字符编码
    :return: jdbc 配置
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

    config = {
        "group.id": cluster_name,
        "data.id": "%s" % data_id,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.source.jdbc.JdbcSourceConnector",
        "tasks.max": "1",
        "dest.kafka.bs": channel_host,
        "topic": topic,
        "connection.url": url,
        "connection.user": str(resource["db_user"]),
        "connection.password": str(resource["db_pass"]),
        "db.table": table_name,
        "mode": mode,
        "incrementing.column.name": column,  # 同时设置自增和时间字段, puller会根据模式自动选择正确的字段
        "timestamp.column.name": column,
        "timestamp.time.format": time_format,
        "timestamp.delay.interval.ms": delay_interval_ms,
        "poll.interval.ms": poll_period_ms,
        "processing.conditions": resource_info.conditions,
    }
    if "batch_max_rows" in resource:
        config["batch.max.rows"] = resource["batch_max_rows"]
    if "record_package_size" in resource:
        config["record.package.size"] = resource["record_package_size"]
    if encoding == "GBK":
        config["character.encoding"] = "GB18030"
    return config


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
    config = {
        "group.id": cluster,
        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tcaplus.TcaplusSinkConnector",
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
