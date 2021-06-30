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

import contextlib
import json
import threading
import time
import traceback

import kazoo.client
import kazoo.retry
from clickhouse_driver import Client
from common.log import logger
from datahub.common.const import (
    APPEND_FIELDS,
    AVERAGE,
    BAD_FIELDS,
    CAPACITY,
    CHECK_DIFF,
    CHECK_RESULT,
    CK_DEFAULT_CONNECT_TIMEOUT_SEC,
    CK_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
    CLICKHOUSE,
    CLICKHOUSE_FIELDS,
    CLUSTER_NAME,
    CONNECTION_INFO,
    CONSISTENCY,
    COUNT,
    DEFAULT,
    DELETE_FIELDS,
    DISTINCT_PARTITIONS,
    DISTRIBUTED_TABLE,
    ELAPSED,
    EMPTY_STRING,
    ENABLE,
    EXCEPTION,
    EXPIRES,
    EXPRESSION,
    FACTOR,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    FREE_DISK,
    GRANULARITY,
    HOSTS,
    HTTP_PORT,
    INDEX_TYPE,
    INFO,
    INNER_CLUSTER,
    IP,
    MAX,
    MESSAGE,
    MIN,
    NAME,
    NODES,
    ORDER_BY,
    PARTITION_TIME,
    PARTITIONS,
    PHYSICAL_TABLE_NAME,
    PROCESSING_TYPE,
    QUERY,
    QUERY_ID,
    QUERYSET,
    REPLICATED_TABLE,
    REPORT_TIME,
    RESULT_TABLE_ID,
    RT_FIELDS,
    SAMPLE,
    SCHEMAS,
    SIZE,
    SNAPSHOT,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGE_USAGE,
    STORAGES,
    SUM,
    TABLE,
    TABLE_RECORD_NUMS,
    TABLE_SIZE_MB,
    TCP_TGW,
    TIMESTAMP,
    TOP_PARTITIONS,
    TOTAL_PARTITIONS,
    TOTAL_SPACE,
    TOTAL_SUM,
    TOTAL_USAGE,
    TYPE,
    USED_MAX,
    USED_MIN,
    USED_SPACE,
    USED_SUM,
    WEIGHTS,
    ZK_ADDRESS,
)
from datahub.storekit import model_manager, util
from datahub.storekit.exceptions import (
    ClickHouseAddIndexException,
    ClickHouseConfigException,
    ClickHouseDropIndexException,
    ClickHousePrepareException,
    ClickHouseStorageConfigException,
)
from datahub.storekit.settings import (
    ADD_COLUMN_SQL,
    ALTER_INDEX_SQL,
    ALTER_TABLE_SQL,
    ALTER_TTL_SQL,
    CHECK_TABLE_EXIST_SQL,
    CK_NODE_ZK_PATH_FORMAT,
    CK_TB_ZK_PATH_FORMAT,
    CLICKHOUSE_MAINTAIN_TIMEOUT,
    CREATE_DB_SQL,
    CREATE_DISTRIBUTED_TABLE_SQL,
    CREATE_REPLICATED_TABLE_SQL,
    DROP_COLUMN_SQL,
    DROP_INDEX_SQL,
    DROP_TABLE_SQL,
    ENGINE_FULL_SQL,
    FORMAT_READABLE_SQL,
    FUSING_THRESHOLD,
    ORDER_BY_SQL,
    PARTITION_BY_SQL,
    QUERY_ALL_CAPACITY_SQL,
    QUERY_CAPACITY_BYTES_SQL,
    QUERY_CAPACITY_SQL,
    QUERY_CLUSTER_CAPACITY_SQL,
    QUERY_CLUSTER_SQL,
    QUERY_CLUSTER_TOP_PARTITIONS_SQL,
    QUERY_COUNT_SQL,
    QUERY_DISTINCT_PARTITIONS_SQL,
    QUERY_FIELDS_SQL,
    QUERY_PROCESSLIST_SQL,
    QUERY_SAMPLE_SQL,
    QUERY_TOP_PARTITIONS_SQL,
    QUERY_TOTAL_PARTITIONS_SQL,
    RT_TYPE_TO_CLICKHOUSE_MAPPING,
    RTX_RECEIVER,
    SAMPLE_BY_SQL,
    SHOW_SCHEMA_SQL,
    TRUNCATE_TABLE_SQL,
    TTL_BY_SQL,
    ClICKHOUSE_DEFAULT_COLUMNS,
    ClICKHOUSE_EXCEPT_FIELDS,
)
from datahub.storekit.util import translate_expires_day


def initialize(rt_info):
    """
    初始化rt的clickhouse存储
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """
    return prepare(rt_info)


def common_initial(rt_info):
    """
    :param rt_info: rt基础信息
    :return: client clickhouse客户端
             db_name 库名
             distributed_table 分布式表名
             replicated_table 复制表名
             inner_cluster clickhouse集群内部虚拟集群名
    """
    ck = rt_info[STORAGES][CLICKHOUSE]
    conn = json.loads(ck[STORAGE_CLUSTER][CONNECTION_INFO])
    host, port = conn[TCP_TGW].split(":")[0], int(conn[TCP_TGW].split(":")[1])
    client = build_client(host, port)
    physical_tn = ck[PHYSICAL_TABLE_NAME]
    db_name, distributed_table = physical_tn.split(".")[0], physical_tn.split(".")[1]
    replicated_table = f"{distributed_table}_local"
    inner_cluster = conn[INNER_CLUSTER]

    return client, db_name, distributed_table, replicated_table, inner_cluster


def build_client(
    host,
    port,
    database=DEFAULT,
    user=DEFAULT,
    connect_timeout=CK_DEFAULT_CONNECT_TIMEOUT_SEC,
    sync_request_timeout=CK_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
):
    """
    :param host: 主机名或者ip
    :param port: ck server 端口
    :param database: 库名
    :param user: 用户名
    :param connect_timeout: 连接超时
    :param sync_request_timeout: ping请求超时
    :return:
    """
    return Client(
        host=host,
        port=port,
        database=database,
        user=user,
        connect_timeout=connect_timeout,
        sync_request_timeout=sync_request_timeout,
    )


def info(rt_info):
    """
    获取rt的clickhouse存储相关信息：表结构，字段，样例数据，各节点分区信息，数据量信息和表一致性检查
    :param rt_info: rt的字段和配置信息
    :return: rt的clickhouse相关信息
    """
    client, db_name, distributed_table, replicated_table, _ = common_initial(rt_info)
    result = {}
    try:
        # 查看复制表结构
        sql = SHOW_SCHEMA_SQL.format(db_name, replicated_table)
        result[REPLICATED_TABLE] = get_table_schema(client, sql)

        # 表字段信息
        sql = QUERY_FIELDS_SQL.format(db_name, replicated_table)
        result[FIELDS] = [f"{cell[0]}:{cell[1]}" for cell in client.execute(sql)]

        # 查看分布式表结构
        sql = SHOW_SCHEMA_SQL.format(db_name, distributed_table)
        result[DISTRIBUTED_TABLE] = get_table_schema(client, sql)

        # 采样数据
        sql = QUERY_SAMPLE_SQL.format(db_name, distributed_table)
        result[SAMPLE] = client.execute(sql)

        # 行数和空间占用大小
        sql = QUERY_COUNT_SQL.format(db_name, distributed_table)
        result[COUNT] = client.execute(sql)[0][0]

        # 采集表数据量
        result[SIZE] = table_capacity(client, db_name, replicated_table)

        # 采集分区数
        result[PARTITIONS] = table_partitions(client, db_name, replicated_table)

        # 节点间表结构一致性检查
        result[CONSISTENCY] = check_table_consistency(client, db_name, distributed_table, replicated_table)
    except Exception as e:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to get info", exc_info=True)
        result[EXCEPTION] = str(e)
    finally:
        client.disconnect()

    ck = rt_info[STORAGES][CLICKHOUSE]
    ck[INFO] = result
    return ck


def get_table_schema(client, sql):
    """
    :param client: clickhouse客户端
    :param sql: 查表结构的sql
    :return: 表结构字符串
    """
    return client.execute(sql)[0][0].replace("\n", " ")


def get_all_table_capacity(conn):
    """
    读取clickhouse集群中全部local后缀表的数据量信息
    :param conn: 集群链接信息
    :return:
    """
    host, port = conn[TCP_TGW].split(":")[0], int(conn[TCP_TGW].split(":")[1])
    client = build_client(host, port)
    try:
        resultSet = client.execute(QUERY_CLUSTER_SQL)
        rt_size = {}
        for re in resultSet:
            host, port = re[0], int(re[1])
            inner_client = build_client(host, port)
            try:
                size_info = inner_client.execute(QUERY_ALL_CAPACITY_SQL)
                for info in size_info:
                    db, table, total_bytes, total_rows = info[0], info[1], int(info[2]), int(info[3])
                    physical_tn = f"{db}.{table}"
                    if physical_tn in rt_size:
                        rt_size[physical_tn][TABLE_SIZE_MB] += total_bytes / 1024 / 1024
                        rt_size[physical_tn][TABLE_RECORD_NUMS] += total_rows / 2
                    else:
                        rt_size[physical_tn] = {
                            TABLE_SIZE_MB: total_bytes / 1024 / 1024,
                            TABLE_RECORD_NUMS: total_rows / 2,
                            REPORT_TIME: time.time(),
                        }
            finally:
                inner_client.disconnect()
        return rt_size
    finally:
        client.disconnect()


def table_capacity(client, db_name, replicated_table):
    """
    采集表数据量
    :param replicated_table: 本地复制表
    :param client: clickhouse客户端
    :param db_name: 库名
    :return: 指定表在各个server上的数据量信息
    """
    capacity = {}
    capacity_bytes = []
    resultSet = client.execute(QUERY_CLUSTER_SQL)
    for re in resultSet:
        host, port = re[0], int(re[1])
        inner_client = build_client(host, port)
        try:
            sql = QUERY_CAPACITY_SQL.format(db_name, replicated_table)
            instance = f"{host}:{port}"
            capacity[instance] = inner_client.execute(sql)[0][0]
            sql = QUERY_CAPACITY_BYTES_SQL.format(db_name, replicated_table)
            capacity_bytes.append(float(inner_client.execute(sql)[0][0]))
        finally:
            inner_client.disconnect()

    size = len(capacity_bytes)
    capacity_max, capacity_min, capacity_sum, capacity_average = 0, 0, 0, 0
    if size > 0:
        capacity_max = format_readable_size(client, max(capacity_bytes))
        capacity_min = format_readable_size(client, min(capacity_bytes))
        capacity_sum = format_readable_size(client, sum(capacity_bytes))
        capacity_average = format_readable_size(client, sum(capacity_bytes) / size)

    return {CAPACITY: capacity, MAX: capacity_max, MIN: capacity_min, SUM: capacity_sum, AVERAGE: capacity_average}


def format_readable_size(client, size):
    """
    字节转可读格式的容量大小
    :param client: clickhouse客户端
    :param size: 字节大小
    :return: 可读形式的容量大小
    """
    sql = FORMAT_READABLE_SQL.format(size)
    return client.execute(sql)[0][0]


def cluster_processlist(cluster_name):
    """
    在各个节点上取前十个正在执行且耗时最最长的查询
    :param cluster_name: clickhouse集群名
    :return: 正在运行的sql
    """
    client = cluster_common_initial(cluster_name)
    processlist = {}
    try:
        resultSet = client.execute(QUERY_CLUSTER_SQL)
        for re in resultSet:
            host, port = re[0], int(re[1])
            inner_client = build_client(host, port)
            try:
                instance = f"{host}:{port}"
                processlist[instance] = get_processlist(inner_client)
            finally:
                inner_client.disconnect()
    finally:
        client.disconnect()

    return processlist


def top_partitions(cluster_name):
    """
    在各个节点上取前十个parts最多的分区
    :param cluster_name: clickhouse集群名
    :return: 分区信息
    """
    client = cluster_common_initial(cluster_name)
    top_partitions = {}
    try:
        resultSet = client.execute(QUERY_CLUSTER_SQL)
        for result in resultSet:
            host, port = result[0], int(result[1])
            inner_client = build_client(host, port)
            try:
                reSet = inner_client.execute(QUERY_CLUSTER_TOP_PARTITIONS_SQL)
                top_parts = [f"{r[0]} {r[1]} {r[2]}.{r[3]}" for r in reSet]
                instance = f"{host}:{port}"
                top_partitions[instance] = top_parts
            finally:
                inner_client.disconnect()
    finally:
        client.disconnect()

    return top_partitions


def get_processlist(client):
    """
    show processlist
    :param client: clickhouse客户端
    :return: 取节点上前十个正在执行且耗时最最长的查询
    """
    result = []
    query_results = client.execute(QUERY_PROCESSLIST_SQL)
    for re in query_results:
        process = {QUERY_ID: re[0], QUERY: re[1], ELAPSED: re[2]}
        result.append(process)
    return result


def cluster_capacity(cluster_name):
    """
    采集集群容量
    :param cluster_name: 集群名称
    :return: 容量信息
    """
    capacity = {}
    capacity_usage = []
    capacity_used = []
    capacity_total = []
    client = cluster_common_initial(cluster_name)
    try:
        resultSet = client.execute(QUERY_CLUSTER_SQL)
        for re in resultSet:
            host, port = re[0], int(re[1])
            inner_client = build_client(host, port)
            try:
                result = inner_client.execute(QUERY_CLUSTER_CAPACITY_SQL)
                free_space, total_space = int(result[0][0]), int(result[0][1])
                used_space = total_space - free_space
                storage_usage = 100 * used_space / total_space
                capacity_used.append(used_space)
                capacity_total.append(total_space)
                capacity_usage.append(storage_usage)
                instance = f"{host}:{port}"
                capacity[instance] = {
                    USED_SPACE: format_readable_size(inner_client, used_space),
                    TOTAL_SPACE: format_readable_size(inner_client, total_space),
                    STORAGE_USAGE: storage_usage,
                }
            finally:
                inner_client.disconnect()

        size = len(capacity_total)
        used_sum, usage_max, usage_min, total_usage, total_sum = 0, 0, 0, 0, 0
        if size > 0:
            usage_max, usage_min = max(capacity_usage), min(capacity_usage)
            used_sum = sum(capacity_used)
            total_sum = sum(capacity_total)
            total_usage = 100 * sum(capacity_used) / sum(capacity_total)

        return {
            CAPACITY: capacity,
            USED_MAX: usage_max,
            USED_MIN: usage_min,
            USED_SUM: used_sum,
            TOTAL_SUM: total_sum,
            TOTAL_USAGE: total_usage,
            TIMESTAMP: time.time(),
        }
    finally:
        client.disconnect()


def cluster_common_initial(cluster_name):
    """
    生成client客户端
    :param cluster_name: 集群名称
    :return: client客户端
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, CLICKHOUSE)
    conn = json.loads(cluster.connection_info)
    host, port = conn[TCP_TGW].split(":")[0], int(conn[TCP_TGW].split(":")[1])
    return build_client(host, port)


def table_partitions(client, db_name, replicated_table):
    """
    采集表数据量
    :param replicated_table: 本地复制表
    :param client: clickhouse客户端
    :param db_name: 库名
    :return: 指定表在各个server上的分区状态
    """
    partitions = {}
    resultSet = client.execute(QUERY_CLUSTER_SQL)
    for result in resultSet:
        part = {}
        host, port = result[0], int(result[1])
        inner_client = build_client(host, port)
        try:
            sql = QUERY_DISTINCT_PARTITIONS_SQL.format(db_name, replicated_table)
            part[DISTINCT_PARTITIONS] = inner_client.execute(sql)[0][0]
            sql = QUERY_TOTAL_PARTITIONS_SQL.format(db_name, replicated_table)
            part[TOTAL_PARTITIONS] = inner_client.execute(sql)[0][0]
            sql = QUERY_TOP_PARTITIONS_SQL.format(db_name, replicated_table)
            part[TOP_PARTITIONS] = [f"{r[0]} {r[1]} {r[2]}.{r[3]}" for r in inner_client.execute(sql)]
            partitions[f"{host}:{port}"] = part
        finally:
            inner_client.disconnect()

    return partitions


def alter_table(rt_info):
    """
    修改rt的clickhouse存储相关信息： 表结构变更add/drop column，TTL修改
    :param rt_info: rt的字段和配置信息
    :return: rt的clickhouse存储的变更结果
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        # alter table fields
        field_diff = check_schema(rt_info)
        append_fields, delete_fields = field_diff[CHECK_DIFF][APPEND_FIELDS], field_diff[CHECK_DIFF][DELETE_FIELDS]
        if append_fields or delete_fields:
            alter_fields(
                client, db_name, distributed_table, replicated_table, inner_cluster, append_fields, delete_fields
            )

        # alter table ttl
        set_expire(
            client,
            db_name,
            replicated_table,
            inner_cluster,
            rt_info[PROCESSING_TYPE],
            rt_info[STORAGES][CLICKHOUSE][EXPIRES],
        )
        check_tables(client, db_name, distributed_table, replicated_table)
    except Exception as e:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to create table", exc_info=True)
        raise e
    finally:
        client.disconnect()


def set_expire(client, db_name, replicated_table, inner_cluster, processing_type, expire):
    """
    设置table ttl规则
    :param replicated_table: 本地复制表
    :param client: clickhouse客户端
    :param db_name: 库名
    :param inner_cluster: ck集群的内部逻辑集群
    :param processing_type: rt的processing_type类型
    :param expire: 表数据保存周期
    """
    if processing_type in [QUERYSET, SNAPSHOT]:
        return

    expire_days = translate_expires_day(expire)
    if expire_days <= 0:
        return

    ttl_by = TTL_BY_SQL.format(expire_days)
    sql = ENGINE_FULL_SQL.format(db_name, replicated_table)
    engine_full = get_table_schema(client, sql)
    if ttl_by not in engine_full:
        sql = ALTER_TTL_SQL.format(db_name, replicated_table, inner_cluster, expire_days)
        client.execute(sql)


def check_tables(client, db_name, distributed_table, replicated_table):
    """
    各个节点间表校验，包括存在性和一致性
    :param replicated_table: 本地复制表
    :param distributed_table: 全局分布式表
    :param client: clickhouse客户端
    :param db_name: 库名
    :return: True/False
    """
    # 校验是否建表成功且一致
    created = check_table_exists(client, db_name, distributed_table, replicated_table)
    consistency = check_table_consistency(client, db_name, distributed_table, replicated_table)[CONSISTENCY]
    if not (created and consistency):
        raise ClickHousePrepareException(message=f"是否建表成功: {created}, 各表是否一致: {consistency}")


def alter_fields(client, db_name, distributed_table, replicated_table, inner_cluster, append_fields, delete_fields):
    """
    增减字段
    :param replicated_table: 本地复制表
    :param distributed_table: 全局分布式表
    :param client: clickhouse客户端
    :param db_name: 库名
    :param inner_cluster: ck集群的内部逻辑集群
    :param append_fields: 待添加字段
    :param delete_fields: 待删除字段
    """
    adds = [ADD_COLUMN_SQL.format(e) for e in append_fields]
    drops = [DROP_COLUMN_SQL.format(e) for e in delete_fields]
    alter = ", ".join(adds + drops)
    # 变更复制表
    sql = ALTER_TABLE_SQL.format(db_name, replicated_table, inner_cluster, alter)
    client.execute(sql)
    # 变更分布式表
    sql = ALTER_TABLE_SQL.format(db_name, distributed_table, inner_cluster, alter)
    client.execute(sql)


def create_table(rt_info):
    """
    创建clickhouse物理表，包括分布式表和复制表
    建表规则：存在dteventtimestamp字段, 则添加__time字段, 并且基于__time设置ttl和partition by,order by, sample by;
            不存在，则只设置order by, 不设置ttl, sample by, partition by
    :param rt_info: rt的字段和配置信息
    :return: 执行结果
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        rt_id = rt_info[RESULT_TABLE_ID]
        ck = rt_info[STORAGES][CLICKHOUSE]
        storage_config = json.loads(ck[STORAGE_CONFIG])
        processing_type = rt_info[PROCESSING_TYPE]
        rt_fields = rt_info[FIELDS]

        # 建库
        sql = CREATE_DB_SQL.format(db_name, inner_cluster)
        client.execute(sql)

        # 建复制表
        order_list = [e.lower() for e in storage_config.get(ORDER_BY, "")]
        field_name_list = construct_fields(rt_fields, processing_type, True)
        if not order_list or not set(order_list).issubset(field_name_list):
            raise ClickHouseStorageConfigException("order_by必须设置，且字段存在")
        fields = ", ".join(construct_fields(rt_fields, processing_type))
        if processing_type in [QUERYSET, SNAPSHOT]:
            order_by = ORDER_BY_SQL.format(", ".join(order_list))  # 分区内部数据排序和主键
            partition_by = EMPTY_STRING
            sample_by = EMPTY_STRING
            ttl_by = EMPTY_STRING  # 数据保存时间
        else:
            order_list.insert(0, PARTITION_TIME)
            order_by = ORDER_BY_SQL.format(", ".join(order_list))
            partition_by = PARTITION_BY_SQL
            sample_by = SAMPLE_BY_SQL
            ttl_by = TTL_BY_SQL.format(translate_expires_day(ck[EXPIRES]))

        sql = CREATE_REPLICATED_TABLE_SQL.format(
            db_name,
            replicated_table,
            inner_cluster,
            fields,
            db_name,
            replicated_table,
            order_by,
            partition_by,
            sample_by,
            ttl_by,
        )
        logger.info(f"{rt_id}: create clickhouse replicated_table: {sql}")
        client.execute(sql)

        # 建分布式表
        sql = CREATE_DISTRIBUTED_TABLE_SQL.format(
            db_name, distributed_table, inner_cluster, fields, inner_cluster, db_name, replicated_table
        )
        logger.info(f"{rt_id}: create clickhouse distributed_table: {sql}")
        client.execute(sql)

        check_tables(client, db_name, distributed_table, replicated_table)
    except Exception as e:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to create table", exc_info=True)
        raise e
    finally:
        client.disconnect()


def prepare(rt_info):
    """
    准备rt关联的clickhouse存储（创建新库表或旧表新增字段), 用途：
        1）create：任何节点上的分布式表或者复制表不存在，执行后将采用rt配置创建（注：不能处理复制表被改名的情形）;
        2）alter: 如果rt字段发生增减，则ck物理表字段也随之增减；如果数据保存周期变化，则ck物理表ttl随之变更
    :param rt_info: rt的配置信息
    :return: True/False
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        if not check_table_exists(client, db_name, distributed_table, replicated_table):
            create_table(rt_info)
        else:
            alter_table(rt_info)

        # 同步zk
        sync_zk_table_whitelist(rt_info)
        return True
    except Exception as e:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: failed to prepare", exc_info=True)
        raise e
    finally:
        client.disconnect()


def maintain(rt_info):
    """
    维护clickhouse table的数据保存周期
    :param rt_info: rt的配置信息
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        set_expire(
            client,
            db_name,
            replicated_table,
            inner_cluster,
            rt_info[PROCESSING_TYPE],
            rt_info[STORAGES][CLICKHOUSE][EXPIRES],
        )
    except Exception as e:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to maintain", exc_info=True)
        raise e
    finally:
        client.disconnect()


def maintain_all():
    """
    根据用户设定的数据保留时间维护clickhouse表数据保留规则
    :return: True
    """
    start = time.time()
    cluster_list = model_manager.get_storage_cluster_configs_by_type(CLICKHOUSE)
    check_threads = []
    for cluster in cluster_list:
        cluster_name = cluster[CLUSTER_NAME]
        thread = threading.Thread(target=maintain_clickhouse_cluster, name=cluster_name, args=(cluster_name,))
        # 设置线程为守护线程，主线程结束后，结束子线程
        thread.setDaemon(True)
        check_threads.append(thread)
        thread.start()

    for th in check_threads:
        th.join(timeout=CLICKHOUSE_MAINTAIN_TIMEOUT)

    end = time.time()
    logger.info(f"clickhouse maintain_all total time: {(end - start)}(s)")
    return True


def maintain_clickhouse_cluster(cluster_name):
    """
    维护单个clickhouse集群
    :param cluster_name: 集群名
    """
    storage_rt_list = model_manager.get_storage_rt_objs_by_name_type(cluster_name, CLICKHOUSE)
    for rt_storage in storage_rt_list:
        try:
            rt_info = util.get_rt_info(rt_storage.result_table_id)
            maintain(rt_info)
        except Exception as e:
            logger.warning(
                f"{rt_storage.storage_cluster_config.cluster_name}: failed to maintain the retention rule of "
                f"datasource {rt_storage.physical_table_name}, exception: {str(e)}"
            )


def construct_fields(rt_fields, processing_type, only_name=False):
    """
    :param only_name: 是否只返回字段名列表
    :param processing_type: rt的数据来源类型，包括snapshot, clean, batch等
    :param rt_fields: rt的字段信息信息
    :return: 一个字典，包含连接clickhouse所需的配置
    """
    fields = []
    if processing_type in [QUERYSET, SNAPSHOT]:
        for field in rt_fields:
            col_name, col_type = field[FIELD_NAME].lower(), RT_TYPE_TO_CLICKHOUSE_MAPPING[field[FIELD_TYPE].lower()]
            fields.append(f"{col_name} {col_type}")
    else:
        for field in rt_fields:
            col_name, col_type = field[FIELD_NAME].lower(), RT_TYPE_TO_CLICKHOUSE_MAPPING[field[FIELD_TYPE].lower()]
            if col_name not in ClICKHOUSE_EXCEPT_FIELDS:
                fields.append(f"{col_name} {col_type}")
        fields = fields + ClICKHOUSE_DEFAULT_COLUMNS

    if only_name:
        return [e.split(" ")[0] for e in fields]

    return fields


def delete(rt_info, delete_schema=True):
    """
    清空数据, 并删除表结构
    :param delete_schema: 是否删除表结构
    :param rt_info: 结果表
    :return:
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        if delete_schema:
            sql = DROP_TABLE_SQL.format(db_name, distributed_table, inner_cluster)
            client.execute(sql)
            sql = DROP_TABLE_SQL.format(db_name, replicated_table, inner_cluster)
            client.execute(sql)
            return not check_table_exists(client, db_name, distributed_table, replicated_table)
        else:
            sql = TRUNCATE_TABLE_SQL.format(db_name, replicated_table, inner_cluster)
            client.execute(sql)
            return True
    except Exception as e:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: drop or truncate table failed", exc_info=True)
        raise e
    finally:
        client.disconnect()


def add_index(rt_info, params):
    """
    创建跳数索引
    :param rt_info: 结果表信息
    :param params: 参数
    :return:
    """
    index_name, expression, index_type, granularity = (
        params[NAME],
        params[EXPRESSION],
        params[INDEX_TYPE],
        params[GRANULARITY],
    )
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        sql = ALTER_INDEX_SQL.format(
            db_name, replicated_table, inner_cluster, index_name, expression, index_type, granularity
        )
        logger.info(f"{rt_info[RESULT_TABLE_ID]}: going to add index, sql: {sql}")
        client.execute(sql)
        return True
    except Exception as e:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: add index failed", exc_info=True)
        raise ClickHouseAddIndexException(message_kv={MESSAGE: str(e)})
    finally:
        client.disconnect()


def drop_index(rt_info, index_name):
    """
    创建跳数索引
    :param rt_info: 结果表信息
    :param index_name: 索引名称
    :return:
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    try:
        sql = DROP_INDEX_SQL.format(db_name, replicated_table, inner_cluster, index_name)
        logger.info(f"{rt_info[RESULT_TABLE_ID]}: going to drop index, sql: {sql}")
        client.execute(sql)
        return True
    except Exception as e:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: drop index failed", exc_info=True)
        raise ClickHouseDropIndexException(message_kv={MESSAGE: str(e)})
    finally:
        client.disconnect()


def check_table_exists(client, db_name, distributed_table, replicated_table):
    """
    校验建表是否存在，包括复制表和分布式表, 只要存在任何一个节点有表不存在就算作总体上不存在
    :param replicated_table: 本地复制表
    :param distributed_table: 全局分布式表
    :param client: clickhouse客户端
    :param db_name: 库名
    :return: True/False
    """
    resultSet = client.execute(QUERY_CLUSTER_SQL)
    for result in resultSet:
        if not table_exists(result[0], int(result[1]), db_name, distributed_table, replicated_table):
            return False
    return True


def table_exists(host, port, db_name, distributed_table, replicated_table):
    """
    检查表是否存在
    :param host: host
    :param port: 端口
    :param replicated_table: 本地复制表
    :param distributed_table: 全局分布式表
    :param db_name: 库名
    """
    client = None
    try:
        client = build_client(host, port)
        sql = CHECK_TABLE_EXIST_SQL.format(db_name, distributed_table, replicated_table)
        if len(client.execute(sql)) != 2:
            return False
    finally:
        if not client:
            client.disconnect()
    return True


def check_table_consistency(client, db_name, distributed_table, replicated_table):
    """
    校验每个节点上的表结构是否一致
    :param replicated_table: 本地复制表
    :param distributed_table: 全局分布式表
    :param client: clickhouse客户端
    :param db_name: 库名
    :return: 一致性结果
    """
    resultSet = client.execute(QUERY_CLUSTER_SQL)
    schemas = {}
    hash_code = 0
    for result in resultSet:
        host, port = result[0], int(result[1])
        inner_client = build_client(host, port)
        try:
            program = f"{host}:{port}"
            schemas[program] = {}
            sql = SHOW_SCHEMA_SQL.format(db_name, distributed_table)
            schema = get_table_schema(inner_client, sql)
            schemas[program][DISTRIBUTED_TABLE] = schema
            hash_code = hash_code ^ hash(schema)
            sql = SHOW_SCHEMA_SQL.format(db_name, replicated_table)
            schema = get_table_schema(inner_client, sql)
            schemas[program][REPLICATED_TABLE] = schema
            hash_code = hash_code ^ hash(schema)
        finally:
            inner_client.disconnect()

    consistency = True if hash_code == 0 else False
    return {CONSISTENCY: consistency, SCHEMAS: schemas}


def check_schema(rt_info):
    """
    校验RT的字段与ck物理存储字段是否匹配
    :param rt_info: rt的配置信息
    :return: rt字段和存储字段的schema对比
    """
    client, db_name, _, replicated_table, _ = common_initial(rt_info)
    try:
        result = {RT_FIELDS: {}, CLICKHOUSE_FIELDS: {}, CHECK_RESULT: True, CHECK_DIFF: {}}
        for field in rt_info[FIELDS]:
            if field[FIELD_NAME].lower() in ClICKHOUSE_EXCEPT_FIELDS:
                continue
            result[RT_FIELDS][field[FIELD_NAME].lower()] = field[FIELD_TYPE].lower()

        # 表字段信息
        sql = QUERY_FIELDS_SQL.format(db_name, replicated_table)
        for cell in client.execute(sql):
            field_name, file_type = cell[0], cell[1]
            if field_name.lower() in ClICKHOUSE_EXCEPT_FIELDS:
                continue
            result[CLICKHOUSE_FIELDS][field_name.lower()] = file_type

        append_fields, delete_fields, bad_fields = check_rt_clickhouse_fields(
            result[RT_FIELDS], result[CLICKHOUSE_FIELDS]
        )
        result[CHECK_DIFF] = {APPEND_FIELDS: append_fields, DELETE_FIELDS: delete_fields, BAD_FIELDS: bad_fields}
        if bad_fields:
            result[CHECK_RESULT] = False

        logger.warning(f"{rt_info[RESULT_TABLE_ID]} diff between rt fields and ck fields: {result}")
        return result
    except Exception as e:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to create table", exc_info=True)
        raise e
    finally:
        client.disconnect()


def check_rt_clickhouse_fields(rt_table_columns, clickhouse_columns):
    """
    对比rt的字段，和clickhouse物理表字段的区别
    :param rt_table_columns: rt的字段转换为clickhouse中字段后的字段信息
    :param clickhouse_columns: clickhouse物理表字段
    :return: (append_fields, bad_fields)，需增加的字段 和 需要删除的字段
    """
    append_fields, bad_fields, delete_fields = [], {}, []
    for key, value in rt_table_columns.items():
        col_name, col_type = key.lower(), value.lower()
        if col_name in clickhouse_columns:
            if RT_TYPE_TO_CLICKHOUSE_MAPPING[col_type] != clickhouse_columns[col_name]:
                bad_fields[col_name] = "difference between rt and clickhouse({} != {})".format(
                    col_type, clickhouse_columns[col_name]
                )
        else:
            append_fields.append(f"{col_name} {RT_TYPE_TO_CLICKHOUSE_MAPPING[col_type]}")

    delete_fields = list(set(clickhouse_columns.keys()).difference(set(rt_table_columns.keys())))
    if PARTITION_TIME in delete_fields:
        delete_fields.remove(PARTITION_TIME)

    return append_fields, delete_fields, bad_fields


def clusters():
    """
    获取clickhouse存储集群列表
    :return: clickhouse存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(CLICKHOUSE)
    return result


@contextlib.contextmanager
def open_zk(hosts):
    command_retry = kazoo.retry.KazooRetry()
    connection_retry = kazoo.retry.KazooRetry()
    zk = kazoo.client.KazooClient(hosts=hosts, connection_retry=connection_retry, command_retry=command_retry)
    zk.start()
    try:
        yield zk
    finally:
        zk.stop()


def sync_zk_table_whitelist(rt_info):
    """
    同步zk
    :param rt_info: 结果表信息
    """
    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
    ck = rt_info[STORAGES][CLICKHOUSE]
    conn = json.loads(ck[STORAGE_CLUSTER][CONNECTION_INFO])

    result_set = client.execute(QUERY_CLUSTER_SQL)
    hosts = [result[0] for result in result_set]
    if not hosts:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: not found host list, {QUERY_CLUSTER_SQL}")
        raise ClickHouseConfigException(message_kv={MESSAGE: "not found host list"})

    path = CK_TB_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], rt_info[RESULT_TABLE_ID])

    with open_zk(conn[ZK_ADDRESS]) as zk:
        set_zk_data(zk, path, json.dumps(hosts))


def set_zk_data(zk_client, path, data):
    """
    设置zk值
    :param zk_client: zk连接
    :param path: 路径
    :param data: 数据
    """
    logger.info(f"{zk_client.hosts}: going to set {data} for {path}")
    # 节点是否存在, 不存在则创建
    zk_client.ensure_path(path)
    if zk_client.exists(path):
        zk_client.set(path, data)
    else:
        zk_client.create(path, data, ephemeral=True)


def read_zk_data(zk_client, path):
    """
    读取zk节点信息
    :param zk_client: zk客户端
    :param path: 路径
    """
    if not zk_client.exists(path, watch=None):
        raise ClickHouseConfigException(message_kv={MESSAGE: f"{path}: not found path of ck config"})

    data, stat = zk_client.get(path, watch=None)
    if not data:
        raise ClickHouseConfigException(message_kv={MESSAGE: f"{path}: has no ck config"})

    logger.info(f"{zk_client.hosts}: content is {data}, {path}")
    return data


def read_node_data(zk_client, path):
    """
    读取节点信息
    :param zk_client: zk客户端
    :param path: 路径
    """
    result = dict()
    try:
        data = read_zk_data(zk_client, path)
        data_dict = json.loads(data)
        # 检查更新时间，若超过2小时，则告警
        threshold = int(time.time()) * 1000 - 2 * 60 * 60 * 1000
        if threshold > data_dict[TIMESTAMP]:
            raise ClickHouseConfigException(
                message_kv={MESSAGE: f"{path}: last mtime {data_dict[TIMESTAMP]} < threshold time {threshold}"}
            )
        result = data_dict
    except Exception:
        msg = f"{path}: failed to query path weight config, {traceback.format_exc(1000)}"
        logger.error(msg)
        util.wechat_msg(RTX_RECEIVER, msg)

    return result


def read_tb_data(zk_client, path):
    """
    读取节点表信息
    :param zk_client: zk客户端
    :param path: 路径
    """
    data_list = list()
    try:
        data = read_zk_data(zk_client, path)
        data_list = json.loads(data)
    except Exception as e:
        msg = f"{path}: failed to query path table config, {traceback.format_exc(1000)}"
        logger.error(msg)
        util.wechat_msg(RTX_RECEIVER, msg)
        raise e

    return data_list


def weights(rt_info):
    """
    获取权重信息
    :param rt_info: rt的配置信息
    """
    result = {WEIGHTS: {}}
    try:
        ck = rt_info[STORAGES][CLICKHOUSE]
        conn = json.loads(ck[STORAGE_CLUSTER][CONNECTION_INFO])

        client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)
        # 获取节点列表
        result_set = client.execute(QUERY_CLUSTER_SQL)
        hosts = {rs[0]: int(rs[1]) for rs in result_set}
        # 获取zk节点权重信息，并计算权重比例
        if not hosts:
            msg = f"{rt_info[RESULT_TABLE_ID]}: not found host list, {QUERY_CLUSTER_SQL}"
            logger.error(msg)
            util.wechat_msg(RTX_RECEIVER, msg)
            raise ClickHouseConfigException(message_kv={MESSAGE: msg})

        with open_zk(conn[ZK_ADDRESS]) as zk:
            # 获取表白名单列表
            table_white_list = read_tb_data(
                zk, CK_TB_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], rt_info[RESULT_TABLE_ID])
            )

            # 获取本地表列表
            table_host_list = [
                host
                for host, port in hosts.items()
                if table_exists(host, port, db_name, distributed_table, replicated_table)
            ]

            # 可用该表的节点列表，需要有表权限和本地表同时满足
            sum_weight = 0
            avi_table_hosts = [t_h for t_h in table_host_list if (t_h in hosts.keys() and t_h in table_white_list)]
            for host in avi_table_hosts:
                data = read_node_data(zk, CK_NODE_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], host))
                if not data:
                    logger.warning(f"{host}: failed to query node data")
                    continue

                weight = data[FREE_DISK] * data[FACTOR]
                if data[ENABLE] and weight > FUSING_THRESHOLD:
                    result[TIMESTAMP] = data[TIMESTAMP]
                    result[WEIGHTS][f"{host}:{conn[HTTP_PORT]}"] = weight
                    sum_weight += weight
                else:
                    # 节点不可用，容量低于熔断阀值（200），此处依赖容量告警
                    logger.error(
                        "{}: disk free {} lte fusing threshold {} or not enable".format(
                            host, data[FREE_DISK], FUSING_THRESHOLD
                        )
                    )

        # 计算占比
        for host, weight in result[WEIGHTS].items():
            result[WEIGHTS][host] = int(weight * 100 / sum_weight)

    except Exception:
        msg = f"{rt_info[RESULT_TABLE_ID]}: failed to query weights, {traceback.format_exc(1000)}"
        logger.error(msg)
        util.wechat_msg(RTX_RECEIVER, msg)

    return result


def route_config(rt_info):
    """
    获取zk配置信息
    :param rt_info: rt的配置信息
    """
    result = {NODES: [], TABLE: []}
    ck = rt_info[STORAGES][CLICKHOUSE]
    conn = json.loads(ck[STORAGE_CLUSTER][CONNECTION_INFO])

    client, db_name, distributed_table, replicated_table, inner_cluster = common_initial(rt_info)

    # 获取节点列表
    result_set = client.execute(QUERY_CLUSTER_SQL)
    hosts = [rs[0] for rs in result_set]

    if not hosts:
        msg = f"{rt_info[RESULT_TABLE_ID]}: not found host list, {QUERY_CLUSTER_SQL}"
        logger.error(msg)
        raise ClickHouseConfigException(message_kv={MESSAGE: msg})

    with open_zk(conn[ZK_ADDRESS]) as zk:
        for host in hosts:
            data = read_node_data(zk, CK_NODE_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], host))
            result[NODES].append({host: data})

        result[TABLE] = read_tb_data(
            zk, CK_TB_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], rt_info[RESULT_TABLE_ID])
        )
    return result


def set_route_config(rt_info, params):
    """
    设置zk配置
    :param rt_info: rt的配置信息
    :param params: 参数
    """
    ck = rt_info[STORAGES][CLICKHOUSE]
    conn = json.loads(ck[STORAGE_CLUSTER][CONNECTION_INFO])
    with open_zk(conn[ZK_ADDRESS]) as zk:
        if params[TYPE] == NODES:
            path = CK_NODE_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], params[IP])
            data = read_node_data(zk, path)
            for k in [FACTOR, ENABLE, FREE_DISK]:
                if k in params:
                    data[k] = params[k]

            data[TIMESTAMP] = int(time.time()) * 1000
            set_zk_data(zk, path, json.dumps(data))
        else:
            path = CK_TB_ZK_PATH_FORMAT % (ck[STORAGE_CLUSTER][CLUSTER_NAME], rt_info[RESULT_TABLE_ID])
            hosts = params[HOSTS].split(",")
            set_zk_data(zk, path, json.dumps(hosts))
