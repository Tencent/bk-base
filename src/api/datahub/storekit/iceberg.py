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

import copy
import json
from datetime import datetime, timedelta

from common.log import logger
from common.transaction import auto_meta_sync
from datahub.common.const import (
    ADD_FIELDS,
    APPEND_FIELDS,
    ASSIGN_EXPRESSION,
    BAD_FIELDS,
    BK_BIZ_ID,
    CAMEL_ASYNC_COMPACT,
    CHECK_DIFF,
    CHECK_RESULT,
    CODE,
    COMPACT_END,
    COMPACT_START,
    CONDITION_EXPRESSION,
    CONFIG,
    CONNECTION,
    CONNECTION_INFO,
    DATA_TYPE,
    DATABASENAME,
    DEFAULT_FLUSH_SIZE,
    DEFAULT_INTERVAL,
    DELETE_PARTITION,
    DFS_CLIENT_FAILOVER_PROXY_PROVIDER,
    DFS_HA_NAMENODES,
    DFS_NAMENODE_HTTP_ADDRESS,
    DFS_NAMENODE_RPC_ADDRESS,
    DFS_NAMENODE_SERVICERPC_ADDRESS,
    DFS_NAMESERVICES,
    ET,
    EXPIREDAYS,
    EXPIRES,
    FIELD,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    FILES,
    FLUSH_SIZE,
    FS_DEFAULTFS,
    GEOG_AREA,
    GEOG_AREAS,
    HDFS,
    HDFS_CLUSTER_NAME,
    HDFS_CONF,
    HDFS_CONF_DIR,
    HDFS_DEFAULT_PARAMS,
    HDFS_URL,
    HIVE_METASTORE_HOSTS,
    HIVE_METASTORE_PORT,
    HIVE_METASTORE_URIS,
    HOSTS,
    ICEBERG,
    ICEBERG_FIELDS,
    IDS,
    INFO,
    INTERVAL,
    LOCATION,
    LOG_DIR,
    MANAGE,
    MAPLELEAF,
    MESSAGE,
    METHOD,
    MSG,
    NAME,
    NEWTABLENAME,
    PARQUET,
    PARQUET_FLUSH_SIZE,
    PARTITION,
    PARTITION_SPEC,
    PHYSICAL_TABLE_NAME,
    PORT,
    PROCESSING_TYPE,
    QUERYSET,
    RECORD_NUM,
    REMOVE_FIELDS,
    RENAME_FIELDS,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    RPC_PORT,
    RT_FIELDS,
    SCHEMA,
    SERVICERPC_PORT,
    SNAPSHOT,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    TABLENAME,
    TAGS,
    TIME_FIELD,
    TIMESTAMP_MS,
    TOPIC_DIR,
    TRANSFORM,
    TYPE,
    UPDATE_PROPERTIES,
)
from datahub.storekit import model_manager, util
from datahub.storekit.api import DataHubApi
from datahub.storekit.exceptions import (
    IcebergAlterTableException,
    IcebergChangeDataException,
    IcebergCreateTableException,
    IcebergDropTableException,
    IcebergInfoException,
    IcebergUpdateLocationException,
)
from datahub.storekit.settings import (
    DEFAULT_RECORD_NUM,
    EXCEPT_FIELDS,
    HIVE_METASTORE_SERVER,
    ICEBERG_ADDITIONAL_FIELDS,
    ICEBERG_COMPACT_DELTA_DAY,
    ICEBERG_DEFAULT_CONF,
    ICEBERG_DEFAULT_PARTITION,
    PROCESSING_TYPES_NOT_ADDING_FIELDS,
    RT_TYPE_TO_ICEBERG_MAPPING,
)
from datahub.storekit.util import response_to_str


def initialize(rt_info):
    """
    初始化rt的iceberg存储
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """

    return prepare(rt_info)


def info(rt_info):
    """
    获取rt的iceberg存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的iceberg相关信息
    """
    hdfs = rt_info[STORAGES][HDFS]
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.iceberg_info.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg info failed: {response_to_str(rsp)}")
        hdfs[INFO] = {}
    else:
        hdfs[INFO] = json.loads(rsp.data)

    return hdfs


def alter(rt_info):
    """
    修改rt的iceberg存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的iceberg存储的变更结果
    """
    return prepare(rt_info)


def prepare(rt_info):
    """
    准备rt关联的iceberg存储（创建新库表或旧表新增字段）
    :param rt_info: rt的配置信息
    :return: True/False
    """
    try:
        table_schema = schema_info(rt_info)
        add_time_fields = rt_info[PROCESSING_TYPE] not in PROCESSING_TYPES_NOT_ADDING_FIELDS
        expected_fields = expected_physical_table_fields(rt_info[FIELDS], add_time_fields)
        params = construct_connect_config(rt_info)
        storage_conf = json.loads(rt_info[STORAGES][HDFS][STORAGE_CONFIG])
        if add_time_fields:
            # 对于分区表，需要比较分区的定义是否发生变化
            spec = storage_conf.get(PARTITION_SPEC, ICEBERG_DEFAULT_PARTITION)
            table_partition = partition_info(rt_info)
            if not verify_partition_spec(spec, table_partition):
                logger.info(f"iceberg table partition spec is different {spec} {table_partition}")
                params[PARTITION] = spec
                rsp = DataHubApi.iceberg_change_partition_spec.create(params)
                if not rsp.is_success():
                    logger.error(
                        f"{rt_info[RESULT_TABLE_ID]}: iceberg change partition spec failed: {response_to_str(rsp)}"
                    )
                    return False

        add_fields, remove_fields = fields_difference(expected_fields, table_schema)
        if len(add_fields) == 0 and len(remove_fields) == 0:
            return True

        params[ADD_FIELDS] = add_fields
        params[REMOVE_FIELDS] = remove_fields
        rsp = DataHubApi.iceberg_alter_table.create(params)
        if not rsp.is_success():
            logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg alter_table failed: {response_to_str(rsp)}")
            return False

    except IcebergInfoException:
        create_iceberg_table(rt_info)

    return True


def verify_partition_spec(conf_spec, table_spec):
    """
    对比iceberg表的分区配置和实际的分区定义，如果存在差异，则返回False，否则返回True
    :param conf_spec: 表分区配置，参考格式 [{"field": "dtEventTime", "method": "HOUR", "args": 0}]
    :param table_spec: 表分区定义，参考格式 [{"sourceId": 3, "fieldId": 1000, "name": "dteventtime_hour", "transform": "HOUR"}]
    :return: True/False
    """
    if conf_spec and len(conf_spec) == len(table_spec):
        for idx in range(0, len(conf_spec)):
            transform = conf_spec[idx][METHOD].lower()
            name = f"{conf_spec[idx][FIELD].lower()}_{transform}"
            if transform != table_spec[idx][TRANSFORM].lower() or name != table_spec[idx][NAME]:
                return False

    return True


def fields_difference(expected_fields, physical_table_fields):
    """
    rt表和实际物理表的差异
    :param expected_fields: 数组，每个元素中包含rt的一个字段信息，字段名/类型等，均为小写字母。
    :param physical_table_fields: iceberg表所包含的字段信息，均为小写字母。字典组成的数组结构。
    :return: 元组，包含需添加的字段信息和需删除的字段信息
    """
    current_field_names = [field[NAME] for field in physical_table_fields]
    add_fields = []
    for field in expected_fields:
        if field[NAME] not in current_field_names:
            add_fields.append(field)

    expected_field_names = [field[NAME] for field in expected_fields]
    remove_fields = []
    # iceberg中的字段名都是小写的，类型信息是大写的
    for field_name in current_field_names:
        if field_name not in expected_field_names:
            remove_fields.append(field_name)

    return add_fields, remove_fields


def check_rt_iceberg_fields(rt_table_columns, iceberg_columns):
    """
    对比rt的字段，和iceberg物理表字段的区别
    :param rt_table_columns: rt的字段转换为iceberg中字段后的字段信息
    :param iceberg_columns: iceberg物理表字段
    :return: (append_fields, bad_fields)，需变更增加的字段 和 有类型修改的字段
    """
    append_fields, bad_fields = [], []
    for key, value in list(rt_table_columns.items()):
        col_name, col_type = key.lower(), value.lower()
        if col_name in iceberg_columns:
            if RT_TYPE_TO_ICEBERG_MAPPING[col_type] != iceberg_columns[col_name].lower():
                bad_fields.append(
                    {col_name: f"difference between rt and iceberg({col_type} != {iceberg_columns[col_name]})"}
                )
        else:
            append_fields.append({FIELD_NAME: col_name, FIELD_TYPE: col_type})

    return append_fields, bad_fields


def expected_physical_table_fields(fields, add_time_fields):
    """
    :param fields: rt的字段信息信息
    :param add_time_fields: 是否添加系统默认的一些时间字段
    :return: 一个字典的数组，按照字段顺序排列，包含此rt表所需的物理表的字段名称和字段类型，均为小写
    """
    field_dict = []
    if add_time_fields:
        for field in fields:
            if field[FIELD_NAME].lower() not in EXCEPT_FIELDS:
                field_dict.append({NAME: field[FIELD_NAME].lower(), TYPE: field[FIELD_TYPE].lower()})
        for (k, v) in list(ICEBERG_ADDITIONAL_FIELDS.items()):
            field_dict.append({NAME: k.lower(), TYPE: v.lower()})
    else:
        for field in fields:
            field_dict.append({NAME: field[FIELD_NAME].lower(), TYPE: field[FIELD_TYPE].lower()})

    return field_dict


def maintain(rt_info):
    """
    根据用户设定的数据保留时间清理iceberg过期数据，并合并部分数据文件
    :param rt_info: rt的配置信息
    """
    hdfs = rt_info[STORAGES][HDFS]
    if hdfs[DATA_TYPE] != ICEBERG:
        return False

    rt_id = rt_info[RESULT_TABLE_ID]
    physical_tn = hdfs[PHYSICAL_TABLE_NAME]
    try:
        if rt_info[PROCESSING_TYPE] in [QUERYSET, SNAPSHOT]:
            return maintain_datalab_iceberg_rt(rt_id, hdfs[EXPIRES])
        else:
            # 清理过期数据
            expire_days = util.translate_expires_day(hdfs[EXPIRES])
            if not expire_data(rt_info, expire_days):
                logger.error(f"{rt_id} {physical_tn}: failed to clean expire data")
                return False

            # 合并小文件
            if expire_days > ICEBERG_COMPACT_DELTA_DAY or expire_days == -1:
                today = datetime.today()
                start = (today - timedelta(ICEBERG_COMPACT_DELTA_DAY + 1)).strftime("%Y%m%d")
                end = (today - timedelta(ICEBERG_COMPACT_DELTA_DAY)).strftime("%Y%m%d")
                if not compact_files(rt_info, start, end):
                    logger.error(f"{rt_id} {physical_tn}: failed to compact data files")
                    return False
    except Exception:
        logger.warning(f"{rt_id}: failed to maintain", exc_info=True)
        return False

    return True


def maintain_datalab_iceberg_rt(result_table_id, expires):
    """
    :param result_table_id: 结果表id
    :param expires: 过期时间
    """
    # 无分区表，根据快照生成时间，若过期，则删除表后，重建
    rt_info = util.get_rt_info(result_table_id)
    if not rt_info:
        logger.error(f"{result_table_id}: not found rt info")
        return False

    snapshot_list = snapshots(rt_info)
    if snapshot_list:
        # 取最近修改快照
        create_time = snapshot_list[0][TIMESTAMP_MS]
        delta_day = util.translate_expires_day(expires)
        delete_time = util.get_timestamp_diff(-(delta_day + 1))
        logger.info(
            f"{result_table_id}: going to maintain datalab iceberg rt, snapshot: {snapshot_list[0]}, "
            f"create_time: {create_time}, delete_time: {delete_time}"
        )

        if delete_time > create_time:
            # 这里直接rename，对于大数据量rt来说，目前delete操作除了删除meta，还会删除数据，会非常耗时
            if rename_for_future_delete(rt_info) and prepare(rt_info):
                logger.info(f"{result_table_id}: maintain datalab iceberg rt success.")
            else:
                logger.error(f"{result_table_id}: failed to maintain datalab iceberg rt.")
                return False

    return True


def check_schema(rt_info):
    """
    校验RT的字段（名字、类型）的修改是否满足存储的限制
    :param rt_info: rt的配置信息
    :return: rt字段和存储字段的schema对比
    """
    result = {RT_FIELDS: {}, ICEBERG_FIELDS: {}, CHECK_RESULT: True, CHECK_DIFF: {}}
    for field in rt_info[FIELDS]:
        if field[FIELD_NAME].lower() in EXCEPT_FIELDS:
            continue
        result[RT_FIELDS][field[FIELD_NAME].lower()] = field[FIELD_TYPE].lower()

    schema = schema_info(rt_info)
    for field in schema:
        if field[NAME].lower() in EXCEPT_FIELDS:
            continue
        result[ICEBERG_FIELDS][field[NAME].lower()] = field[TYPE].lower()

    append_fields, bad_fields = check_rt_iceberg_fields(result[RT_FIELDS], result[ICEBERG_FIELDS])
    result[CHECK_DIFF] = {APPEND_FIELDS: append_fields, BAD_FIELDS: bad_fields}
    if bad_fields:
        result[CHECK_RESULT] = False

    logger.info(f"diff result: {result}")
    return result


def delete(rt_info):
    """
    清空数据
    :param rt_info: 结果表
    :return:
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.iceberg_drop_table.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: drop table failed: {response_to_str(rsp)}")
        raise IcebergDropTableException(message_kv={MESSAGE: rsp.message})
    else:
        logger.info(f"{rt_info[RESULT_TABLE_ID]}: drop table success")


def rename_for_future_delete(rt_info):
    """
    将表重命名为原名称 + todelete_yyyyMMddHHmmss 的格式，便于后续删除
    :param rt_info: rt的配置信息
    :return: 操作结果
    """
    params = construct_connect_config(rt_info)
    table_name = params[TABLENAME]
    rename = f'{table_name}_todelete_{datetime.now().strftime("%Y%m%d%H%M%S")}'
    params[NEWTABLENAME] = rename
    # 调用hubmanager的接口重命名表，返回结果msg
    rsp = DataHubApi.iceberg_rename_table.create(params)
    if not rsp.is_success():
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: iceberg rename table failed: {response_to_str(rsp)}")
        return False
    return True


def update_location(rt_info, location):
    """
    变更location
    :param rt_info: rt的配置信息
    :param location: location地址
    """
    params = construct_connect_config(rt_info)
    params[LOCATION] = location
    rsp = DataHubApi.iceberg_update_location.create(params)

    if not rsp.is_success():
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: iceberg update location failed: {response_to_str(rsp)}")
        raise IcebergUpdateLocationException(message_kv={MESSAGE: rsp.message})
    else:
        logger.info(f"{rt_info[RESULT_TABLE_ID]}: iceberg update location success, location: {params[LOCATION]}")


def location(rt_info):
    """
    获取location
    :param rt_info: rt的配置信息
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.iceberg_location.create(params)

    if not rsp.is_success() or not rsp.data:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: query iceberg location failed: {response_to_str(rsp)}")
        raise IcebergInfoException(message_kv={MESSAGE: rsp.message})

    return rsp.data


def schema_info(rt_info):
    """
    获取表的schema信息
    :param rt_info: rt的配置信息
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.iceberg_schema_info.create(params)

    if not rsp.is_success():
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: query iceberg schema info failed: {response_to_str(rsp)}")
        raise IcebergInfoException(message_kv={MESSAGE: rsp.message})

    return rsp.data


def partition_info(rt_info):
    """
    分区信息
    :param rt_info: rt的配置信息
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.iceberg_partition_info.create(params)

    if not rsp.is_success():
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: query iceberg partition info failed: {response_to_str(rsp)}")
        raise IcebergInfoException(message_kv={MESSAGE: rsp.message})

    return rsp.data


def construct_connect_config(rt_info):
    """
    :param rt_info: rt的配置信息
    :return: 一个字典，包含连接iceberg所需的配置
    """
    hdfs = rt_info[STORAGES][HDFS]
    physical_tn = hdfs[PHYSICAL_TABLE_NAME]
    database_name, table_name = physical_tn.split(".")[0], physical_tn.split(".")[1]
    conn_info = hdfs[STORAGE_CLUSTER][CONNECTION_INFO]
    geog_area = rt_info[TAGS][MANAGE][GEOG_AREA][0][CODE]
    hdfs_config = construct_hdfs_conf(conn_info, geog_area, rt_info[STORAGES][HDFS][DATA_TYPE])

    # 将所有HDFS集群的链接信息加入到hdfs_config里，适应iceberg表数据文件和元文件可能跨hdfs集群的情形
    all_conf = all_clusters_conf()
    hdfs_config.update(all_conf[HDFS_CONF])

    return get_base_api_params(database_name, table_name, hdfs_config)


def get_base_api_params(database_name, table_name, hdfs_config):
    """
    构造请求hubmanager的iceberg相关接口的基础参数
    :param database_name: db名称
    :param table_name: 表名称
    :param hdfs_config: hdfs相关配置，包含hive metastore连接信息
    :return: 字典，包含请求hubmanager中iceberg相关接口的基础参数
    """
    # 时间字段为隐藏的____et，用于数据分区，数据按照时间过期和小数据文件合并
    return {DATABASENAME: database_name, TABLENAME: table_name, CONFIG: hdfs_config, TIME_FIELD: ET}


def construct_hdfs_conf(conn_info, geog_area, data_type=PARQUET):
    """
    构造iceberg相关的配置项字典
    :param data_type: hdfs中数据的存储格式
    :param geog_area: 区域信息，用于获取正确的hive metastore地址
    :param conn_info: 表所在hdfs集群的连接配置信息
    :return: iceberg相关的配置项字典
    """
    conn = json.loads(conn_info)
    # 加载默认的iceberg相关配置
    conf = copy.deepcopy(ICEBERG_DEFAULT_CONF)
    port = HIVE_METASTORE_SERVER.get(geog_area, {}).get(HIVE_METASTORE_PORT, 9083)
    hosts = HIVE_METASTORE_SERVER.get(geog_area, {}).get(HIVE_METASTORE_HOSTS, "").split(",")
    metastore_uris = [f"thrift://{host}:{port}" for host in hosts]
    conf[HIVE_METASTORE_URIS] = ",".join(metastore_uris)

    # 如果hdfs连接配置中包含iceberg相关配置，则覆盖默认的配置
    prefix = ICEBERG + "."
    for k in conn:
        if k.startswith(prefix):
            conf[k.replace(prefix, "", 1)] = conn[k]

    conf[FS_DEFAULTFS] = conn.get(HDFS_URL, "")
    # 设置参数值，其中namenode有多个，需要拆分为list
    if HDFS_DEFAULT_PARAMS in conn:
        conf.update(conn.get(HDFS_DEFAULT_PARAMS, {}))

    conf.update(build_hdfs_conn_conf(conn))
    conf[HDFS_CONF_DIR] = conn.get(HDFS_CONF_DIR, "")
    conf[TOPIC_DIR] = conn.get(TOPIC_DIR, "")
    conf[LOG_DIR] = conn.get(LOG_DIR, "")
    if data_type != ICEBERG:
        conf[FLUSH_SIZE] = conn.get(PARQUET_FLUSH_SIZE, DEFAULT_FLUSH_SIZE)
        conf[INTERVAL] = conn.get(INTERVAL, DEFAULT_INTERVAL)

    return conf


def build_hdfs_conn_conf(conn):
    """
    根据HDFS集群的连接信息（dict）构造连接HDFS的配置
    :param conn: HDFS连接串
    :return: 使用java连接HDFS集群的配置
    """
    cluster_name = conn[HDFS_CLUSTER_NAME]
    conf = {DFS_NAMESERVICES: cluster_name}
    hosts_arr = conn[HOSTS].split(",")
    namenode_arr = conn[IDS].split(",")
    if len(namenode_arr) != len(hosts_arr):
        raise ValueError(f"bad hdfs cluster config: {json.dumps(conn)}")

    # 设置参数值，其中namenode有多个，需要拆分为list
    conf[f"{DFS_HA_NAMENODES}.{cluster_name}"] = conn[IDS]
    conf[
        f"{DFS_CLIENT_FAILOVER_PROXY_PROVIDER}.{cluster_name}"
    ] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    for i in range(0, len(namenode_arr)):
        suffix = f"{cluster_name}.{namenode_arr[i]}"
        conf[f"{DFS_NAMENODE_RPC_ADDRESS}.{suffix}"] = f"{hosts_arr[i]}:{conn[RPC_PORT]}"
        conf[f"{DFS_NAMENODE_SERVICERPC_ADDRESS}.{suffix}"] = f"{hosts_arr[i]}:{conn[SERVICERPC_PORT]}"
        conf[f"{DFS_NAMENODE_HTTP_ADDRESS}.{suffix}"] = f"{hosts_arr[i]}:{conn[PORT]}"

    return conf


def all_clusters_conf():
    """
    获取所有HDFS集群的连接信息，包括区域列表，以及区域对应的hive metastore地址信息。
    :return: 字典，包含HDFS集群的连接信息。
    """
    result = {GEOG_AREAS: [], HIVE_METASTORE_URIS: {}, HDFS_CONF: {}}
    for geog_area in list(HIVE_METASTORE_SERVER.keys()):
        port = HIVE_METASTORE_SERVER.get(geog_area, {}).get(HIVE_METASTORE_PORT, 9083)
        hosts = HIVE_METASTORE_SERVER.get(geog_area, {}).get(HIVE_METASTORE_HOSTS, "").split(",")
        metastore_uris = [f"thrift://{host}:{port}" for host in hosts]
        result[HIVE_METASTORE_URIS][geog_area] = ",".join(metastore_uris)
        result[GEOG_AREAS].append(geog_area)

    name_services = []
    for cluster in model_manager.get_storage_cluster_configs_by_type(HDFS):
        name_services.append(cluster[CONNECTION][HDFS_CLUSTER_NAME])
        result[HDFS_CONF].update(build_hdfs_conn_conf(cluster[CONNECTION]))

    result[HDFS_CONF][DFS_NAMESERVICES] = ",".join(name_services)
    return result


def create_iceberg_table(rt_info, raise_exception=True):
    """
    创建hdfs存储对于的iceberg表
    :param rt_info: rt信息
    :param raise_exception: 当创建表失败时是否抛出异常，默认为True
    """
    params = construct_connect_config(rt_info)
    add_time_fields = rt_info[PROCESSING_TYPE] not in PROCESSING_TYPES_NOT_ADDING_FIELDS
    params[SCHEMA] = expected_physical_table_fields(rt_info[FIELDS], add_time_fields)
    # 使用存储配置中定义的分区信息，如果不存在，则使用默认的表分区配置
    storage_conf = json.loads(rt_info[STORAGES][HDFS][STORAGE_CONFIG])
    if PARTITION_SPEC in storage_conf:
        params[PARTITION] = storage_conf[PARTITION_SPEC]
    else:
        params[PARTITION] = ICEBERG_DEFAULT_PARTITION if add_time_fields else []
    rsp = DataHubApi.iceberg_create_table.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: create table failed: {response_to_str(rsp)}")
        if raise_exception:
            raise IcebergCreateTableException(message_kv={MESSAGE: rsp.message})
    else:
        logger.info(f"{rt_info[RESULT_TABLE_ID]}: create table success")


def update_data(rt_info, condition, assign):
    """
    按条件更新数据
    :param rt_info: rt的字段和配置信息
    :param condition: 条件表达式
    :param assign: 赋值表达式
    :return: 执行结果
    """
    params = construct_connect_config(rt_info)
    params[CONDITION_EXPRESSION] = condition
    params[ASSIGN_EXPRESSION] = assign
    rsp = DataHubApi.iceberg_update_data.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg update_data failed: {response_to_str(rsp)}")
        raise IcebergChangeDataException(f"iceberg update_data failed with condition: {condition}, assign: {assign}")

    return rsp.data


def delete_data(rt_info, condition):
    """
    按条件删数据
    :param rt_info: rt的字段和配置信息
    :param condition: 条件表达式
    :return:  执行结果
    """
    params = construct_connect_config(rt_info)
    params[CONDITION_EXPRESSION] = condition
    rsp = DataHubApi.iceberg_delete_data.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg delete_data failed: {response_to_str(rsp)}")
        raise IcebergChangeDataException(f"iceberg delete_data failed with condition: {condition}")

    return rsp.data


def expire_data(rt_info, expire_days):
    """
    清理过期数据
    :param expire_days: 数据保存周期
    :param rt_info: rt的字段和配置信息
    :return:  执行结果
    """
    params = construct_connect_config(rt_info)
    return expire_data_with_params(params, expire_days)


def expire_data_with_params(params, expire_days):
    """
    清理过期数据
    :param params: iceberg的连接参数
    :param expire_days: 数据保存周期
    """
    # expire_days: 多保留一天作为buffer; expire_days = -1 表示永久保存, 0和其它负数不合法，此外iceberg-sdk要求expireDays > 1
    expire_days = expire_days + 1
    if expire_days < 0 or expire_days == 1:
        logger.error(f"iceberg expire_data failed with invalid expire_days: {expire_days - 1}")
        return False
    elif expire_days == 0:
        return True
    else:
        params[EXPIREDAYS] = expire_days

    rsp = DataHubApi.iceberg_expire.create(params)
    if not rsp.is_success():
        logger.error(f"{params[TABLENAME]}: iceberg expire_data failed: {response_to_str(rsp)}")
        return False

    return True


def compact_files(rt_info, start, end, async_compact=False):
    """
    合并数据文件
    :param rt_info: rt的字段和配置信息
    :param start: 起始时间
    :param end: 结束时间
    :param async_compact: 是否异步执行compact任务
    :return:  执行结果
    """
    params = construct_connect_config(rt_info)
    params[COMPACT_START], params[COMPACT_END], params[CAMEL_ASYNC_COMPACT] = start, end, async_compact
    rsp = DataHubApi.iceberg_compact.create(params)
    if not rsp.is_success():
        logger.error(f"{params[TABLENAME]}: iceberg compact files failed: {response_to_str(rsp)}")
        return False

    return True


def update_timestamp_partition(rt_info, field_name, transform):
    """
    更新时间字段的分区策略。当字段并非分区字段，或者分区策略和之前一样时，抛出异常。
    :param rt_info: rt的字段和配置信息
    :param field_name: 分区字段名称
    :param transform: 分区策略，大写字母，可选值为YEAR/MONTH/DAY/HOUR
    """
    rt_id = rt_info[RESULT_TABLE_ID]
    storage_conf = json.loads(rt_info[STORAGES][HDFS][STORAGE_CONFIG])
    if not storage_conf.get(PARTITION_SPEC):
        raise IcebergAlterTableException(message_kv={MESSAGE: "not partition table"})
    else:
        partition_def = storage_conf.get(PARTITION_SPEC)
        # 获取iceberg表分区信息，对比分区字段是否和field_name一致。iceberg表字段均为小写。
        field_name = field_name.lower()
        for i in range(0, len(partition_def)):
            entry = partition_def[i]
            if field_name == entry.get(FIELD).lower():
                if entry.get(METHOD).upper() == transform:
                    raise IcebergAlterTableException(message_kv={MESSAGE: "partition def not changed."})
                else:
                    entry[METHOD] = transform
                    partition_def[i] = entry
                    storage_conf[PARTITION_SPEC] = partition_def
                    # 更新存储的分区配置信息到db里
                    objs = model_manager.get_storage_rt_objs_by_rt_type(rt_id, HDFS)
                    relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
                    relation_obj.storage_config = json.dumps(storage_conf)
                    with auto_meta_sync(using=MAPLELEAF):
                        relation_obj.save()

                    params = construct_connect_config(rt_info)
                    params[PARTITION] = partition_def
                    rsp = DataHubApi.iceberg_change_partition_spec.create(params)
                    if not rsp.is_success():
                        logger.error(f"{rt_id}: iceberg change partition spec failed: {response_to_str(rsp)}")
                        raise IcebergAlterTableException(message_kv={MESSAGE: rsp.message})
                    else:
                        logger.info(f"{rt_id}: iceberg table partition changed to {partition_def}")
                        return

    # 走到这里，说明未找到field_name分区字段定义，无法对表分区进行修改
    raise IcebergAlterTableException(message_kv={MESSAGE: f"field {field_name} not found"})


def delete_partitions(rt_info, partitions):
    """
    删除指定分区
    :param rt_info: rt的字段和配置信息
    :param partitions: 分区值集合
    :return:  执行结果
    """
    params = construct_connect_config(rt_info)
    params[DELETE_PARTITION] = partitions
    rsp = DataHubApi.iceberg_delete_partitions.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg delete_partitions failed: {response_to_str(rsp)}")
        raise IcebergChangeDataException(f"iceberg delete_partitions failed with partitions: {partitions}")

    return rsp.data


def read_records(rt_info, condition, record_num=DEFAULT_RECORD_NUM):
    """
    按条件读数据
    :param rt_info: rt的字段和配置信息
    :param condition: 条件表达式
    :param record_num: 返回记录行数
    :return: 查询结果数据列表
    """
    params = construct_connect_config(rt_info)
    params[RECORD_NUM] = record_num
    params[CONDITION_EXPRESSION] = condition
    rsp = DataHubApi.iceberg_read_records.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg read_records failed: {response_to_str(rsp)}")
        return []

    return rsp.data


def snapshots(rt_info, record_num=DEFAULT_RECORD_NUM):
    """
    获取rt的iceberg 快照信息
    :param rt_info: rt的字段和配置信息
    :param record_num: 返回记录行数
    :return: rt快照记录
    """
    params = construct_connect_config(rt_info)
    params[RECORD_NUM] = record_num
    rsp = DataHubApi.iceberg_snapshots.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg snapshots failed: {response_to_str(rsp)}")
        return []

    return rsp.data


def commit_msgs(rt_info, record_num=DEFAULT_RECORD_NUM):
    """
    获取rt的iceberg commit信息
    :param rt_info: rt的字段和配置信息
    :param record_num: 返回记录行数
    :return: rt的commit记录
    """
    params = construct_connect_config(rt_info)
    params[RECORD_NUM] = record_num
    rsp = DataHubApi.iceberg_commit_msgs.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg commit_msgs failed: {response_to_str(rsp)}")
        return []

    return rsp.data


def sample_records(rt_info, record_num, raw):
    """
    获取rt的iceberg的采样数据
    :param rt_info: rt的字段和配置信息
    :param record_num:
    :param raw: 是否原样返回hubmanager的结果，如果为False，则去掉隐藏时间分区字段，并使用meta中字段名称
    :return: rt的采样数据
    """
    params = construct_connect_config(rt_info)
    params[RECORD_NUM] = record_num
    rsp = DataHubApi.iceberg_sample_records.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg sample_records failed: {response_to_str(rsp)}")
        return []

    if raw:
        return rsp.data
    else:
        # 返回的结果集中字段名称都是小写字符，需要转换为元数据中的字段名称，所以需要首先生成字段名称的映射关系
        name_mapping = {}
        for field in rt_info[FIELDS]:
            name_mapping[field[FIELD_NAME].lower()] = field[FIELD_NAME]

        for k in ICEBERG_ADDITIONAL_FIELDS:
            if k != ET:
                name_mapping[k.lower()] = k

        result = []
        for row in rsp.data:
            item = {}
            for (k, v) in list(row.items()):
                if k in name_mapping:
                    item[name_mapping[k]] = v
            result.append(item)

        return result


def update_properties(rt_info, properties):
    """
    更新iceberg table表属性
    :param properties: 属性集合
    :param rt_info: rt的字段和配置信息
    :return: 执行结果
    """
    params = construct_connect_config(rt_info)
    params[UPDATE_PROPERTIES] = properties
    rsp = DataHubApi.iceberg_update_properties.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg update_properties failed: {response_to_str(rsp)}")
        return False

    return True


def delete_datafiles(rt_info, files, msg):
    """
    删除iceberg表中数据文件
    :param rt_info: rt的字段和配置信息
    :param files: 待删除的文件列表
    :param msg: 备注信息
    :return:
    """
    params = construct_connect_config(rt_info)
    params[FILES] = files
    params[MSG] = msg
    rsp = DataHubApi.iceberg_delete_datafiles.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to delete datafiles {files}. {response_to_str(rsp)}")
        return False

    return True


def list_catalog(rt_info):
    """
    列出rt所在catalog全部库表
    :param rt_info: rt的字段和配置信息
    :return: rt所在catalog全部库表
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.list_catalog.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg list_catalog failed: {response_to_str(rsp)}")
        return {}

    return rsp.data


def force_release_lock(rt_info):
    """
    强制释放rt所在hive metastore上的lock
    :param rt_info: rt的字段和配置信息
    :return: 执行结果
    """
    params = construct_connect_config(rt_info)
    rsp = DataHubApi.force_release_lock.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg force_release_lock failed: {response_to_str(rsp)}")
        return False

    return True


def rename_fields(rt_info, fields):
    """
    修改rt的iceberg表字段名
    :param rt_info: rt的字段和配置信息
    :param fields: 字段改名映射关系
    :return: True/False
    """
    params = construct_connect_config(rt_info)
    params[RENAME_FIELDS] = fields
    rsp = DataHubApi.iceberg_alter_table.create(params)
    if not rsp.is_success():
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: iceberg rename fields failed: {response_to_str(rsp)}")
        return False

    return True


def physical_table_name(bk_biz_id, result_table_name):
    """
    :param bk_biz_id: 业务id
    :param result_table_name: 结果表名
    """
    # result_table_name 使用小写，在hive 元信息中hive表均为小写
    return f"{ICEBERG}_{bk_biz_id}.{result_table_name.lower()}_{bk_biz_id}"


def change_hdfs_storage_to_iceberg(rt_info):
    """
    将rt配置信息中HDFS存储的数据类型（data_type）修改为iceberg类型，并设置对应的物理表名称、分区配置等信息
    :param rt_info: rt的配置信息
    :return: 更新后的rt配置信息
    """
    if rt_info[STORAGES][HDFS][DATA_TYPE] != ICEBERG:
        physical_tn = physical_table_name(rt_info[BK_BIZ_ID], rt_info[RESULT_TABLE_NAME])
        rt_info[STORAGES][HDFS][DATA_TYPE] = ICEBERG
        rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME] = physical_tn
        storage_conf = json.loads(rt_info[STORAGES][HDFS][STORAGE_CONFIG])

        # 如果配置不存在分区信息，则使用默认分区信息
        if not storage_conf.get(PARTITION_SPEC):
            storage_conf[PARTITION_SPEC] = ICEBERG_DEFAULT_PARTITION
            rt_info[STORAGES][HDFS][STORAGE_CONFIG] = json.dumps(storage_conf)

    return rt_info
