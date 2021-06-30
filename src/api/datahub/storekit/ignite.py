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
import random

from common.log import logger
from datahub.common.const import (
    APPEND_FIELDS,
    BACKUPS,
    BAD_FIELDS,
    CHECK_DIFF,
    CHECK_RESULT,
    CLUSTER_NAME,
    CONNECTION_INFO,
    DTEVENTTIMESTAMP,
    EXPIRES,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    HOST,
    IGNITE,
    INDEXED_FIELDS,
    INFO,
    MESSAGE,
    PASSWORD,
    PHYSICAL_TABLE_NAME,
    PORT,
    PROCESSING_TYPE,
    QUERYSET,
    RESULT_TABLE_ID,
    RT_FIELDS,
    SAMPLE,
    SCENARIO_TYPE,
    SCHEMA,
    SNAPSHOT,
    SQL,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGE_KEYS,
    STORAGES,
    USER,
    VARCHAR,
)
from datahub.storekit import exceptions, model_manager, util
from datahub.storekit.api import DataHubApi
from datahub.storekit.settings import (
    COLUMN_FIELDS,
    DATA_REGION,
    EXCEPT_FIELDS,
    IGNITE_ALTER_TABLE_SQL,
    IGNITE_CONN,
    IGNITE_CREATE_DATALAB_TABLE_SQL_PREFIX,
    IGNITE_CREATE_INDEX_SQL,
    IGNITE_CREATE_TABLE_SQL_PREFIX,
    IGNITE_CREATE_TABLE_SQL_SUFFIX,
    IGNITE_IP_LIB_SCENARIO_TYPE,
    IGNITE_JOIN_SCENARIO_TYPE,
    IGNITE_SHOW_COLUMN_SQL,
    IGNITE_SHOW_TABLE_SQL,
    IGNITE_SYSTEM_FIELDS,
    IGNITE_TYPE_MAPPING,
    PAR_EXPIRY_TEMP,
    PARTITION_BACKUPS,
    PARTITION_TEMPLATE,
    REPLICATED_BACKUPS,
    REPLICATED_TEMPLATE,
    SKIP_RT_FIELDS,
    TABLE_FIELDS,
)


def initialize(rt_info):
    """
    初始化rt的ignite存储
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """
    return prepare(rt_info)


def info(rt_info):
    """
    获取rt的ignite存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的ignite相关信息
    """
    ignite = rt_info[STORAGES][IGNITE]
    physical_tn = ignite[PHYSICAL_TABLE_NAME]
    conn_info = json.loads(ignite[STORAGE_CLUSTER][CONNECTION_INFO])
    conn_str = _generate_conn_str(conn_info)
    ptn_arr = physical_tn.split(".")
    format_param = {
        "TABLE_SCHEMA": ptn_arr[0],
        "TABLE_NAME": ptn_arr[1],
    }
    fields = [field[FIELD_NAME] for field in rt_info[FIELDS] if field[FIELD_NAME] not in SKIP_RT_FIELDS]
    sample_sql = f'SELECT {",".join(fields)} FROM {format_param["TABLE_SCHEMA"]}.{format_param["TABLE_NAME"]} LIMIT 10'
    cache_info = _query_cache_info(
        ignite[STORAGE_CLUSTER][CLUSTER_NAME],
        conn_str,
        format_param["TABLE_SCHEMA"],
        format_param["TABLE_NAME"],
        format_param["TABLE_NAME"],
    )
    ignite[INFO] = {
        "cache": cache_info,
        SAMPLE: _get_sql_result(
            ignite[STORAGE_CLUSTER][CLUSTER_NAME], conn_str, format_param["TABLE_SCHEMA"], sample_sql.format(), fields
        ),
    }
    return ignite


def alter(rt_info):
    """
    修改rt的ignite存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的ignite存储的变更结果
    """
    return prepare(rt_info)


def delete(rt_info):
    """
    删除rt的ignite存储相关配置
    :param rt_info: rt的配置信息
    :return: rt的ignite存储清理结果
    """
    return True


def prepare(rt_info):
    """
    准备rt关联的ignite存储（创建新cache或旧表新增字段）
    :param rt_info: rt的配置信息
    :return: True/False
    """
    ignite = rt_info[STORAGES][IGNITE]
    physical_tn = ignite[PHYSICAL_TABLE_NAME]
    ptn_arr = physical_tn.split(".")
    conn_info = json.loads(ignite[STORAGE_CLUSTER][CONNECTION_INFO])
    conn_str = _generate_conn_str(conn_info)
    cluster_name = ignite[STORAGE_CLUSTER][CLUSTER_NAME]
    is_datalab_rt = rt_info[PROCESSING_TYPE] in [SNAPSHOT, QUERYSET]
    # 如果表不存在，则创建表
    if not _is_table_exist(conn_info, ptn_arr[0], ptn_arr[1]):
        # 创建表与索引
        create_table_sql = _generate_create_table_sql(rt_info, is_datalab_rt)
        _execute_sql_result(cluster_name, conn_str, ptn_arr[0], create_table_sql)

        create_index_sql = _generate_create_index_sql(rt_info, is_datalab_rt)
        if create_index_sql:
            logger.info(f"{rt_info[RESULT_TABLE_ID]}: going to create index, sql: {create_index_sql}")
            _execute_sql_result(cluster_name, conn_str, ptn_arr[0], create_index_sql)
    else:
        # 如果表存在，则对比schema，走变更表流程
        table_cols = get_ignite_table_columns(rt_info, is_datalab_rt)
        rt_cols = _get_rt_table_columns(rt_info[FIELDS], is_datalab_rt)
        append_fields, bad_fields = _check_rt_ignite_fields(rt_cols, table_cols)
        if bad_fields:
            raise exceptions.SchemaMismatchError(
                message_kv={
                    "cols": json.dumps([list(i.values())[0] for i in bad_fields]),
                    "msg": json.dumps(bad_fields),
                }
            )
        elif append_fields:
            alter_sql = _generate_alter_table_sql(ptn_arr[0], ptn_arr[1], append_fields)
            _execute_sql_result(cluster_name, conn_str, ptn_arr[1], alter_sql)
        else:
            logger.info(f"{rt_info[RESULT_TABLE_ID]} has no old fields with field type modified or new fields added")
    return True


def check_schema(rt_info):
    """
    校验RT的字段（名字、类型）的修改是否满足存储的限制
    :param rt_info: rt的配置信息
    :return: rt字段和存储字段的schema对比
    """
    ignite = rt_info[STORAGES][IGNITE]
    physical_tn = ignite[PHYSICAL_TABLE_NAME]
    ptn_arr = physical_tn.split(".")
    conn_info = json.loads(ignite[STORAGE_CLUSTER][CONNECTION_INFO])
    is_datalab_rt = rt_info[PROCESSING_TYPE] in [SNAPSHOT, QUERYSET]

    result = {RT_FIELDS: {}, "ignite_fields": {}, CHECK_RESULT: True, CHECK_DIFF: {}}
    for field in rt_info[FIELDS]:
        if not is_datalab_rt and field[FIELD_NAME].lower() in EXCEPT_FIELDS:
            continue
        result[RT_FIELDS][field[FIELD_NAME]] = field[FIELD_TYPE]

    if _is_table_exist(conn_info, ptn_arr[0], ptn_arr[1]):
        # 表已经创建，检查字段的变化情况
        table_cols = get_ignite_table_columns(rt_info, is_datalab_rt)
        rt_cols = _get_rt_table_columns(rt_info[FIELDS], is_datalab_rt)
        append_fields, bad_fields = _check_rt_ignite_fields(rt_cols, table_cols)

        result["ignite_fields"] = table_cols
        result[CHECK_DIFF] = {APPEND_FIELDS: append_fields, BAD_FIELDS: bad_fields}
        if bad_fields:
            result[CHECK_RESULT] = False
    return result


def clusters():
    """
    获取ignite存储集群列表
    :return: ignite存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(IGNITE)
    return result


def _get_sql_result(cluster_name, conn_str, schema, sql, fields):
    """
    :param cluster_name: 集群名称
    :param conn_str: 连接信息
    :param schema: schema
    :param sql: 执行sql
    :param fields: 字段信息
    :return: 执行结果
    """
    param = {"clusterName": cluster_name, "url": conn_str, SQL: sql, SCHEMA: schema, FIELDS: fields}

    rsp = DataHubApi.ignite_sql_query.list(param)
    if not rsp.is_success():
        logger.info(f"ignite query sql error, message: {rsp.message}")
        raise exceptions.IgniteQueryException(message_kv={MESSAGE: rsp.message})

    return rsp.data


def _execute_sql_result(cluster_name, conn_str, schema, sql):
    """
    :param cluster_name: 集群名称
    :param conn_str: 连接信息
    :param schema: schema
    :param sql: 执行sql
    :return: 执行结果
    """
    param = {
        "clusterName": cluster_name,
        "url": conn_str,
        SQL: sql,
        SCHEMA: schema,
    }

    rsp = DataHubApi.ignites_sql_execute.list(param)
    if not rsp.is_success():
        logger.info(f"ignite execute sql error, message: {rsp.message}")
        raise exceptions.IgniteQueryException(message_kv={MESSAGE: rsp.message})

    return True


def _query_cache_info(cluster_name, conn_str, schema, cache_name, table_name):
    """
    :param cluster_name: 集群名称
    :param conn_str: 连接信息
    :param schema: schema
    :param cache_name: cache名称
    :param table_name: table名
    :return: cache信息
    """
    param = {
        "clusterName": cluster_name,
        "url": conn_str,
        "cacheName": cache_name,
        "tableName": table_name,
        SCHEMA: schema,
    }

    rsp = DataHubApi.ignite_cache_info.list(param)
    if not rsp.is_success():
        logger.info(f"query cache info error, message: {rsp.message}")
        raise exceptions.IgniteInfoException(message_kv={MESSAGE: rsp.message})

    return rsp.data


def _generate_create_index_sql(rt_info, is_datalab_rt):
    """
    生成ignite的索引语句
    :param rt_info: rt的配置信息
    :param is_datalab_rt: 是否是数据探索rt
    :return: ignite的索引语句
    """
    indexed_fields = json.loads(rt_info[STORAGES][IGNITE][STORAGE_CONFIG]).get(INDEXED_FIELDS, [])
    physical_table_name = rt_info[STORAGES][IGNITE][PHYSICAL_TABLE_NAME]
    table_name = physical_table_name.split(".")[1]

    if not is_datalab_rt:
        indexed_fields.append(DTEVENTTIMESTAMP.lower())

    # 如果不存在索引字段，不需要创建索引
    if not indexed_fields:
        logger.info("no index fields need to create!")
        return None

    format_param = {"FIELDS": ",".join(indexed_fields), "TABLE_NAME": table_name}
    return IGNITE_CREATE_INDEX_SQL.format(**format_param)


def _generate_alter_table_sql(schema_name, table_name, append_fields):
    """
    生成表变更语句，在尾部增加字段
    :param schema_name: schema名称
    :param table_name: 表名称
    :param append_fields: 增加的字段和类型信息
    :return: 变更的表sql语句
    """

    format_param = {
        "TABLE_SCHEMA": schema_name,
        "TABLE_NAME": table_name,
        "FIELDS": ",".join(f"{field[FIELD_NAME]} {field[FIELD_TYPE]}" for field in append_fields),
    }
    return IGNITE_ALTER_TABLE_SQL.format(**format_param)


def _generate_create_table_sql(rt_info, is_datalab_rt):
    """
    生成ignite的建表语句
    :param rt_info: rt的配置信息
    :param is_datalab_rt: 是否是数据探索rt
    :return: ignite的建表语句
    """

    ignite = rt_info[STORAGES][IGNITE]
    conn_info = json.loads(ignite[STORAGE_CLUSTER][CONNECTION_INFO])
    storage_config = json.loads(ignite[STORAGE_CONFIG])
    key_fields = storage_config.get(STORAGE_KEYS, [])
    expires = ignite[EXPIRES]
    scenario_type = storage_config.get(SCENARIO_TYPE, IGNITE_JOIN_SCENARIO_TYPE)
    physical_table_name = ignite[PHYSICAL_TABLE_NAME]
    processing_type = rt_info[PROCESSING_TYPE]
    rt_ignite_cols = _get_rt_table_columns(rt_info[FIELDS], processing_type)

    sql_fields = list()
    for field in rt_info[FIELDS]:
        name = field[FIELD_NAME]
        if name in rt_ignite_cols:
            ignite_type = rt_ignite_cols[name]
            # DEFAULT NULL 是错误语法，在ignite中，默认只能是常量
            field_sql = f"{name} {ignite_type}"
            sql_fields.append(field_sql)

    if is_datalab_rt:
        sql = IGNITE_CREATE_DATALAB_TABLE_SQL_PREFIX + ", ".join(sql_fields) + ", " + IGNITE_CREATE_TABLE_SQL_SUFFIX
    else:
        sql = IGNITE_CREATE_TABLE_SQL_PREFIX + ", ".join(sql_fields) + ", " + IGNITE_CREATE_TABLE_SQL_SUFFIX

    template = _adapter_template(scenario_type, expires)
    backups = REPLICATED_BACKUPS if template == REPLICATED_TEMPLATE else conn_info.get(BACKUPS, PARTITION_BACKUPS)
    data_region = DATA_REGION[scenario_type]
    format_param = {
        "TEMPLATE": template,
        "DATA_REGION": data_region,
        "PRIMARY_KEYS": ",".join(key_fields),
        "TABLE_NAME": physical_table_name.split(".")[1],
        "BACKUPS": backups,
    }
    return sql.format(**format_param)


def _adapter_template(scenario_type, expires):
    """
    :param scenario_type: 场景类型
    :param expires: 过期时间
    :return: ignite cache 模版
    """
    # iplib 关联数据为复制模式。无备份
    if scenario_type == IGNITE_IP_LIB_SCENARIO_TYPE:
        template = REPLICATED_TEMPLATE
    else:
        expires_days = util.translate_expires_day(expires)
        # 根据过期天数适配最合适的cache模版
        if expires_days == -1:
            template = PARTITION_TEMPLATE
        else:
            template = PAR_EXPIRY_TEMP.get(expires_days)
            # 不存在过期天数对应的模版,则选择第一个比其小的模版
            if not template:
                expire_list = PAR_EXPIRY_TEMP.keys()
                expire_list.sort()
                for expire in expire_list:
                    if expires_days < expire:
                        template = PAR_EXPIRY_TEMP[expire]
                        break
    return template if template else PARTITION_TEMPLATE


def _get_storage_info(rt_info):
    """
    获取存储基本信息
    :param rt_info: rt的信息
    :return: ignite, physical_tn, conn_info
    """
    ignite = rt_info[STORAGES][IGNITE]
    physical_tn = ignite[PHYSICAL_TABLE_NAME]
    conn_info = json.loads(ignite[STORAGE_CLUSTER][CONNECTION_INFO])
    return ignite, physical_tn, conn_info


def get_ignite_table_columns(rt_info, is_datalab_rt):
    """
    获取ignite中表的字段信息，不包含系统添加的时间字段
    :param rt_info: 结果表信息
    :param is_datalab_rt: 是否是数据探索rt
    :return: ignite中的字段信息（字段名称，字段类型）
    """
    ignite, physical_tn, conn_info = _get_storage_info(rt_info)

    ptn_arr = physical_tn.split(".")
    format_param = {
        "TABLE_SCHEMA": ptn_arr[0].upper(),
        "TABLE_NAME": ptn_arr[1].upper(),
    }
    sql = IGNITE_SHOW_COLUMN_SQL.format(**format_param)
    conn_str = _generate_conn_str(conn_info)

    column_result = _get_sql_result(
        ignite[STORAGE_CLUSTER][CLUSTER_NAME], conn_str, format_param["TABLE_SCHEMA"], sql, COLUMN_FIELDS
    )

    result = {}
    if column_result:
        for column in column_result:
            # 跳过一些时间字段，以及timestamp/offset字段
            col = column["column_name"].lower()

            if not is_datalab_rt and col in EXCEPT_FIELDS:
                continue

            if col not in IGNITE_SYSTEM_FIELDS:
                result[col] = column["type_name"].lower()
    logger.info(f"{rt_info[RESULT_TABLE_ID]}: ignite columns {result}.")
    return result


def _is_table_exist(connection, schema, table_name):
    """
    判断table是否存在
    :param connection: 数据库连接
    :param schema: schema名称
    :param table_name: table名称
    :return: True/False
    """
    # INFORMATION_SCHEMA 中所有表所有数据均已转为大写,需要使用大写才能查询
    format_param = {"TABLE_SCHEMA": schema.upper(), "TABLE_NAME": table_name.upper()}
    result = _get_sql_result(
        connection["cluster.name"],
        _generate_conn_str(connection),
        schema,
        IGNITE_SHOW_TABLE_SQL.format(**format_param),
        TABLE_FIELDS,
    )

    return True if result else False


def _generate_conn_str(connection):
    """
    生成连接参数
    :param connection: 数据库连接
    :return: ignite连接参数
    """
    conn_array = connection[HOST].split(",")
    return IGNITE_CONN % (
        conn_array[random.randint(0, len(conn_array) - 1)],
        connection[PORT],
        connection.get(USER, ""),
        connection.get(PASSWORD, ""),
    )


def _get_rt_table_columns(rt_fields, is_datalab_rt):
    """
    获取rt的字段转换为ignite中表字段后的字段信息
    :param rt_fields: rt的字段信息
    :param is_datalab_rt: 是否是数据探索rt
    :return: rt字段转换为ignite字段后的字段信息
    """
    result = {}
    for field in rt_fields:
        name = field[FIELD_NAME]
        # 跳过一些时间字段，以及timestamp/offset字段
        if not is_datalab_rt and name.lower() in EXCEPT_FIELDS:
            continue

        ignite_type = _translate_ignite_field_type(field[FIELD_TYPE])
        result[name] = ignite_type

    return result


def _translate_ignite_field_type(field_type):
    """
    将rt的字段类型转换为ignite中的字段类型
    :param field_type: rt的字段类型
    :return: ignite中数据类型
    """
    # 如果没找到匹配的数据类型，使用text类型
    return IGNITE_TYPE_MAPPING.get(field_type, VARCHAR)


def _check_rt_ignite_fields(rt_table_columns, ignite_columns):
    """
    对比rt的字段，和ignite物理表字段的区别
    :param rt_table_columns: rt的字段转换为ignite中字段后的字段信息
    :param ignite_columns: ignite物理表字段
    :return: (append_fields, bad_fields)，需变更增加的字段 和 有类型修改的字段
    """
    append_fields, bad_fields = [], []
    for col_name, col_type in rt_table_columns.items():
        # ignite配置默认会转换为大写，这里统一转化为小写
        col_name_lower = col_name.lower()
        if col_name_lower in ignite_columns.keys():
            # 再对比类型
            if col_type != ignite_columns[col_name_lower]:
                bad_fields.append({col_name: f"({ignite_columns[col_name_lower]} != {col_type})"})
        else:
            append_fields.append({FIELD_NAME: col_name, FIELD_TYPE: col_type})

    return append_fields, bad_fields
