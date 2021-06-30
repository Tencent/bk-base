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
import re
import time

import pymysql
from common.base_crypt import BaseCrypt
from common.log import logger
from datahub.common.const import (
    APPEND_FIELDS,
    BAD_FIELDS,
    BIGINT,
    CHECK_DIFF,
    CHECK_RESULT,
    CONNECTION,
    CONNECTION_INFO,
    DTEVENTTIMESTAMP,
    EXPIRES,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    HOST,
    INDEXED_FIELDS,
    INFO,
    INT,
    LONGTEXT,
    MYSQL,
    MYSQL_FIELDS,
    PASSWORD,
    PHYSICAL_TABLE_NAME,
    PORT,
    RESULT_TABLE_ID,
    RT_FIELDS,
    SAMPLE,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    STRING,
    TABLE,
    TEXT,
    THEDATE,
    USER,
)
from datahub.storekit import exceptions, model_manager, util
from datahub.storekit.exceptions import MySQLConnDBException, MysqlExecuteException
from datahub.storekit.settings import (
    ADD_PARTITION,
    CREATE_TABLE_BASIC_SQL,
    CREATE_TABLE_LAST_SQL,
    DROP_PARTITION,
    EXCEPT_FIELDS,
    MAX_PARTITION,
    MYSQL_TYPE_MAPPING,
    PARTITION_DAYS,
    REORGANIZE_PARTITION,
    SAMPLE_DATA_SQL,
    SHOW_TABLE_SQL,
)


def initialize(rt_info):
    """
    初始化rt的mysql存储
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """
    return prepare(rt_info)


def info(rt_info):
    """
    获取rt的mysql存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的mysql相关信息
    """
    mysql = rt_info[STORAGES][MYSQL]
    physical_tn = mysql[PHYSICAL_TABLE_NAME]
    conn_info = json.loads(mysql[STORAGE_CLUSTER][CONNECTION_INFO])
    conn = get_mysql_connection(conn_info)
    table_info = get_sql_result(conn, SHOW_TABLE_SQL % physical_tn)
    table_info = table_info[0][1] if table_info else None
    mysql[INFO] = {TABLE: table_info, SAMPLE: get_sql_result(conn, SAMPLE_DATA_SQL % physical_tn)}
    conn.close()
    return mysql


def alter(rt_info):
    """
    修改rt的mysql存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的mysql存储的变更结果
    """
    return prepare(rt_info)


def delete(rt_info):
    """
    删除rt的mysql存储相关配置
    :param rt_info: rt的配置信息
    :return: rt的mysqls存储清理结果
    """
    # 对于mysql中表删除，考虑通过rename表名，在尾部标记to_delete，然后定期清理的方式
    mysql = rt_info[STORAGES][MYSQL]
    physical_tn = mysql[PHYSICAL_TABLE_NAME]
    ptn_arr = physical_tn.split(".")
    conn_info = json.loads(mysql[STORAGE_CLUSTER][CONNECTION_INFO])
    conn = get_mysql_connection(conn_info)
    del_suffix = "_to_delete"
    if get_sql_result(conn, f"SHOW TABLES IN {ptn_arr[0]} LIKE '{ptn_arr[1]}{del_suffix}'"):
        # 如果rename为xx_to_delete的表已经存在，则先drop掉此表
        get_sql_result(conn, f"DROP TABLE {physical_tn}{del_suffix}")
    rename_table_sql = f"RENAME TABLE {physical_tn} TO {physical_tn}{del_suffix}"
    get_sql_result(conn, rename_table_sql)
    conn.close()
    return True


def prepare(rt_info):
    """
    准备rt关联的mysql存储（创建新库表或旧表新增字段）
    :param rt_info: rt的配置信息
    :return: True/False
    """
    mysql = rt_info[STORAGES][MYSQL]
    physical_tn = mysql[PHYSICAL_TABLE_NAME]
    ptn_arr = physical_tn.split(".")
    conn_info = json.loads(mysql[STORAGE_CLUSTER][CONNECTION_INFO])
    conn = get_mysql_connection(conn_info)
    # 首先获取表信息，如果表不存在，则走建库建表流程
    if not get_sql_result(conn, f"SHOW TABLES IN {ptn_arr[0]} LIKE '{ptn_arr[1]}'"):
        # 尝试建库
        get_sql_result(conn, f"CREATE DATABASE IF NOT EXISTS {ptn_arr[0]}")
        create_sql = generate_create_table_sql(rt_info)
        get_sql_result(conn, create_sql, True)
    else:
        # 如果表存在，则对比schema，走变更表流程
        table_cols = get_mysql_table_columns(conn, physical_tn)
        rt_cols = get_rt_table_columns(rt_info[FIELDS])
        append_fields, bad_fields = check_rt_mysql_fields(rt_cols, table_cols)
        if bad_fields:
            raise exceptions.SchemaMismatchError(
                message_kv={
                    "cols": json.dumps([list(i.values())[0] for i in bad_fields]),
                    "msg": json.dumps(bad_fields),
                }
            )
        elif append_fields:
            alter_sql = get_alter_table_sql(physical_tn, append_fields)
            get_sql_result(conn, alter_sql, True)
        else:
            logger.info(f"{rt_info[RESULT_TABLE_ID]} has no old fields with field type modified or new fields added")
    conn.close()
    return True


def maintain(rt_info, delta_day=1):
    """
    维护mysql表的分区
    :param rt_info: rt的配置信息
    :param delta_day 维护日期偏移量
    """
    mysql = rt_info[STORAGES][MYSQL]
    physical_tn = mysql[PHYSICAL_TABLE_NAME]
    conn_info = json.loads(mysql[STORAGE_CLUSTER][CONNECTION_INFO])
    conn = get_mysql_connection(conn_info)
    table_info = get_sql_result(conn, SHOW_TABLE_SQL % physical_tn)
    if table_info:
        pattern = re.compile(r"PARTITION p(\d+) VALUES LESS THAN")
        partitions = pattern.findall(table_info[0][1])  # 包含分区日期的列表
        if partitions:
            # 非分区表无法改为分区表，只有对分区表才进行下面的逻辑
            add_date = util.get_date_by_diff(delta_day)  # 添加分区的日期 continue
            add_next_date = util.get_date_by_diff(delta_day + 1)
            if add_date not in partitions:
                if MAX_PARTITION in partitions:
                    add_sql = REORGANIZE_PARTITION % (
                        physical_tn,
                        MAX_PARTITION,
                        add_date,
                        add_next_date,
                        MAX_PARTITION,
                    )
                else:
                    add_sql = ADD_PARTITION % (physical_tn, add_date, add_next_date, MAX_PARTITION)
                get_sql_result(conn, add_sql)

            expires_day = util.translate_expires_day(mysql[EXPIRES])
            if expires_day > 0:
                # 数据需要过期，则计算数据过期的时间，然后删除对应的分区
                delete_date = util.get_date_by_diff(-(expires_day + 1))  # 加一天，expires内的都是需要保留的，超出的需删除
                drop_partitions = []
                for partition in partitions:
                    if int(partition) <= int(delete_date):
                        drop_partitions.append(f"p{partition}")
                if drop_partitions:
                    drop_sql = DROP_PARTITION % (physical_tn, ", ".join(drop_partitions))
                    get_sql_result(conn, drop_sql)
    conn.close()


def maintain_all(delta_day=1):
    """
    维护所有的mysql表的分区
    """
    storage_rt_list = model_manager.get_storage_rt_objs_by_type(MYSQL)
    count = 0
    start_time_ts = time.time()
    mysql_conns = {}
    maintain_failed_rts = []
    # 避免此接口执行时间跨天，导致添加的分区发生变化。
    add_date = util.get_date_by_diff(delta_day)
    add_next_date = util.get_date_by_diff(delta_day + 1)
    for rt_storage in storage_rt_list:
        try:
            # 逐条处理，生成对应的actions，然后执行，避免将actions放入一个dict再遍历一轮
            count += 1
            cluster_name = rt_storage.storage_cluster_config.cluster_name
            physical_tn = rt_storage.physical_table_name
            if cluster_name not in mysql_conns:
                mysql_cluster = model_manager.get_storage_cluster_config(cluster_name, MYSQL)
                if mysql_cluster:
                    # TODO 处理无法连接mysql的异常
                    conn = get_mysql_connection(mysql_cluster[CONNECTION])
                    mysql_conns[cluster_name] = conn
            mysql_conn = mysql_conns[cluster_name]
            table_info = get_sql_result(mysql_conn, SHOW_TABLE_SQL % physical_tn)
            if table_info:
                pattern = re.compile(r"PARTITION p(\d+) VALUES LESS THAN")
                partitions = pattern.findall(table_info[0][1])  # 包含分区日期的列表
                if not partitions:
                    maintain_failed_rts.append(rt_storage.result_table_id)
                    continue
                if add_date not in partitions:
                    if MAX_PARTITION in partitions:
                        add_sql = REORGANIZE_PARTITION % (
                            physical_tn,
                            MAX_PARTITION,
                            add_date,
                            add_next_date,
                            MAX_PARTITION,
                        )
                    else:
                        add_sql = ADD_PARTITION % (physical_tn, add_date, add_next_date, MAX_PARTITION)
                    get_sql_result(mysql_conn, add_sql)

                expires_day = util.translate_expires_day(rt_storage.expires)
                if expires_day > 0:
                    # 数据需要过期，则计算数据过期的时间，然后删除对应的分区
                    delete_date = util.get_date_by_diff(-(expires_day + 1))  # 加一天，expires内的都是需要保留的，超出的需删除
                    drop_partitions = []
                    for partition in partitions:
                        if int(partition) <= int(delete_date):
                            drop_partitions.append(f"p{partition}")
                    if drop_partitions:
                        drop_sql = DROP_PARTITION % (physical_tn, ", ".join(drop_partitions))
                        get_sql_result(mysql_conn, drop_sql)
            else:
                maintain_failed_rts.append(rt_storage.result_table_id)
        except Exception:
            maintain_failed_rts.append(rt_storage.result_table_id)

    for conn in list(mysql_conns.values()):
        conn.close()
    # 记录总共处理的rt数量，以及异常的rt列表
    logger.info(
        f"{count} mysql rts maintain takes {int(time.time() - start_time_ts)}(s), "
        f"failed are {json.dumps(maintain_failed_rts)}"
    )


def check_schema(rt_info):
    """
    校验RT的字段（名字、类型）的修改是否满足存储的限制
    :param rt_info: rt的配置信息
    :return: rt字段和存储字段的schema对比
    """
    mysql = rt_info[STORAGES][MYSQL]
    physical_tn = mysql[PHYSICAL_TABLE_NAME]
    ptn_arr = physical_tn.split(".")
    conn_info = json.loads(mysql[STORAGE_CLUSTER][CONNECTION_INFO])
    conn = get_mysql_connection(conn_info)

    result = {RT_FIELDS: {}, MYSQL_FIELDS: {}, CHECK_RESULT: True, CHECK_DIFF: {}}
    for field in rt_info[FIELDS]:
        if field[FIELD_NAME].lower() in EXCEPT_FIELDS:
            continue
        result[RT_FIELDS][field[FIELD_NAME]] = field[FIELD_TYPE]

    if get_sql_result(conn, f"SHOW TABLES IN {ptn_arr[0]} LIKE '{ptn_arr[1]}'"):
        # 表已经创建，检查字段的变化情况
        table_cols = get_mysql_table_columns(conn, physical_tn)
        rt_cols = get_rt_table_columns(rt_info[FIELDS])
        append_fields, bad_fields = check_rt_mysql_fields(rt_cols, table_cols)

        result[MYSQL_FIELDS] = table_cols
        result[CHECK_DIFF] = {APPEND_FIELDS: append_fields, BAD_FIELDS: bad_fields}
        if bad_fields:
            result[CHECK_RESULT] = False
    conn.close()

    return result


def clusters():
    """
    获取mysql存储集群列表
    :return: mysql存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(MYSQL)
    return result


def generate_create_table_sql(rt_info):
    """
    生成mysql的建表语句，包含表字段、索引和分区（最近7天，每天一个分区）
    :param rt_info: rt的配置信息
    :return: mysql的建表语句
    """
    indexed_fields = json.loads(rt_info[STORAGES][MYSQL][STORAGE_CONFIG]).get(INDEXED_FIELDS, [])
    rt_mysql_cols = get_rt_table_columns(rt_info[FIELDS])

    field_sql = ""
    for field in rt_info[FIELDS]:
        name = field[FIELD_NAME]
        if name in rt_mysql_cols:
            mysql_type = rt_mysql_cols[name]
            if name in indexed_fields:
                if field[FIELD_TYPE] in [STRING, TEXT, LONGTEXT]:
                    idx = f"KEY `ind_{name}` (`{name}`(30), `{DTEVENTTIMESTAMP}`) "
                else:
                    idx = f"KEY `ind_{name}` (`{name}`, `{DTEVENTTIMESTAMP}`)"
                field_sql = f"{field_sql} `{name}` {mysql_type} DEFAULT NULL, {idx}, "
            else:
                field_sql = f"{field_sql} `{name}` {mysql_type} DEFAULT NULL, "
    sql = CREATE_TABLE_BASIC_SQL + field_sql + CREATE_TABLE_LAST_SQL + get_partition_sql(PARTITION_DAYS)
    return sql.replace("`<TABLE_NAME>`", rt_info[STORAGES][MYSQL][PHYSICAL_TABLE_NAME])


def get_partition_sql(days):
    """
    按照天数生成分区的sql
    :param days: 分区中包含的天数，每天一个分区
    :return: 分区的sql
    """
    partitions = f"PARTITION BY RANGE (`{THEDATE}`) ("
    for i in range(days):
        day = util.get_date_by_diff(i)
        next_day = util.get_date_by_diff(1 + i)
        partitions += f"PARTITION `p{day}` VALUES LESS THAN ({next_day}), "
    partitions += f"PARTITION `p{MAX_PARTITION}` VALUES LESS THAN MAXVALUE) "

    return partitions


def get_alter_table_sql(physical_table_name, append_fields):
    """
    生成表变更语句，在尾部增加字段
    :param physical_table_name: 物理表名称
    :param append_fields: 增加的字段和类型信息
    :return: 变更的表sql语句
    """
    sql_columns = []
    for column in append_fields:
        sql_columns.append(f"ADD {column[FIELD_NAME]} {column[FIELD_TYPE]}")
    return f'ALTER TABLE {physical_table_name} {", ".join(sql_columns)}'


def translate_mysql_field_type(field_type, raw=False):
    """
    将rt的字段类型转换为mysql中的字段类型
    :param field_type: rt的字段类型
    :param raw: 是否返回mysql数据类型
    :return: mysql中数据类型
    """
    # 如果没找到匹配的数据类型，使用text类型
    mysql_type = MYSQL_TYPE_MAPPING.get(field_type, TEXT)
    if not raw:
        if mysql_type == INT:
            mysql_type = f"{mysql_type}(11)"
        elif mysql_type == BIGINT:
            mysql_type = f"{mysql_type}(20)"
    return mysql_type


def get_mysql_table_columns(conn, physical_table_name):
    """
    获取mysql中表的字段信息，不包含系统添加的时间字段
    :param conn: mysql数据库连接
    :param physical_table_name: 物理表名
    :return: mysql中的字段信息（字段名称，字段类型）
    """
    sql = f"DESC {physical_table_name}"
    entries = get_sql_result(conn, sql)
    result = {}
    if entries:
        for entry in entries:
            # 跳过一些时间字段，以及timestamp/offset字段
            col = entry[0].lower()
            if col not in EXCEPT_FIELDS:
                result[col] = entry[1]

    return result


def get_rt_table_columns(rt_fields, raw=False):
    """
    获取rt的字段转换为mysql中表字段后的字段信息
    :param rt_fields: rt的字段信息
    :param raw: 是否返回mysql数据类型
    :return: rt字段转换为mysql字段后的字段信息
    """
    result = {}
    for field in rt_fields:
        name = field[FIELD_NAME]
        # 跳过一些时间字段，以及timestamp/offset字段
        if name.lower() not in EXCEPT_FIELDS:
            mysql_type = translate_mysql_field_type(field[FIELD_TYPE], raw)
            result[name] = mysql_type

    return result


def check_rt_mysql_fields(rt_table_columns, mysql_columns):
    """
    对比rt的字段，和mysql物理表字段的区别
    :param rt_table_columns: rt的字段转换为mysql中字段后的字段信息
    :param mysql_columns: mysql物理表字段
    :return: (append_fields, bad_fields)，需变更增加的字段 和 有类型修改的字段
    """
    append_fields, bad_fields = [], []
    for col_name, col_type in list(rt_table_columns.items()):
        # 先判断rt的字段名称，是否在mysql中。将字段改为小写进行对比，mysql中不区分字段名称大小写
        col_name_lower = col_name.lower()
        if col_name_lower in list(mysql_columns.keys()):
            # 再对比类型
            if col_type != mysql_columns[col_name_lower]:
                bad_fields.append({col_name: f"({mysql_columns[col_name_lower]} != {col_type})"})
        else:
            append_fields.append({FIELD_NAME: col_name, FIELD_TYPE: col_type})

    return append_fields, bad_fields


def get_mysql_connection(conn_info):
    """
    获取mysql(tspider)数据库连接
    :param conn_info: 集群连接信息，字典
    :return: mysql连接
    """
    try:
        conn = pymysql.connect(
            host=conn_info[HOST],
            port=conn_info[PORT],
            user=conn_info[USER],
            passwd=BaseCrypt.bk_crypt().decrypt(conn_info[PASSWORD]),
            use_unicode=1,
            charset="utf8",
        )
        conn.autocommit(True)
    except Exception:
        logger.error(f"connect to mysql failed，conn_info: {json.dumps(conn_info)}")
        raise MySQLConnDBException()
    return conn


def is_db_exist(connection, db_name):
    """
    判断db是否存在
    :param connection: 数据库连接
    :param db_name: 库名
    :return: True/False
    """
    sql = f"SHOW DATABASES LIKE '{db_name}'"
    dbs = [i[0] for i in get_sql_result(connection, sql)]
    return db_name in dbs


def get_sql_result(conn, sql, raise_exception=False):
    """
    执行sql语句，将执行结果返回
    :param conn: mysql(tspider)的数据库连接
    :param sql: 执行的sql语句
    :param raise_exception: 发生异常时，是否抛出异常
    :return: 执行结果
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"execute sql exception: {e}, sql: {sql}")
        if raise_exception:
            raise MysqlExecuteException()
    finally:
        cursor.close()
    return None
