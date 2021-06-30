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
import traceback

from common.log import logger
from datahub.common.const import BIGINT, FIELD_NAME, FIELD_TYPE, LONG, STRING, TEXT
from datahub.storekit import util
from datahub.storekit.exceptions import NotFoundHiveServer
from datahub.storekit.settings import (
    DAY_HOURS,
    EXCEPT_FIELDS,
    HDFS_DEFAULT_COLUMNS,
    HIVE_ADD_COLUMNS_SQL,
    HIVE_ADD_PARTITION_SQL,
    HIVE_CREATE_DB,
    HIVE_CREATE_TABLE_BASIC_SQL,
    HIVE_DESC_TABLE_SQL,
    HIVE_DROP_PARTITION_SQL,
    HIVE_DROP_TABLE_SQL,
    HIVE_LAST_SQL,
    HIVE_LAST_SQL_NO_PART,
    HIVE_ONE_PARTITION_SQL,
    HIVE_SERVER,
    HIVE_TABLE_EXISTS_SQL,
    RTX_RECEIVER,
    SKIP_RT_FIELDS,
)
from pyhive import hive
from TCLIService import ttypes


def get_hive_connection(geog_area_code):
    """
    :param geog_area_code: 区域code
    创建hive server的连接，用于后续执行sql。默认连接default数据库。
    :return: 连接hive server的数据库连接
    """
    # 查询结果表区域，根据区域选择hiveServer(可能存在多个，通过consul解析，做容灾处理)

    hive_server = HIVE_SERVER.get(geog_area_code)
    if not hive_server:
        raise NotFoundHiveServer()

    hive_server_pass = hive_server["HIVE_SERVER_PASS"] if hive_server["HIVE_SERVER_PASS"] else None
    return hive.connect(
        host=hive_server["HIVE_SERVER_HOST"],
        port=hive_server["HIVE_SERVER_PORT"],
        username=hive_server["HIVE_SERVER_USER"],
        database="default",
        password=hive_server_pass,
    )


def exe_hive_ddl_sql(conn, ddl_sql):
    """
    执行Hive DDL语句，用于建表、添加分区、建库等
    :param conn: hive的数据库连接
    :return: 执行结果 True/False
    """
    from TCLIService import ttypes

    cursor = conn.cursor()
    try:
        cursor.execute(ddl_sql)
        result = cursor.poll()  # 获取执行结果
        # 判断执行结果
        if result.operationState == ttypes.TOperationState.FINISHED_STATE and result.status.statusCode in [
            ttypes.TStatusCode.SUCCESS_STATUS,
            ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS,
        ]:
            return True
        else:
            logger.warning(f"execute hive sql {ddl_sql} result: {result}")
            return False
    except Exception as e:  # pyhive.exc.OperationalError
        error_msg = traceback.format_exc()
        logger.info(f"execute hive sql failed {ddl_sql}. {error_msg}")
        util.wechat_msg(RTX_RECEIVER, f"execute hive sql failed {ddl_sql[0:120]}... {error_msg}")
        util.mail_msg(RTX_RECEIVER, f"execute hive sql failed {ddl_sql}. {error_msg}")
        if error_msg.__contains__("Broken pipe"):
            raise e
    finally:
        cursor.close()

    return False


def exe_hive_sql_get_result(conn, sql):
    """
    执行Hive sql语句，将执行结果返回
    :param conn: hive的数据库连接
    :return: 执行结果 True/False
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.poll()  # 获取执行结果
        # 判断执行结果
        if result.operationState == ttypes.TOperationState.FINISHED_STATE and result.status.statusCode in [
            ttypes.TStatusCode.SUCCESS_STATUS,
            ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS,
        ]:
            return cursor.fetchall()
        else:
            logger.warning(f"execute hive sql {sql} result: {result}")
    except Exception as e:  # pyhive.exc.OperationalError
        error_msg = traceback.format_exc()
        logger.info(f"execute hive sql failed {sql}. {error_msg}")
        util.wechat_msg(RTX_RECEIVER, f"execute hive sql failed {sql[0:120]}... {error_msg}")
        util.mail_msg(RTX_RECEIVER, f"execute hive sql failed {sql}. {error_msg}")
        if error_msg.__contains__("Broken pipe"):
            raise e
    finally:
        cursor.close()

    return None


def trans_fields_to_hive_fields(fields, exclude_default_columns=False):
    """
    将rt表中的字段转换为hive表中建表语句字符串的column定义部分
    :param fields: rt表中的字段列表
    :param exclude_default_columns: 属否包含默认字段
    :return: 转换后的建表语句中column部分
    """
    columns = [] if exclude_default_columns else copy.deepcopy(HDFS_DEFAULT_COLUMNS)

    # 正常情况下，rt_info 中不会存在内部时间字段。但是不排查历史表会有
    for field in fields:
        if field[FIELD_NAME].lower() in SKIP_RT_FIELDS or (
            not exclude_default_columns and field[FIELD_NAME].lower() in EXCEPT_FIELDS
        ):
            continue

        field_name = f"`{field[FIELD_NAME]}`"  # 转义所有的字段名称，避免关键字冲突，以及不合法的字段名
        if field[FIELD_TYPE] == LONG:
            columns.append(f"{field_name} {BIGINT}")
        elif field[FIELD_TYPE] == TEXT:
            columns.append(f"{field_name} {STRING}")
        else:
            columns.append(f"{field_name} {field[FIELD_TYPE]}")

    return columns


def generate_hive_create_db(biz_id):
    """
    构建hive创建数据库的语句，避免后续执行建表语句时抛出数据库不存在异常
    :param biz_id: 业务ID
    :return: hive的建库语句
    """
    return HIVE_CREATE_DB.format(biz_id=biz_id)


def generate_hive_drop_table(biz_id, table_name):
    """
    构建hive中drop表的sql语句
    :param biz_id: 业务ID
    :param table_name: 表名
    :return: hive的drop表语句
    """
    return HIVE_DROP_TABLE_SQL.format(biz_id=biz_id, table_name=table_name)


def generate_hive_is_table_exist(biz_id, table_name):
    """
    生成检查hive中表是否存在的sql语句
    :param biz_id: 业务ID
    :param table_name: 表名
    :return: hive的检查表是否存在的sql语句
    """
    return HIVE_TABLE_EXISTS_SQL.format(biz_id=biz_id, table_name=table_name)


def generate_hive_desc_table(biz_id, table_name):
    """
    生成hive中描述表的sql语句
    :param biz_id: 业务ID
    :param table_name: 表名
    :return: hive的描述表的sql语句
    """
    return HIVE_DESC_TABLE_SQL.format(biz_id=biz_id, table_name=table_name)


def generate_hive_add_table_columns(biz_id, table_name, columns):
    """
    生成hive变更表结构，增加字段的SQL语句
    :param biz_id: 业务ID
    :param table_name: 表名
    :param columns: 需要增加的字段信息
    :return: hive的变更表结构，增加字段的SQL
    """
    return HIVE_ADD_COLUMNS_SQL.format(biz_id=biz_id, table_name=table_name, columns=columns)


def generate_hive_create_table_sql(
    biz_id, table_name, physical_table_name, columns, hdfs_url, data_type, with_partition_sql=True
):
    """
    构建hive建表语句
    :param biz_id: 业务ID
    :param table_name: hive中的表名称，相当于物理表的最后一段字符串
    :param physical_table_name: 物理表名，包含hdfs上的目录路径
    :param columns: column字段定义
    :param hdfs_url: hdfs集群地址
    :param data_type: 数据类型（parquet/json）
    :param with_partition_sql: 是否包含分区，默认包含
    :return: 建表的SQL语句
    """
    # 建表SQL
    sql = HIVE_CREATE_TABLE_BASIC_SQL.format(biz_id=biz_id, table_name=table_name, columns=columns)
    if with_partition_sql:
        sql += HIVE_LAST_SQL[data_type].format(
            hdfs_url=hdfs_url, biz_id=biz_id, physical_table_name=physical_table_name
        )
    else:
        sql += HIVE_LAST_SQL_NO_PART[data_type].format(
            hdfs_url=hdfs_url, biz_id=biz_id, physical_table_name=physical_table_name
        )

    return sql


def generate_hive_add_partition(biz_id, table_name, physical_table_name, days_before):
    """
    根据过期时间配置构建hive表的新增分区sql语句，其中包含所有过期日期内的，以及今天的分区目录
    :param biz_id: 业务ID
    :param table_name: hive中的表名
    :param physical_table_name: 物理表名，包含hdfs上的目录路径
    :param days_before: 和当前日期相差的天数
    :return: 添加分区的SQL语句
    """
    add_sql = []
    # 这里需要建所有过期日期内的，以及今天和明天的分区目录，避免当天的维护任务已经执行，错过添加明天的分区机会
    for n in range(-days_before, 2):
        date = util.get_date_by_diff(n)
        add_sql.append(generate_hive_date_partitions(physical_table_name, date))
    return HIVE_ADD_PARTITION_SQL.format(biz_id=biz_id, table_name=table_name, partitions=" ".join(add_sql))


def generate_hive_add_partition_by_date(biz_id, table_name, physical_table_name, date):
    """
    按照日期生成添加当天所有分区的变更SQL语句
    :param biz_id: 业务ID
    :param table_name: hive中的表名
    :param physical_table_name: 物理表名，包含hdfs上的目录路径
    :param date: 日期字符串（yyyymmdd）
    :return: 包含指定日期的所有分区的SQL。
    """
    sql = generate_hive_date_partitions(physical_table_name, date)
    return HIVE_ADD_PARTITION_SQL.format(biz_id=biz_id, table_name=table_name, partitions=sql)


def generate_hive_date_partitions(physical_table_name, date):
    """
    按照日期生成当天24小时的分区记录
    :param physical_table_name: 物理表名，包含hdfs上的目录路径
    :param date: 日期字符串（yyyymmdd）
    :return: 包含当天24小时分区的字符串
    """
    partitions = []
    for hour in DAY_HOURS:
        datehour = int(f"{date}{hour}")
        partitions.append(
            HIVE_ONE_PARTITION_SQL.format(
                physical_table_name=physical_table_name,
                datehour=datehour,
                year=date[0:4],
                month=date[4:6],
                day=date[6:8],
                hour=hour,
            )
        )

    return " ".join(partitions)


def generate_hive_drop_date_partitions(biz_id, table_name, date):
    """
    按照日期生成删除当天hive表中对应分区的SQL语句
    :param biz_id: 业务ID
    :param table_name: 表名
    :param date: 日期
    :return: 删除指定日期中hive表分区的SQL语句
    """
    partitions = []
    for hour in DAY_HOURS:
        partitions.append(f"PARTITION(dt_par_unit={date}{hour})")
    return HIVE_DROP_PARTITION_SQL.format(biz_id=biz_id, table_name=table_name, partitions=", ".join(partitions))
