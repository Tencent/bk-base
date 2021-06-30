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

import logging
import os
import threading
import time
from logging import NullHandler

import jpype
import jpype.imports
from jpype.types import JClass, JDouble, JFloat, JInt, JLong, JObject

# Set default logging handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

START_LOCK = threading.Lock()

STORAGES = "storages"
HDFS = "hdfs"
DATA_TYPE = "data_type"
ICEBERG = "iceberg"
PHYSICAL_TABLE_NAME = "physical_table_name"
CONNECTION_INFO = "connection_info"
STORAGE_CLUSTER = "storage_cluster"
STORAGE_CONFIG = "storage_config"
DFS_REPLICATION = "dfs.replication"
HDFS_DEFAULT_PARAMS = "hdfs_default_params"
HDFS_CLUSTER_NAME = "hdfs_cluster_name"
HOSTS = "hosts"
PORT = "port"
RPC_PORT = "rpc_port"
SERVICERPC_PORT = "servicerpc_port"
IDS = "ids"
HDFS_URL = "hdfs_url"
FS_DEFAULTFS = "fs.defaultFS"
DFS_NAMESERVICES = "dfs.nameservices"
DFS_HA_NAMENODES = "dfs.ha.namenodes"
DFS_CLIENT_FAILOVER_PROXY_PROVIDER = "dfs.client.failover.proxy.provider"
DFS_NAMENODE_RPC_ADDRESS = "dfs.namenode.rpc-address"
DFS_NAMENODE_SERVICERPC_ADDRESS = "dfs.namenode.servicerpc-address"
DFS_NAMENODE_HTTP_ADDRESS = "dfs.namenode.http-address"
DTEVENTTIMESTAMP = "dtEventTimeStamp"
ET = "____et"  # 默认分区字段
INT = "int"
LONG = "long"
FLOAT = "float"
DOUBLE = "double"
STRING = "string"
TEXT = "text"
TIMESTAMP = "timestamp"
IN = "in"
NOT_IN = "not in"
IS_NULL = "is null"
NOT_NULL = "not null"
JCLASS_EXPRESSION = "org.apache.iceberg.expressions.Expression"
JCLASS_EXPRESSIONS = "org.apache.iceberg.expressions.Expressions"
JCLASS_BKTABLE = "com.tencent.bk.base.datahub.iceberg.BkTable"
JCLASS_DATABUFFER = "com.tencent.bk.base.datahub.iceberg.DataBuffer"
JCLASS_ASSIGN = "com.tencent.bk.base.datahub.iceberg.functions.Assign"
JCLASS_GENERICRECORD = "org.apache.iceberg.data.GenericRecord"
JCLASS_INSTANT = "java.time.Instant"
JCLASS_OFFSETDATETIME = "java.time.OffsetDateTime"
JCLASS_ZONEOFFSET = "java.time.ZoneOffset"
JCLASS_ARRAYLIST = "java.util.ArrayList"
JCLASS_HASHMAP = "java.util.HashMap"
JCLASS_HASHSET = "java.util.HashSet"
JCLASS_INTEGER = "java.lang.Integer"
JCLASS_LONG = "java.lang.Long"
JCLASS_FLOAT = "java.lang.Float"
JCLASS_DOUBLE = "java.lang.Double"

LOCAL_FILE_SYSTEM = "file:///"
COMMIT_INTERVAL = 600000  # 600 seconds
COMMIT_LIMIT = 1000000  # one million records


def start_jvm_on_need(jvm_args=None):
    """
    使用lib目录下的jar包启动jvm，便于在python中调用datalake sdk中的方法。
    @:param jvm_args 传递给java虚拟机的参数，例如内存参数大小，GC配置等
    """
    with START_LOCK:
        if not jpype.isJVMStarted():
            cps = []
            # 获取lib目录下所有jar包，加入到启动classpath里
            current_path = os.path.abspath(__file__)
            lib_path = os.path.join(os.path.dirname(current_path), "lib")
            for path, dir_list, file_list in os.walk(lib_path):
                for file_name in file_list:
                    if file_name.endswith(".jar"):
                        cps.append(path + os.path.sep + file_name)

            # 启动jvm
            if jvm_args is None:
                jpype.startJVM(classpath=cps)
            else:
                assert isinstance(jvm_args, list), "jvm_args should be list"
                jvm_path = jpype.getDefaultJVMPath()
                jpype.startJVM(jvm_path, *jvm_args, classpath=cps)

            logger.info("started jvm {} with args {} and jars {}".format(jpype.getJVMVersion(), jvm_args, cps))

    logger.info("%s: waiting and checking jvm status until started..." % threading.current_thread().name)
    cnt_down = 15  # 如果15s内JVM还未启动，一定是环境有问题了
    while cnt_down > 0 and not jpype.isJVMStarted():
        # 等待jvm启动成功，可能是其他线程在调用
        time.sleep(1)
        cnt_down -= 1

    if cnt_down == 0:
        raise RuntimeError("jvm not started in 15 seconds, unable to continue!")


def check_jvm_status():
    """
    检查jvm状态，如果jvm未启动，则抛出运行时异常
    """
    if not jpype.isJVMStarted():
        raise RuntimeError("should start jvm first!")


def _find_column_index(column_names, column_name):
    """
    寻找字段在字段名称数组中的索引编号，忽略大小写
    :param column_names: 字段名称数组，一维
    :param column_name: 字段名称
    :return: 字段索引编号，没找到时返回-1
    """
    for idx in range(0, len(column_names)):
        if column_name.lower() == column_names[idx].lower():
            return idx
    return -1


def _cast_value_by_data_type(field_type, value):
    """
    将python的数据类型转换为java能识别的数据类型
    :param field_type: 字段类型，小写字符
    :param value: 字段值
    :return: java能识别的数据类型
    """
    if field_type == INT:
        return JInt(int(value))
    elif field_type == LONG:
        return JLong(int(value))
    elif field_type == FLOAT:
        return JFloat(float(value))
    elif field_type == DOUBLE:
        return JDouble(float(value))
    elif field_type in [STRING, TEXT]:
        return str(value)
    else:
        raise TypeError("field type {} not supported! value: {}".format(field_type, value))


def _cast_list_by_data_type(field_type, list_values):
    """
    将list结构的python数据转换为java能识别的数据类型
    :param field_type: 字段类型，小写字符
    :param list_values: list数据
    :return: java能识别的数据类型构成的数组
    """
    if isinstance(list_values, list):
        return [_cast_value_by_data_type(field_type, value) for value in list_values]
    else:
        raise TypeError("not a list of {} type, value: {}".format(field_type, list_values))


def _map_to_python_obj(obj):
    """
    将java对象转换为python对象
    :param obj: java对象
    :return: python对象
    """
    if obj is None:
        return None

    clazz = obj.getClass()
    if clazz in [JClass(JCLASS_INTEGER), JClass(JCLASS_LONG)]:
        return int(obj)
    elif clazz in [JClass(JCLASS_FLOAT), JClass(JCLASS_DOUBLE)]:
        return float(obj)
    else:
        return str(obj.toString())


def build_field_condition_expr(field_name, field_type, operator, right_val):
    """
    构造字段过滤表达式
    :param field_name: 字段名称
    :param field_type: 字段类型
    :param operator: 操作符，支持">=", ">", "=", "!=", "<", "<=", "in", "not in", "is null", "not null"
    :param right_val: 表达式右值，对于in/not in，为list结构的值，其他情况为primary type
    :return: 字段的表达式
    """
    j_expressions = JClass(JCLASS_EXPRESSIONS)
    # 所有字段名称/字段类型/操作符均转为小写字符
    field_name = field_name.lower()
    field_type = field_type.lower()
    operator = operator.lower()
    if operator in [IN, NOT_IN]:
        values = _cast_list_by_data_type(field_type, right_val)
        if operator == NOT_IN:
            return j_expressions.notIn(field_name, values)
        else:
            return j_expressions.in_(field_name, values)
    elif operator in [IS_NULL, NOT_NULL]:
        if operator == IS_NULL:
            return j_expressions.isNull(field_name)
        else:
            return j_expressions.notNull(field_name)
    elif operator in [">=", ">", "=", "!=", "<", "<="]:
        value = _cast_value_by_data_type(field_type, right_val)
        if operator == ">=":
            return j_expressions.greaterThanOrEqual(field_name, value)
        elif operator == ">":
            return j_expressions.greaterThan(field_name, value)
        elif operator == "=":
            return j_expressions.equal(field_name, value)
        elif operator == "!=":
            return j_expressions.notEqual(field_name, value)
        elif operator == "<":
            return j_expressions.lessThan(field_name, value)
        elif operator == "<=":
            return j_expressions.lessThanOrEqual(field_name, value)
    else:
        raise ValueError("field operator {} not valid. {}({}): {}".format(operator, field_name, field_type, right_val))


def build_and_condition(left_expr, right_expr):
    """
    构造and表达式
    :param left_expr: and的左表达式
    :param right_expr: and的右表达式
    :return: and表达式
    """
    j_expressions = JClass(JCLASS_EXPRESSIONS)
    left = JObject(left_expr, JClass(JCLASS_EXPRESSION))
    right = JObject(right_expr, JClass(JCLASS_EXPRESSION))

    return j_expressions.and_(left, right)


def build_or_condition(left_expr, right_expr):
    """
    构造or表达式
    :param left_expr: or的左表达式
    :param right_expr: or的右表达式
    :return: or表达式
    """
    j_expressions = JClass(JCLASS_EXPRESSIONS)
    left = JObject(left_expr, JClass(JCLASS_EXPRESSION))
    right = JObject(right_expr, JClass(JCLASS_EXPRESSION))

    return j_expressions.or_(left, right)


def load_table(db_name, table_name, props):
    """
    加载数据湖中的表对象
    :param db_name: iceberg表的库名称，从物理表名称中获取
    :param table_name: iceberg表的表名称，从物理表名称中获取
    :param props:  读取iceberg表所需的配置项，通过接口 v3/storekit/hdfs/:rt_id/hdfs_conf/ 获取
    :return: 表对象
    """
    check_jvm_status()

    # 构建iceberg表并加载
    j_bktable = JClass(JCLASS_BKTABLE)
    props_str = {str(k): str(v) for (k, v) in props.items()}
    table = j_bktable(db_name, table_name, props_str)
    table.loadTable()

    return table


def scan_table(table, condition_expr=None, column_names=None):
    """
    检索数据湖表里的数据，将符合条件的数据逐条返回。通过next()方法获取一条符合条件的数据。
    :param table: 表对象
    :param condition_expr: 条件表达式，用于匹配符合条件的数据。如果为None，则匹配表中所有数据。
    :param column_names: 字段名称数组，一维，包含meta中字段和存储增加的时间字段。如果为None，默认返回表里所有字段。
    :return: 数据生成器Generator，通过next()方法获取一条数据，或使用for循环遍历。
    """
    j_expressions = JClass(JCLASS_EXPRESSIONS)
    if condition_expr is None:
        condition_expr = j_expressions.alwaysTrue()

    schema = table.schema()
    if column_names is None:
        column_names = [column.name() for column in schema.columns()]
    else:
        assert isinstance(column_names, list), "column names should be list of strings"

    with table.readRecords(condition_expr, column_names) as records:
        for record in records:
            # 数据湖中表的所有字段均为小写
            yield {c: _map_to_python_obj(record.getField(str(c).lower())) for c in column_names}


def read_table_into_list(table, condition_expr=None, column_names=None, limit=None):
    """
    一次读取数据湖表中数据到python的list中。注意：对于读取超大数据集可能导致内存溢出。
    :param table: 表对象
    :param condition_expr: 条件表达式，用于匹配符合条件的数据。如果为None，则匹配表中所有数据。
    :param column_names: 字段名称数组，一维，包含meta中字段和存储增加的时间字段。如果为None，默认返回表里所有字段。
    :param limit: 最多读取数据量
    :return: 结果集，包含纯python的对象
    """
    j_expressions = JClass(JCLASS_EXPRESSIONS)
    if condition_expr is None:
        condition_expr = j_expressions.alwaysTrue()

    if limit is None:
        limit = total_record_count(table)

    schema = table.schema()
    if column_names is None:
        column_names = [column.name() for column in schema.columns()]
    else:
        assert isinstance(column_names, list), "column names should be list of strings"

    result = []
    for elem in table.readRecordsInArray(condition_expr, limit, column_names):
        entry = [_map_to_python_obj(obj) for obj in elem]
        result.append(entry)

    return result


def append_table(table, column_names, column_data, commit_msg):
    """
    向数据湖中的表里面添加数据
    :param table: 表对象
    :param column_names: 字段名称数组，一维，包含meta中字段和存储增加的时间字段
    :param column_data: 表数据数组，二维。每一行数据即为一条记录，记录中字段值顺序和column_names里字段顺序相同。
                        由于python中int/float范围比java中类型大，这里需要用JLong/JDouble显式转换对象。
                        参考demo.py中的_generate_random_value()方法。
    :param commit_msg: 添加到快照summary里的commit信息
    """
    j_databuffer = JClass(JCLASS_DATABUFFER)
    j_genericrecord = JClass(JCLASS_GENERICRECORD)
    j_instant = JClass(JCLASS_INSTANT)
    j_zoneoffset = JClass(JCLASS_ZONEOFFSET)
    j_arraylist = JClass(JCLASS_ARRAYLIST)

    ts_idx = _find_column_index(column_names, DTEVENTTIMESTAMP)
    et_idx = _find_column_index(column_names, ET)
    buffer = j_databuffer(table, COMMIT_INTERVAL, COMMIT_LIMIT)
    records = j_arraylist(len(column_data))
    schema = table.schema()
    has_et_column = True if schema.findField(ET) else False

    for data in column_data:
        record = j_genericrecord.create(schema)
        for idx in range(0, len(column_names)):
            # 所有表字段均为小写
            column_name = column_names[idx].lower()
            if ts_idx == idx and has_et_column and et_idx == -1:
                # 对于分区表，如果未指定ET字段的值，则使用时间戳dtEventTimeStamp的值计算默认的分区字段的值
                record.setField(ET, j_instant.ofEpochMilli(data[ts_idx]).atOffset(j_zoneoffset.UTC))

            record.setField(column_name, data[idx])
        records.append(record)

    commit_msg_str = {}  # commit信息包含的key/value必须都为字符串
    for (k, v) in commit_msg.items():
        commit_msg_str[str(k)] = str(v)

    buffer.add(records, commit_msg_str)
    buffer.close()


def update_table(table, condition_expr, assign_dict):
    """
    指定条件更新数据湖表中的数据
    :param table: 表对象
    :param condition_expr: 条件表达式，用于匹配符合条件的数据
    :param assign_dict: 需要更新的字段名称和更新后的值构成的映射
    """
    j_hashmap = JClass(JCLASS_HASHMAP)
    j_assign = JClass(JCLASS_ASSIGN)
    transformers = j_hashmap()
    for (k, v) in assign_dict.items():
        transformers[str(k).lower()] = j_assign(v)

    result = table.updateData(condition_expr, transformers)
    logger.info(result)

    return result.summary()


def delete_table_partitions(table, field_name, partitions):
    """
    删除数据湖中表里指定的分区数据
    :param table: 表对象
    :param field_name: 删除分区对于的字段名称
    :param partitions: 待删除的分区的值的集合
    :return: 删除分区数据操作的打点数据
    """
    j_hashset = JClass(JCLASS_HASHSET)
    part_set = j_hashset()
    for p in partitions:
        part_set.add(str(p))

    result = table.deletePartitions(str(field_name).lower(), part_set)
    logger.info(result)

    return result.summary()


def delete_table_records(table, condition_expr):
    """
    指定条件删除数据湖表中的数据
    :param table: 表对象
    :param condition_expr: 条件表达式，用于匹配符合条件的数据
    :return: 此操作的打点数据，包含扫描数据量，扫描文件数，删除数据量，操作耗时等信息
    """
    result = table.deleteData(condition_expr)
    logger.info(result)

    return result.summary()


def truncate_table(table):
    """
    清空表数据
    :param table: 表对象
    """
    table.truncateTable()
    assert 0 == table.totalCount(), "should has no record after truncate"
    logger.info("table %s was truncated, now 0 records in it." % table.toString())


def total_record_count(table):
    """
    获取iceberg表当前的记录数。
    :param table: 表对象
    :return: 当前表中记录数
    """
    return int(table.totalCount())


def record_count_between(table, start_time, end_time, ts_field=None):
    """
    获取指定时间范围内iceberg表记录数，注意start_time/end_time时间格式必须为严格的ISO_OFFSET_DATE_TIME格式，
    例如'2011-12-03T10:15:30Z', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'
    :param table: 表对象
    :param start_time: 开始时间，包含。字符串。
    :param end_time: 结束时间，不包含。字符串。
    :param ts_field: 时间字段名，如果不指定，使用默认的分区时间字段
    :return: 指定时间范围内的记录数。如果开始时间不早于结束时间，返回0。如果读取表数据失败，返回-1。
    """
    j_offsetdatetime = JClass(JCLASS_OFFSETDATETIME)
    start = j_offsetdatetime.parse(start_time)
    end = j_offsetdatetime.parse(end_time)

    if not ts_field:
        ts_field = ET

    return int(table.getRecordCountBetween(ts_field, start, end))
