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
import time

from jpype.types import JClass, JInt, JLong
from tencent_bkbase_datalake import tables
from tencent_bkbase_datalake.tables import DTEVENTTIMESTAMP, ET
from tests import utils

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


TEST_DB_NAME = "iceberg_591"
TEST_TABLE_NAME = "t1_591"
PROPS = {
    "fs.defaultFS": "file:///",
    "hadoop.warehouse.dir": "/tmp",
    "interval": "300000",
    "flush.size": "1000000",
    "table.preserve.snapshot.days": "1",
    "table.max.rewrite.files": "1000",
    "table.max.compact.file.size": "16777216",
    "table.commit.retry.total-timeout-ms": "60000",
    "table.commit.retry.num-retries": "100",
    "table.commit.retry.min-wait-ms": "200",
    "table.commit.retry.max-wait-ms": "2000",
}


def _generate_fields():
    return {
        ET: tables.TIMESTAMP,
        DTEVENTTIMESTAMP: tables.LONG,
        "dtEventTime": tables.STRING,
        "localTime": tables.STRING,
        "f_int": tables.INT,
        "f_long": tables.LONG,
        "f_float": tables.FLOAT,
        "f_double": tables.DOUBLE,
        "f_text": tables.STRING,
    }


def _create_table(tb, fields):
    j_partitionmethod = JClass("com.tencent.bk.base.datahub.iceberg.PartitionMethod")
    j_tablefield = JClass("com.tencent.bk.base.datahub.iceberg.TableField")
    j_arraylist = JClass("java.util.ArrayList")

    fields_arr = j_arraylist()
    for (key, val) in fields.items():
        fields_arr.add(j_tablefield(key, val, True))  # True代表允许null值

    parts_arr = j_arraylist()
    parts_arr.add(j_partitionmethod(ET, "DAY"))

    tb.createTable(fields_arr, parts_arr)
    logger.info(tb.info())


def _modify_partition(tb):
    j_partitionmethod = JClass("com.tencent.bk.base.datahub.iceberg.PartitionMethod")
    j_arraylist = JClass("java.util.ArrayList")

    parts_arr = j_arraylist()
    parts_arr.add(j_partitionmethod(ET, "MONTH"))
    tb.changePartitionSpec(parts_arr)


def _write_table(tb, fields, loop, batch):
    for i in range(0, loop):
        # 这里python里的数据类型和java里不一致，python里范围更广，注意要将column_data中int/float转换为java里的类型
        column_names, column_data = utils.generate_data(fields, batch)
        s1 = time.time()
        tables.append_table(tb, column_names, column_data, {"committer": "van", "loop": i})
        logger.info(">>> append 10w takes %s" % (time.time() - s1))

    logger.info(tb.info())
    logger.info(">>> partitions: %s" % tb.getPartitionPaths())


def _get_scan_records(tb, expr):
    # 遍历表里符合查询条件的数据
    cnt = 0
    for _ in tables.scan_table(tb, condition_expr=expr):
        cnt += 1

    logger.info("matching {} records are {}".format(expr, cnt))
    return cnt


def _get_scan_table_by_columns(tb, column_names):
    # 遍历表里所有的数据，返回指定的字段
    cnt = 0
    for rd in tables.scan_table(tb, column_names=column_names):
        assert len(rd) == len(column_names)
        cnt += 1

    return cnt


def _drop_table_if_exist(tb):
    success = tb.dropTable()
    logger.error("drop iceberg table {} is {}".format(tb, success))


def demo():
    # 测试并行调用start_jvm_on_need，验证执行没问题，jvm可以正常启动
    for i in range(0, 9):
        tables.start_jvm_on_need(["-Xms256M", "-Xmx2G"])

    j_bktable = JClass("com.tencent.bk.base.datahub.iceberg.BkTable")

    table = j_bktable(TEST_DB_NAME, TEST_TABLE_NAME, PROPS)
    _drop_table_if_exist(table)
    # 测试过程中需要创建表，现网中表是通过storekit的接口创建和维护的，无需此步骤，只需要调用tables.load_table即可
    table_fields = _generate_fields()
    _create_table(table, table_fields)
    _modify_partition(table)
    assert 0 == tables.total_record_count(table)

    _write_table(table, table_fields, 3, 100000)
    total_cnt = tables.total_record_count(table)
    assert 3 * 100000 == total_cnt

    # 测试查询功能
    expr1 = tables.build_field_condition_expr("f_InT", tables.INT, ">=", 10000)
    expr2 = tables.build_field_condition_expr("f_int", tables.INT, "<=", 19999)
    expr3 = tables.build_and_condition(expr1, expr2)

    result = tables.read_table_into_list(table, limit=10)
    assert len(result) == 10

    scan_count = _get_scan_table_by_columns(table, ["dtEventTime", "localTime", "f_int", "f_not_exist"])
    assert scan_count == total_cnt
    scan_count = _get_scan_records(table, expr3)

    logger.info(">>> going to delete data %s" % expr3)
    metric = tables.delete_table_records(table, expr3)
    logger.info(">>> deleted records: {} == {}, summary: {}".format(scan_count, metric["affected.records"], metric))
    assert str(scan_count) == metric["affected.records"]

    res = tables.delete_table_partitions(table, ET, ["2020-08-01", "2020-08-02"])
    logger.info(res)
    logger.info(">>> partitions: %s" % table.getPartitionPaths())

    # 直接通过java代码更新数据
    j_assign = JClass("com.tencent.bk.base.datahub.iceberg.functions.Assign")
    j_expressions = JClass("org.apache.iceberg.expressions.Expressions")
    j_hashmap = JClass("java.util.HashMap")

    transformers = j_hashmap()
    transformers["f_int"] = j_assign(JInt(10000))
    transformers["f_long"] = j_assign(JLong(99999999))
    table.updateData(j_expressions.alwaysTrue(), transformers)
    for record in table.sampleRecords(10):
        logger.info(">>> %s" % record)

    tables.update_table(table, j_expressions.alwaysTrue(), {"f_int": JInt(20000), "f_Long": JLong(88888888)})
    for r in table.sampleRecords(10):
        logger.info(">>> %s" % r)

    logger.info("==== drop table t1_591 ====")
    table.dropTable()


if __name__ == "__main__":
    logger.info("==== create table t1_591 with local file system ====")
    demo()
