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

import jpype
import pytest
from jpype.types import JClass, JDouble, JFloat, JInt, JLong
from tencent_bkbase_datalake import tables
from tencent_bkbase_datalake.tables import (
    DOUBLE,
    DTEVENTTIMESTAMP,
    ET,
    FLOAT,
    FS_DEFAULTFS,
    INT,
    LOCAL_FILE_SYSTEM,
    LONG,
    STRING,
)
from tests import utils

SCHEMA = {
    ET: tables.TIMESTAMP,
    DTEVENTTIMESTAMP: LONG,
    "dtEventTime": STRING,
    "localTime": STRING,
    "f_int": INT,
    "f_long": LONG,
    "f_float": FLOAT,
    "f_double": DOUBLE,
    "f_text": STRING,
}
SCHEMA_2 = {DTEVENTTIMESTAMP: LONG, "dtEventTime": STRING, "localTime": STRING, "f_int": INT}

TEST_DB_NAME = "iceberg_591"
TEST_TABLE_NAME = "v_111_591"
TEST_TABLE_NAME_2 = "v_222_591"
PROPS = {
    FS_DEFAULTFS: LOCAL_FILE_SYSTEM,
    "hadoop.warehouse.dir": "/tmp",
    "interval": 300000,
    "flush.size": 1000000,
    "table.preserve.snapshot.days": 1,
    "table.max.rewrite.files": 1000,
    "table.max.compact.file.size": 16777216,
    "table.commit.retry.total-timeout-ms": 60000,
    "table.commit.retry.num-retries": 100,
    "table.commit.retry.min-wait-ms": 200,
    "table.commit.retry.max-wait-ms": 2000,
}


@pytest.fixture(scope="module", autouse=True)
def setup_hdfs_table():
    for i in range(0, 9):
        tables.start_jvm_on_need(["-Xms256M", "-Xmx2G"])

    tables.start_jvm_on_need()  # 验证多次调用启动jvm
    j_bktable = JClass("com.tencent.bk.base.datahub.iceberg.BkTable")
    j_partitionmethod = JClass("com.tencent.bk.base.datahub.iceberg.PartitionMethod")
    j_tablefield = JClass("com.tencent.bk.base.datahub.iceberg.TableField")
    j_arraylist = JClass("java.util.ArrayList")

    props_str = {str(k): str(v) for (k, v) in PROPS.items()}
    table = j_bktable(TEST_DB_NAME, TEST_TABLE_NAME, props_str)
    # 测试过程中需要创建表，现网中表是通过storekit的接口创建和维护的，无需此步骤，只需要调用tables.load_table()即可
    table_fields = copy.deepcopy(SCHEMA)
    fields_arr = j_arraylist()
    for (key, val) in table_fields.items():
        fields_arr.add(j_tablefield(key, val, True))  # True代表允许null值

    parts_arr = j_arraylist()
    parts_arr.add(j_partitionmethod(tables.ET, "DAY"))
    table.createTable(fields_arr, parts_arr)

    # 创建非分区表
    table_2 = j_bktable(TEST_DB_NAME, TEST_TABLE_NAME_2, props_str)
    fields_arr_2 = j_arraylist()
    for (key, val) in SCHEMA_2.items():
        fields_arr_2.add(j_tablefield(key, val, True))  # True代表允许null值

    table_2.createTable(fields_arr_2)

    yield

    table.dropTable()
    table_2.dropTable()


def test_find_column_index():
    columns = ["AAA", "Abc", "bbb", "cCc"]
    assert 1 == tables._find_column_index(columns, "aBc")
    assert 2 == tables._find_column_index(columns, "bBb")
    assert -1 == tables._find_column_index(columns, "abcd")


def test_cast_value():
    values = ["123", "100000000", 9999999999999999, 3.222]
    res = tables._cast_list_by_data_type("int", values)
    for i in range(0, len(values)):
        assert res[i] == JInt(int(values[i]))

    res = tables._cast_list_by_data_type("long", values)
    for i in range(0, len(values)):
        assert res[i] == JLong(int(values[i]))

    res = tables._cast_list_by_data_type("float", values)
    for i in range(0, len(values)):
        assert res[i] == JFloat(float(values[i]))

    res = tables._cast_list_by_data_type("double", values)
    for i in range(0, len(values)):
        assert res[i] == JDouble(float(values[i]))

    res = tables._cast_list_by_data_type("string", values)
    for i in range(0, len(values)):
        assert res[i] == str(values[i])

    res = tables._cast_list_by_data_type("text", values)
    for i in range(0, len(values)):
        assert res[i] == str(values[i])

    with pytest.raises(TypeError):
        tables._cast_list_by_data_type("timestamp", values)

    with pytest.raises(TypeError):
        tables._cast_list_by_data_type("int", 111111)


def test_table_append_and_delete():
    assert jpype.isJVMStarted() is True
    table = tables.load_table(TEST_DB_NAME, TEST_TABLE_NAME, PROPS)
    table_info = json.loads(str(table.info()))
    assert table_info["table"] == "{}.{}".format(TEST_DB_NAME, TEST_TABLE_NAME)
    assert len(table_info["partition"]) == 1
    assert len(table_info["schema"]) == 9
    assert len(table_info["snapshots"]) == 0
    assert len(table_info["partition_paths"]) == 0
    assert len(table_info["sample"]) == 0
    assert tables.total_record_count(table) == 0

    # 数据写入测试
    plus_days = 3
    loops = 5
    batch = 20000
    schema = copy.deepcopy(SCHEMA)
    cols = [k for k in schema.keys()]
    for loop in range(0, loops):
        # 这里python里的数据类型和java里不一致，python里范围更广，注意要将column_data中int/float转换为java里的类型
        column_names, column_data = utils.generate_data(schema, batch, plus_days)
        tables.append_table(table, column_names, column_data, {"committer": "van", "loop": loop})

    # 使用scan_table方法遍历表里的数据
    total = 0
    for _ in tables.scan_table(table):
        total += 1

    assert total == batch * loops
    assert tables.total_record_count(table) == total

    with pytest.raises(AssertionError):
        for _ in tables.scan_table(table, column_names={"col1": "____et", "col2": "f_int"}):
            total += 1

    result = tables.read_table_into_list(table, column_names=["____et", "col2"], limit=10)
    assert len(result) == 10
    for item in result:
        assert len(item) == 2

    result = tables.read_table_into_list(table)
    assert len(result) == total
    for item in result:
        assert len(item) == len(schema)

    snapshots = table.getLatestSnapshots(10)
    assert len(snapshots) == 5  # 调用append 5次，生成5个snapshot
    summary = snapshots[0].summary()
    assert summary["added-data-files"] == str(plus_days)
    assert summary["added-records"] == str(batch)
    assert summary["changed-partition-count"] == str(plus_days)
    assert summary["total-data-files"] == str(plus_days * loops)
    assert summary["dt-sdk-msg"] == '{"operation": "dt-add", "data": {"committer": "van", "loop": "4"}}'
    assert summary["total-records"] == "100000"

    partition_paths = table.getPartitionPaths()
    assert len(partition_paths) == plus_days

    # 按照分区进行数据删除测试
    part_str = utils.TEST_START_DATE.strftime("%Y-%m-%d")
    metric = tables.delete_table_partitions(table, tables.ET, [part_str])
    assert metric["scan.files"] == str(loops)
    assert metric["scan.records"] == metric["affected.records"]
    del_record_num_1 = int(str(metric["affected.records"]))
    summary = table.getLatestSnapshots(10)[0].summary()
    assert summary["deleted-data-files"] == str(loops)
    assert summary["deleted-records"] == metric["affected.records"]
    assert summary["changed-partition-count"] == str(1)
    assert summary["total-data-files"] == str((plus_days - 1) * loops)
    assert summary["dt-sdk-msg"] == '{{"operation": "dt-delete", "data": {{"{}_day": "{}"}}}}'.format(ET, part_str)

    # 按照条件进行数据删除测试
    expr1 = tables.build_field_condition_expr("f_InT", tables.INT, ">=", 10000)
    expr2 = tables.build_field_condition_expr("f_long", tables.LONG, "<=", 1000000)
    expr3 = tables.build_or_condition(expr1, expr2)
    match_cnt = 0
    with table.readRecords(expr3, cols) as records:
        for _ in records:
            match_cnt += 1

    metric = tables.delete_table_records(table, expr3)
    assert metric["scan.files"] == str((plus_days - 1) * loops)
    assert metric["scan.records"] == str(loops * batch - del_record_num_1)
    metric["affected.records"] = str(match_cnt)
    del_record_num_2 = int(str(metric["affected.records"]))
    summary = table.getLatestSnapshots(10)[0].summary()
    assert summary["deleted-data-files"] == metric["scan.files"]
    assert summary["deleted-records"] == metric["scan.records"]
    assert summary["changed-partition-count"] == str(plus_days - 1)
    assert summary["total-data-files"] == str(plus_days - 1)
    assert summary["total-records"] == str(loops * batch - del_record_num_1 - del_record_num_2)
    assert summary["added-data-files"] == summary["total-data-files"]
    assert summary["added-records"] == summary["total-records"]

    # 更新数据
    f_int = JInt(888)
    f_long = JLong(99999999)
    expr_less_than = tables.build_field_condition_expr("f_int", tables.INT, "<", 1000)
    metric = tables.update_table(table, expr_less_than, {"f_int": f_int, "f_long": f_long})
    assert metric["scan.files"] == str(plus_days - 1)
    assert int(str(metric["scan.records"])) > int(str(metric["affected.records"]))
    summary = table.getLatestSnapshots(10)[0].summary()
    assert summary["deleted-data-files"] == metric["scan.files"]
    assert summary["deleted-records"] == metric["scan.records"]
    assert summary["added-records"] == metric["scan.records"]
    assert summary["changed-partition-count"] == summary["total-data-files"]
    assert summary["added-data-files"] == summary["total-data-files"]
    assert summary["added-records"] == summary["total-records"]
    with table.readRecords(expr_less_than, cols) as records:
        for record in records:
            assert record.getField("f_int") == f_int
            assert record.getField("f_long") == f_long

    # 按照时间条件检索数据并删除，注意时间格式必须为严格的ISO_OFFSET_DATE_TIME格式
    # 例如：'2011-12-03T10:15:30Z', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'
    start_str = "2020-08-03T10:00:00+00:00"
    end_str = "2020-08-04T00:00:00Z"
    assert 0 == tables.record_count_between(table, end_str, start_str)
    to_delete_cnt = tables.record_count_between(table, start_str, end_str)

    expr4 = tables.build_field_condition_expr(ET, tables.STRING, ">=", start_str)
    expr5 = tables.build_field_condition_expr(ET, tables.STRING, "<", end_str)
    expr6 = tables.build_and_condition(expr4, expr5)

    metric = tables.delete_table_records(table, expr6)
    assert metric["scan.files"] == "1"
    assert int(str(metric["scan.records"])) > int(str(metric["affected.records"]))
    del_record_num_3 = int(str(metric["affected.records"]))
    assert del_record_num_3 == to_delete_cnt

    summary = table.getLatestSnapshots(10)[0].summary()
    assert summary["added-data-files"] == "1"
    assert summary["deleted-data-files"] == "1"
    assert summary["changed-partition-count"] == "1"
    assert summary["total-data-files"] == str(plus_days - 1)
    assert summary["total-records"] == str(loops * batch - del_record_num_1 - del_record_num_2 - del_record_num_3)

    # 写入不包含ET字段的数据，使用dtEventTimeStamp计算ET字段的值
    schema.pop(ET, None)
    column_names, column_data = utils.generate_data(schema, batch, plus_days)
    tables.append_table(table, column_names, column_data, {})
    summary = table.getLatestSnapshots(10)[0].summary()
    assert summary["added-data-files"] == str(plus_days)
    assert summary["changed-partition-count"] == str(plus_days)
    assert summary["added-records"] == str(batch)
    assert summary["dt-sdk-msg"] == '{"operation": "dt-add", "data": {}}'

    # 测试truncate表
    tables.truncate_table(table)

    # 非分区表测试写入
    table_2 = tables.load_table(TEST_DB_NAME, TEST_TABLE_NAME_2, PROPS)
    # 这里python里的数据类型和java里不一致，python里范围更广，注意要将column_data中int/float转换为java里的类型
    column_names, column_data = utils.generate_data(copy.deepcopy(SCHEMA_2), batch, plus_days)
    tables.append_table(table_2, column_names, column_data, {})

    snapshots = table_2.getLatestSnapshots(10)
    assert len(snapshots) == 1
    summary = snapshots[0].summary()
    assert summary["added-data-files"] == "1"
    assert summary["changed-partition-count"] == "1"
    assert summary["added-records"] == str(batch)
    assert summary["total-records"] == str(batch)
    assert summary["total-data-files"] == "1"

    partition_paths = table_2.getPartitionPaths()
    assert len(partition_paths) == 0
    tables.truncate_table(table_2)
