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
from bkbase.dataflow.batch.exec import sink_dataframe_handler
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


def test_sink_drop_old_reserved_column():
    spark = SparkSession.builder.appName("example").master("local").getOrCreate()
    schema = StructType([StructField("thedate", IntegerType(), True), StructField("count", IntegerType())])
    data = [(1, 3), (2, 4)]
    df = spark.createDataFrame(data, schema)
    result_df = sink_dataframe_handler.__drop_old_reserved_column(df)
    assert len(result_df.schema) == 1
    left_field = result_df.schema[0]
    assert left_field.name == "count"


def test_add_new_reserved_column():
    spark = SparkSession.builder.appName("example").master("local").getOrCreate()
    schema = StructType([StructField("id", IntegerType(), True), StructField("count", IntegerType())])
    data = [(1, 3), (2, 4)]
    df = spark.createDataFrame(data, schema)
    # 202012161400
    dt_event_time = 1608098400000
    result_df = sink_dataframe_handler.__add_new_reserved_column(df, dt_event_time)
    assert len(result_df.schema) == 6
    data = result_df.take(1)[0]
    assert data.thedate == 20201216
    assert data.dtEventTime == "2020-12-16 14:00:00"
    assert data.dtEventTimeStamp == 1608098400000
