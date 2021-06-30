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

from pyspark.sql import DataFrame, SparkSession

ICEBERG = "iceberg"
ET = "____et"  # 默认时间分区字段
HIVE_METASTORE_URIS = "hive.metastore.uris"
LOCALITY = "locality"
SPARK_SQL_TIMEZONE = "spark.sql.session.timeZone"
CHECK_ORDERING = "check-ordering"
DISABLE_NULLABILITY_CHECK = "disable-nullability-check"
APPEND = "append"
OVERWRITE = "overwrite"
FIELD = "field"


def read_iceberg_table(full_table_name, spark, col_names=None, read_options=None):
    """
    读取iceberg表的数据，转换为DataFrame对象返回
    :param full_table_name: 完整的表名称，即rt的HDFS存储对应的physical_table_name
    :param spark: spark session
    :param col_names: 需要将小写的字段名称转换为包含大写字母的字段名称列表
    :param read_options: 需要传递的一些参数
    :return: DataFrame对象。默认字段名称为小写字母，可通过col_names传参映射为包含大写字母的字段名称。
    """
    assert isinstance(spark, SparkSession), "spark session is invalid"
    assert spark.conf.get(HIVE_METASTORE_URIS), "hive.metastore.uris config is missing"

    if read_options is None:
        read_options = {}

    assert isinstance(read_options, dict), "read_options should be dict"
    read_options[LOCALITY] = False
    df = spark.read.options(**read_options).format(ICEBERG).load(full_table_name)
    if col_names is None:
        return df
    else:
        assert isinstance(col_names, list), "column names should be list of strings"
        mapping = {}
        for name in col_names:
            mapping[str(name).lower()] = str(name)

        return df.toDF(*[mapping.get(c, c) for c in df.columns])


def write_iceberg_table(full_table_name, df, mode=None, partition_spec=None):
    """
    将DataFrame中数据写入iceberg表
    :param full_table_name: 完整的表名称，即rt的HDFS存储对应的physical_table_name
    :param df: DataFrame对象，包含要写入的数据集
    :param mode: 写入模式，支持append和overwrite
    :param partition_spec iceberg表分区定义，从存储的storage_config中获取。对非分区表不需要此参数。
    """
    if partition_spec is None:
        partition_spec = []

    assert isinstance(df, DataFrame), "df should be DataFrame object"
    mode = APPEND if mode is None else mode.lower()
    assert mode in [APPEND, OVERWRITE], "mode %s not allowed, should be append or overwrite" % mode
    assert isinstance(partition_spec, list), "partition spec should be list"

    # 所有字段改为小写字母
    df = df.toDF(*[c.lower() for c in df.columns])
    order_by = []
    for f in partition_spec:
        assert isinstance(f, dict) and f.get(FIELD), "partition spec item should be dict and contains key field"
        # 校验排序字段存在
        field = f.get(FIELD).lower()
        assert field in df.columns, "partition field column %s should exists in DataFrame" % field
        order_by.append(field)

    if order_by:
        # 分区表，需要按照分区的定义字段排序数据，然后写入文件
        df = df.sort(", ".join(order_by))

    df.write.format(ICEBERG).option(CHECK_ORDERING, False).option(DISABLE_NULLABILITY_CHECK, True).mode(mode).save(
        full_table_name
    )
