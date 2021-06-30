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
from datetime import datetime

import pytz
from bkbase.dataflow.batch.conf import batch_conf
from bkbase.dataflow.batch.gateway import py_gateway
from bkbase.dataflow.batch.utils import batch_logger
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    TimestampType,
)

tz = pytz.timezone(batch_conf.TIMEZONE)

BatchUtils = py_gateway.gateway.jvm.org.apache.spark.util.BatchUtils
SparkParquetNumRow = py_gateway.gateway.jvm.com.tencent.bk.base.dataflow.spark.sql.parquet.SparkParquetNumRow


def count_parquet_row_num(spark_session, path_list):
    return SparkParquetNumRow.countParquetRowNum(path_list, spark_session._jsparkSession)


def create_java_tmp_dir():
    return BatchUtils.createTempDir()


def download_file(source, target_dir, target_name, spark_conf, hadoop_conf=py_gateway.Configuration()):
    BatchUtils.downloadFiles(source, target_dir, target_name, spark_conf._jconf, hadoop_conf)


def is_hdfs_path_exist(path):
    p = py_gateway.Path(path)
    fs = p.getFileSystem(py_gateway.Configuration())
    return fs.exists(p)


def get_days_diff(start, end):
    day_start_format = timestamp_to_date(start, "/%Y/%m/%d")
    day_start_millisecond = date_to_timestamp(day_start_format, "/%Y/%m/%d")
    day_end_format = timestamp_to_date(end, "/%Y/%m/%d")
    day_end_millisecond = date_to_timestamp(day_end_format, "/%Y/%m/%d")
    day_diff_millisecond = day_end_millisecond - day_start_millisecond
    return int(day_diff_millisecond / (24 * 3600 * 1000))


def get_current_timestamp():
    return date_to_timestamp(get_current_formatted_time(), "%Y-%m-%d %H:%M:%S")


def get_current_formatted_time():
    return datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")


def generate_hdfs_path(root, start_millisecond, end_millisecond, step=1):
    hour_millisecond = 3600 * 1000
    time_list = list(
        range(time_round_to_hour(start_millisecond), time_round_to_hour(end_millisecond), step * hour_millisecond)
    )
    batch_logger.info("Time list to generate path: %s" % time_list)
    paths = []
    for item in time_list:
        paths.append("{}{}".format(root, timestamp_to_path_format(item)))
    batch_logger.info("Generated path %s" % paths)
    return paths


def timestamp_to_date(millisecond_timestamp, time_format):
    """
    timestamp_to_date(millisecond_timestamp, format) -> String

    Commonly used format codes:
    %Y  Year with century as a decimal number.
    %m  Month as a decimal number [01,12].
    %d  Day of the month as a decimal number [01,31].
    %H  Hour (24-hour clock) as a decimal number [00,23].
    %M  Minute as a decimal number [00,59].
    %S  Second as a decimal number [00,61].
    %z  Time zone offset from UTC.
    %a  Locale's abbreviated weekday name.
    %A  Locale's full weekday name.
    %b  Locale's abbreviated month name.
    %B  Locale's full month name.
    %c  Locale's appropriate date and time representation.
    %I  Hour (12-hour clock) as a decimal number [01,12].
    %p  Locale's equivalent of either AM or PM.
    """
    timestamp = float(millisecond_timestamp / 1000)
    time_from_timestamp = datetime.fromtimestamp(timestamp, tz=tz)
    format_time = time_from_timestamp.strftime(time_format)
    return format_time


def date_to_timestamp(custom_time, time_format):
    """
    date_to_timestamp(custom_time, format) -> int

    Commonly used format codes:
    %Y  Year with century as a decimal number.
    %m  Month as a decimal number [01,12].
    %d  Day of the month as a decimal number [01,31].
    %H  Hour (24-hour clock) as a decimal number [00,23].
    %M  Minute as a decimal number [00,59].
    %S  Second as a decimal number [00,61].
    %z  Time zone offset from UTC.
    %a  Locale's abbreviated weekday name.
    %A  Locale's full weekday name.
    %b  Locale's abbreviated month name.
    %B  Locale's full month name.
    %c  Locale's appropriate date and time representation.
    %I  Hour (12-hour clock) as a decimal number [01,12].
    %p  Locale's equivalent of either AM or PM.
    """
    datetime_obj = datetime.strptime(custom_time, time_format)
    milliceconds_timestamp = int(tz.localize(datetime_obj).timestamp() * 1000.0 + datetime_obj.microsecond / 1000.0)
    return milliceconds_timestamp


def timestamp_to_path_format(milliseconds_timestamp):
    return timestamp_to_date(milliseconds_timestamp, "/%Y/%m/%d/%H")


def path_format_to_timestamp(path):
    return date_to_timestamp(path, "/%Y/%m/%d/%H")


def time_round_to_hour(milliseconds_timestamp):
    return path_format_to_timestamp(timestamp_to_path_format(milliseconds_timestamp))


def map_struct_type(data_type):
    data_type = data_type.lower()
    type_dict = {
        "string": StringType(),
        "int": IntegerType(),
        "float": FloatType(),
        "long": LongType(),
        "double": DoubleType(),
        "byte": ByteType(),
        "varchar": StringType(),
        "short": ShortType(),
        "integer": IntegerType,
        "bigint": LongType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType(),
        "binary": BinaryType(),
        "null": NullType,
    }
    if data_type not in type_dict:
        raise Exception("Not support type %s" % data_type)
    return type_dict[data_type]


def get_rt_number(rt_id):
    if rt_id is not None and rt_id != "":
        return rt_id.split("_", 2)[0]
    else:
        batch_logger.warn("Failed to get Rt business number because of wrong Rt name format: {}".format(rt_id))
        return None


def main():
    py_gateway.set_log_level("INFO")
    time_format = timestamp_to_date(1584615926927, "/%Y/%m/%d/%H")
    assert time_format == "/2020/03/19/19"
    timestamp = date_to_timestamp(time_format, "/%Y/%m/%d/%H")
    assert timestamp == 1584615600000

    paths = generate_hdfs_path("/test", 1585253710310, 1585279710310)
    assert paths[0] == "/test/2020/03/27/04"
    assert paths[1] == "/test/2020/03/27/05"
    assert paths[2] == "/test/2020/03/27/06"
    assert paths[3] == "/test/2020/03/27/07"
    assert paths[4] == "/test/2020/03/27/08"
    assert paths[5] == "/test/2020/03/27/09"
    assert paths[6] == "/test/2020/03/27/10"

    hour_time = time_round_to_hour(1586262326907)
    assert hour_time == 1586260800000
    assert int(timestamp_to_date(hour_time, "%H")) == 20


if __name__ == "__main__":
    main()
