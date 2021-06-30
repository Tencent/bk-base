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

from bkbase.dataflow.batch.conf import batch_conf
from bkbase.dataflow.batch.utils import batch_logger, batch_utils
from pyspark.sql.types import StructField, StructType


def create_hdfs_src_df(source_node, pipeline):
    schema = __get_input_schema(source_node)
    batch_logger.info("Get schema for {}: {}".format(source_node.node_id, schema))

    if isinstance(source_node.input, list):
        input_list = __check_input_path(source_node.input)
    else:
        raise Exception("Normal source node input only support list")

    batch_logger.info("start to generate Dataframe from path: {}".format(input_list))
    df = pipeline.spark.read.schema(schema).parquet(*input_list)
    batch_logger.info("Gernerated dataframe from path %s" % input_list)

    pipeline.check_and_add_monitor_input(input_list, source_node.node_id)
    return df


def create_hdfs_self_dependency_src_df(source_node, pipeline):
    schema = __get_input_schema(source_node)
    batch_logger.info("Get schema for {}: {}".format(source_node.node_id, schema))

    if isinstance(source_node.input, dict):
        input_list = source_node.input
    else:
        raise Exception("Self Dependency Source node input only support dict")

    origin_input_list = __check_input_path(input_list[batch_conf.SELF_DEPENDENCY_ORIGIN_KEY])
    df = pipeline.spark.read.schema(schema).parquet(*origin_input_list)
    if len(df.take(1)) != 0:
        pipeline.check_and_add_monitor_input(origin_input_list, source_node.node_id)
        return df
    batch_logger.info("Self dependency origin path is empty try to load from copy path %s" % input_list)

    copy_input_list = __check_input_path(input_list[batch_conf.SELF_DEPENDENCY_COPY_KEY])
    df = pipeline.spark.read.schema(schema).parquet(*copy_input_list)
    pipeline.check_and_add_monitor_input(copy_input_list, source_node.node_id)
    return df


def __get_input_schema(source_node):
    schema = []
    for field in source_node.fields:
        schema.append(StructField(field["field"], batch_utils.map_struct_type(field["type"])))

    for reserved_field in batch_conf.RESERVED_FIELDS:
        schema.append(StructField(reserved_field["field"], batch_utils.map_struct_type(reserved_field["type"])))

    return StructType(schema)


def __check_input_path(input_list):
    result_list = []
    for path in input_list:
        if batch_utils.is_hdfs_path_exist(path):
            result_list.append(path)
        else:
            batch_logger.info("Remove non-exist path from input list: {}".format(path))
    return result_list
