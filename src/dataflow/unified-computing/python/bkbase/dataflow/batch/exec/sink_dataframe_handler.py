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
from pyspark.sql.functions import lit


def sink_dataframe(df, sink_node, pipeline, mode="overwrite"):
    df_after_drop = __drop_old_reserved_column(df)
    final_df = __add_new_reserved_column(df_after_drop, sink_node.event_time)

    if isinstance(sink_node.output, str):
        batch_logger.info("Start to save {} to {}".format(sink_node.node_id, sink_node.output))
        final_df.write.mode(mode).parquet(sink_node.output)
        pipeline.check_and_add_monitor_output([sink_node.output], sink_node.node_id)
    else:
        raise Exception("Sink node output must be single path in string format")


def __drop_old_reserved_column(df):
    result_df = df
    for reserved_field in batch_conf.RESERVED_FIELDS:
        result_df = result_df.drop(reserved_field["field"])
    return result_df


def __add_new_reserved_column(df, dt_event_time):
    localtime = batch_utils.get_current_formatted_time()
    batch_logger.info("Will add dt_event_timestamp: {}, localtime: {}".format(dt_event_time, localtime))
    result_df = df
    result_df = result_df.withColumn("thedate", lit(int(batch_utils.timestamp_to_date(dt_event_time, "%Y%m%d"))))
    result_df = result_df.withColumn(
        "dtEventTime", lit(batch_utils.timestamp_to_date(dt_event_time, "%Y-%m-%d %H:%M:%S"))
    )
    result_df = result_df.withColumn("dtEventTimeStamp", lit(dt_event_time))
    result_df = result_df.withColumn("localTime", lit(localtime))
    return result_df
