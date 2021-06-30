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
import collections
import io
import re
import time

import fastavro
from bkbase.dataflow.core.storage.kafka_driver import KafkaProducer
from bkbase.dataflow.core.util.avro_util import get_output_schema


class AvroWriter:
    def __init__(self, sink_node, topology):
        self.sink_node = sink_node
        self.topology = topology

    def for_each_batch_writer(self, batch_df, batch_id):
        # foreachPartition 在 driver 侧执行，即 PartitionWriter 在 driver 侧实例化，然后序列化到 executor 侧
        batch_df.foreachPartition(PartitionWriter(self.sink_node, self.topology, batch_id).foreach_partition_write)


class PartitionWriter:
    def __init__(self, sink_node, topology, batch_id):
        self.sink_node = sink_node
        self.topology = topology
        self.batch_id = batch_id
        self.kafka_producer = None
        self.batch_num = 100

    def foreach_partition_write(self, partition):
        # kafka 链接需要在 executor 侧初始化
        if self.kafka_producer is None:
            self.kafka_producer = KafkaProducer(self.sink_node.output["topic"], self.sink_node.output["info"])
        time_records = collections.OrderedDict()
        for data in partition:
            min_str, row = self.get_output_row(data, self.sink_node.get_field_names())
            if min_str in time_records:
                time_records[min_str].append(row)
            else:
                time_records[min_str] = [row]
            if len(time_records) >= self.batch_num:
                self.send_messages(time_records)
        if len(time_records) > 0:
            self.send_messages(time_records)
        self.kafka_producer.close()

    def send_messages(self, time_records):
        for min_key, row_value in time_records.items():
            self.kafka_producer.send_messages(
                (min_key + str(round(time.time() * 1000))).encode("utf-8"),
                self.avro_dump(get_output_schema(self.sink_node), row_value, self.batch_id).encode("utf-8"),
            )
        time_records.clear()

    def avro_dump(self, schema, data, batch_id):
        byte_buff = io.BytesIO()
        avro_row = [
            {
                "_tagTime_": int(time.time()),
                "_value_": data,
                "_metricTag_": "{job_id}|{node_id}|{batch_id}|{time}".format(
                    job_id=self.topology.job_id,
                    node_id=self.sink_node.node_id,
                    batch_id=str(batch_id),
                    time=str(int(time.time() / 60) * 60),
                ),
            }
        ]
        fastavro.writer(byte_buff, schema, avro_row, codec="snappy")
        return byte_buff.getvalue().decode("ISO-8859-1")

    def get_output_row(self, data, node_fields):
        row = {}
        for field in node_fields:
            if field in ["dtEventTimeStamp", "dtEventTime", "localTime"]:
                continue
            row[field] = data[field]
        # 对系统字段进行赋值
        row.update(self.generate_system_fields(data))
        min_str = re.sub(r"[- :]", "", row["dtEventTime"])[0:12] + "00"
        return min_str, row

    def generate_system_fields(self, data):
        """
        生成系统字段

        :param data: 数据
        :return:  根据数据生成系统字段
        """
        local_timestamp = time.time()
        local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(local_timestamp))
        if self.sink_node.event_time is None:
            event_timestamp = int(local_timestamp * 1000)
            event_time = local_time
        else:
            event_timestamp = data[self.sink_node.event_time]
            event_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(event_timestamp / 1000)))
        return {"dtEventTimeStamp": event_timestamp, "dtEventTime": event_time, "localTime": local_time}
