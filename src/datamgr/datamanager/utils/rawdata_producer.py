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
import io
import json
import time

import fastavro
import gevent
from pykafka import Cluster, Producer
from pykafka.handlers import GEventHandler

from api import access_api, databus_api
from common.exceptions import ApiResultError


class RawDataProducer(object):
    def __init__(self, bk_biz_id, raw_data_name):
        raw_data_info = self.get_raw_data_info(bk_biz_id, raw_data_name)
        channel_info = self.get_channel_info(raw_data_info.get("storage_channel_id"))
        producer_connection_info = [
            "{}:{}".format(
                channel_info.get("cluster_domain"), channel_info.get("cluster_port")
            )
        ]
        cluster = Cluster(
            hosts=",".join(producer_connection_info),
            handler=GEventHandler(),
            socket_timeout_ms=120000,
        )
        self.topic = cluster.topics[str(raw_data_info.get("topic"))]
        self.producer = Producer(
            cluster=cluster, topic=self.topic, sync=True, pending_timeout_ms=180000
        )

    @staticmethod
    def get_raw_data_info(bk_biz_id, raw_data_name):
        """
        通过数据源名称查询详情
        """
        res = access_api.raw_datas.list(
            {"bk_biz_id": bk_biz_id, "raw_data_name__icontains": raw_data_name},
            raise_exception=True,
        )
        for d in res.data:
            if d["raw_data_name"] == raw_data_name:
                return d

        raise ApiResultError(
            f"Can not get raw data info which raw_data_name is {bk_biz_id}:{raw_data_name}"
        )

    @staticmethod
    def get_channel_info(storage_channel_id):
        res = databus_api.channels.retrieve(
            {"cluster_name": storage_channel_id}, raise_exception=True
        )
        return res.data

    def produce_avro_message(
        self, message, data_set_id, partition, callback=None, timeout=10
    ):
        produce_count = 0
        avro_records = self.build_avro_message(message.value)
        now = int(time.time() * 1000)
        for records in avro_records:
            for record in records:
                produce_count += 1
                data_time = record.get("dtEventTimeStamp")
                self.produce_message(
                    value=json.dumps(
                        {
                            "data_set_id": str(data_set_id),
                            "data": json.dumps(record),
                            "data_time": data_time,
                            "data_local_time": now,
                        }
                    ),
                    timeout=timeout,
                )
        return produce_count

    def produce_message(self, value, key=None, partition=None, timeout=10):
        max_timeout = timeout
        while True:
            try:
                self.producer.produce(
                    message=bytes(value, encoding="utf-8"), partition_key=key
                )
                return True
            except Exception:
                max_timeout -= 0.1
                gevent.sleep(0.1)
            if max_timeout < 0:
                return False
        return True

    def flush(self):
        self.producer._wait_all()

    @staticmethod
    def build_avro_message(data):
        records = []
        decode_data = data.decode("utf8")
        encode_data = decode_data.encode("ISO-8859-1")
        row_file = io.BytesIO(encode_data)
        records.extend(map(lambda x: x["_value_"], fastavro.reader(row_file)))
        return records
