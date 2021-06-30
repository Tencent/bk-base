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

from dmonitor.metrics.collectors.base import BaseOffsetCollector
from utils.influx_util import influx_query


class PulsarOffsetCollector(BaseOffsetCollector):
    """模拟kafka的offset对pulsar的数据量进行统计
    统计来源来自于与总线上报的pulsar数据总量的指标
    """

    def __init__(self, task):
        super(PulsarOffsetCollector, self).__init__(task)
        self._metric_buffer = {}

    def init_config(self):
        super(PulsarOffsetCollector, self).init_config()
        action_config = self._task._action_config
        self._metric_time_range = action_config.get("metric_time_range", "5m")
        return True

    def init_clients(self, cur_time):
        self._clusters = ["default"]
        return True

    def init_cluster_client(self, cluster_name, kafka_config=False):
        return True

    def clear_cluster(self, cluster_name):
        return True

    def collect_cluster(self, cluster_name):
        start_time = time.time()
        sql = """
            SELECT max("total_count") as Value FROM "pulsar_message_total_count"
            WHERE time >= now() - %s
            GROUP BY "partition", "cluster_name", "topic", time(1m)
        """ % (
            self.metric_time_range
        )
        metrics = influx_query(sql, db="influx_monitor_custom_metrics", is_dict=True)
        logging.info("Start to collect offset info by sql({})".format(sql))
        if not metrics:
            logging.error("【%s】 influxdb failed" % cluster_name)
            return False

        last_buffer = self._metric_buffer.get(cluster_name, {})
        self._metric_buffer[cluster_name] = {}
        new_rec = 0
        old_rec = 0
        empty_rec = 0
        pass_rec = 0
        logging.info("Fetch %s metrics from influxdb" % len(metrics))
        for record in metrics:
            try:
                topic = record.get("topic", "unknown")
                partition = record.get("partition", "unknown")
                setid = record.get("cluster_name", "unknown")
                offset = record.get("Value", -1)
                if (offset is None) or (offset < 0):
                    empty_rec += 1
                    continue
                if self._task_topic_hash_value is not None:
                    hash_value = self.get_topic_hash(topic)
                    if hash_value != self._task_topic_hash_value:
                        pass_rec += 1
                        continue

                metric_info = {
                    "time": int(record.get("time") or 0),
                    "offset": int(offset or 0),
                    "partition": int(partition or 0),
                    "topic": topic,
                    "setid": setid,
                    "is_new": False,
                }
                metric_key = "|".join("%s" % value for value in metric_info.values())
                # 加入buffer， 为下次拉取时去重
                self._metric_buffer[cluster_name][metric_key] = True
                # 如果上次已经处理这次offset上报了， 则不需要再触发一次
                if last_buffer.get(metric_key, False):
                    old_rec += 1
                    continue

                # 同步给计算增量的协程
                self._task._offset_queue.put(metric_info)
                new_rec += 1
            except Exception as e:
                empty_rec += 1
                logging.error(
                    "【{}】handle offset report {} exception {}".format(
                        cluster_name, record, e
                    )
                )

        end_time = time.time()
        logging.info(
            "New: {}, empty: {}, old: {}, pass: {}, hash: {}".format(
                new_rec, empty_rec, old_rec, pass_rec, self._task_topic_hash_value
            )
        )
        logging.info(
            "Success to collect {} offsets from influxdb about pulsar({}) cost time {}s".format(
                new_rec, cluster_name, end_time - start_time
            )
        )
        return True
