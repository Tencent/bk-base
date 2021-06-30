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

import gevent
from common.mixins import KafkaProducerMixin
from dmonitor.metrics.collectors import (
    CntCalculator,
    KafkaOffsetCollector,
    MetricOffsetCollector,
    PulsarOffsetCollector,
)
from gevent import Greenlet, monkey, queue

monkey.patch_all()


def rawdata_msg_cnt_metrics(params):
    logging.info("Start to execute generating rawdata msg cnt metrics task")

    task_configs = {
        "task_pool_size": 100,
        "offset_source": params.get("offset_source", "kafka"),
        "action_config": {
            "topic_hash_value": params.get("topic_hash_value", None),
            "topic_hash_divisor": params.get("topic_hash_divisor", 1),
        },
    }

    try:
        task = RawdataMsgCntTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init rawdata msg cnt metrics task".format(
                error=e
            ),
            exc_info=True,
        )


class RawdataMsgCntTaskGreenlet(KafkaProducerMixin, Greenlet):
    """
    计算Kafka集群中每个Topic按分钟/按天统计的记录条数
    """

    def __init__(self, *args, **kwargs):
        configs = kwargs.pop("configs", {})

        super(RawdataMsgCntTaskGreenlet, self).__init__(*args, **kwargs)

        self._kafka_fail_cnt = 0
        self._kafka_offsets = {}
        self._offset_source = configs.get("offset_source", "kafka")
        self._action_config = configs.get("action_config")
        self._offset_queue = queue.Queue()

        self._target_partition = 0
        self._last_clean_buffer = 0
        self.min_calculator = CntCalculator(self, 60, "kafka_topic_message_cnt")
        self.day_calculator = CntCalculator(
            self, 86400, "kafka_topic_message_day_cnt", "rp_month"
        )

    def _run(self):
        """从任务数据来源中消费待处理数据，并加入到协程池中进行指标处理和加工，或者生成告警"""
        gevent.joinall(
            [
                gevent.spawn(self.collector_offset_point),
                gevent.spawn(self.handle_offsets),
            ]
        )

    def collector_offset_point(self):
        while True:
            try:
                msg = self._offset_queue.get_nowait()
                self.min_calculator.add_point(msg)
                self.day_calculator.add_point(msg)
                continue
            except gevent.queue.Empty as e:
                # 防止空转
                logging.debug("empty %s" % e)
            except Exception as e:
                logging.error("get offset message exception %s" % e)

            gevent.sleep(0.05)

    def handle_offsets(self):
        if self._offset_source == "kafka":
            logging.info("Generate kafka collector")
            collector = KafkaOffsetCollector(self)
        elif self._offset_source == "influxdb":
            logging.info("Generate kafka offset collector")
            collector = MetricOffsetCollector(self)
        elif self._offset_source == "pulsar":
            logging.info("Generate pulsar collector")
            collector = PulsarOffsetCollector(self)
        else:
            return False

        KAFKA_COLLECT_INTERVAL = 60
        CLUSTERS_REFRESH_INTERVAL = 300

        while True:
            cur_time = time.time()
            all_waiting = True

            if (
                cur_time - collector._last_refresh_clusters_time
                > CLUSTERS_REFRESH_INTERVAL
            ):
                collector.init_clients(cur_time)

            for cluster_name in collector._clusters:
                last_collect_time = collector._last_collect_times.get(cluster_name, 0)
                if cur_time - last_collect_time <= KAFKA_COLLECT_INTERVAL:
                    continue
                # 开始采集
                collector._last_collect_times[cluster_name] = cur_time
                all_waiting = False
                gevent.spawn(collector.collect_cluster, cluster_name)

            # 定期清理一下window buffer
            nowtime = time.time()
            if nowtime - self._last_clean_buffer > 60:
                logging.info(
                    "==== current offset buffer len %s ===="
                    % self._offset_queue.qsize()
                )
                logging.info("=== clean buffer ===")
                self._last_clean_buffer = nowtime
                self.min_calculator.clean_buffer(nowtime)
                self.day_calculator.clean_buffer(nowtime)

                all_waiting = False

            if all_waiting:
                # 防止空转
                gevent.sleep(1)
