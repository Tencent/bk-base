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
import json
import logging
import time

import gevent
from gevent import monkey

from dmonitor.metrics.base import DataDelayMax
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS
from utils.time import floor_minute

monkey.patch_all()


def data_delay_metrics():
    logging.info("Start to execute generating data delay metrics task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_delay_audit",
            "partition": False,
            "group_id": "dmonitor",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = DataDelayMetricsTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init delay metrics task".format(error=e),
            exc_info=True,
        )


class DataDelayMetricsTaskGreenlet(BaseDmonitorTaskGreenlet):
    GENERATE_INTERVAL = 60
    CACHE_REFRESH_INTERVAL = 180
    PENDING_TIME = 180
    METRIC_TIMEOUT = 180

    def __init__(self, *args, **kwargs):
        """初始化生成延迟指标的任务

        :param task_configs: 缓存同步任务配置
            {
                'consumer_configs': {
                    'type': 'kafka',
                    'alias': 'op',
                    'topic': 'bkdata_data_monitor_metrics591',
                    'partition': False,
                    'group_id': 'dmonitor',
                    'batch_message_max_count': 5000,
                    'batch_message_timeout': 0.1,
                },
                'task_pool_size': 100,
            }
        """
        configs = kwargs.pop("configs", {})

        super(DataDelayMetricsTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._logical_parents = {}
        self._metric_cache = {}
        self._last_generate_time = now + self.PENDING_TIME
        self._cache_last_refresh_time = None

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新埋点解析依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        if (
            self._cache_last_refresh_time
            and now - self._cache_last_refresh_time < self.CACHE_REFRESH_INTERVAL
        ):
            return

        gevent.spawn(
            self.refresh_metadata,
            self._logical_parents,
            self.refresh_logical_parents,
            update=False,
        )

        self._cache_last_refresh_time = now

    def refresh_logical_parents(self):
        """刷新逻辑节点的上游关系"""
        data_operations = self.fetch_data_operations_from_redis()

        logical_parents = {}
        for data_operation_id, data_operation in data_operations.items():
            for output in data_operation.get("outputs", []):
                logical_key = (output["data_set_id"], output["storage_key"])
                if logical_key not in logical_parents:
                    logical_parents[logical_key] = {"parents": []}
                for input_item in data_operation.get("inputs", []):
                    input_logical_key = (
                        input_item["data_set_id"],
                        input_item["storage_key"],
                    )
                    logical_parents[logical_key]["parents"].append(input_logical_key)
        return logical_parents

    def generate_storage_key(self, relation):
        """生成存储的唯一Key

        :param relation: 关联关系
        """
        storage_id = None
        storage_type = relation["storage_type"]
        if storage_type == "storage":
            storage_id = relation["storage_cluster_config_id"]
        elif storage_type == "channel":
            storage_id = relation["channel_cluster_config_id"]
        return "{}_{}".format(storage_type, storage_id)

    def handle_monitor_value(self, message, now):
        """处理各个模块上报的任务埋点

        :param message: 延迟原始指标
            {
                "time": 1542960360.000001,
                "database": "monitor_data_metrics",
                "data_delay_max": {
                    "waiting_time": 1542960360,
                    "data_time": 1542960360,
                    "delay_time": 1542960360,
                    "output_time": 1542960360,
                    "tags": {
                        "module": "stream",
                        "component": "flink",
                        "cluster": null,
                        "storage": "channel_11",
                        "logical_tag": "591_test1119str",
                        "physical_tag": "171_1fe25fadfef54a4899d781fc9d1e55d3|591_test1119str|0"
                    }
                }
            }
        :param now: 当前处理数据的时间
        """
        try:
            if "data_delay_max" in message:
                metric = DataDelayMax.from_message(message)
                logical_key = (metric.get_tag("logical_tag"), metric.get_tag("storage"))

                metric_time = str(floor_minute(metric.timestamp))

                if logical_key not in self._metric_cache:
                    self._metric_cache[logical_key] = {}

                if metric_time in self._metric_cache[logical_key]:
                    cur_metric = self._metric_cache[logical_key][metric_time]
                    if cur_metric.get_metric("delay_time") < metric.get_metric(
                        "delay_time"
                    ):
                        self._metric_cache[logical_key][metric_time] = metric
                else:
                    self._metric_cache[logical_key][metric_time] = metric
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def do_monitor(self, now, task_pool):
        """执行衍生指标或者监控的逻辑

        :param now: 当前时间戳
        :param task_pool: 任务处理协程池
        """
        if now - self._last_generate_time > self.GENERATE_INTERVAL:
            self.generate_delay_metrics(now)
            self.clear_cache_metrics(now)
            self._last_generate_time = now

    def generate_delay_metrics(self, now):
        """生成延迟指标

        :param now: 当前时间戳
        """
        self.check_metadata(self._logical_parents, 60, "logical parents")

        timeout_minute_time = str(floor_minute(now - self.METRIC_TIMEOUT))

        for logical_key, metrics in self._metric_cache.items():
            for metric_minute_time, metric in metrics.items():
                if metric_minute_time > timeout_minute_time:
                    continue
                delay_time = int(metric.get_metric("delay_time") or 0)
                window_time = int(metric.get_metric("window_time") or 0)
                waiting_time = int(metric.get_metric("waiting_time") or 0)
                ab_delay = delay_time - window_time - waiting_time
                fore_delay = 0
                rel_delay = ab_delay

                try:
                    if ab_delay < 0:
                        ab_delay = 0
                    if ab_delay < 60:
                        # 当前节点无延迟
                        continue
                    # 当前节点的绝对延迟大于1分钟， 需要计算相对延迟来判断延迟发生的具体位置
                    parents = self._logical_parents.get(logical_key, {}).get(
                        "parents", []
                    )
                    if (not parents) or (len(parents) < 1):
                        continue
                    max_parent_delay = 0
                    max_parent_logical_key = False
                    for parent_logical_key in parents:
                        parent_metric = self._metric_cache.get(
                            parent_logical_key, {}
                        ).get(metric_minute_time, False)
                        if not parent_metric:
                            continue
                        parent_delay = int(parent_metric.get_metric("delay_time") or 0)
                        if parent_delay > max_parent_delay:
                            max_parent_delay = parent_delay
                            max_parent_logical_key = parent_logical_key
                    if not max_parent_logical_key:
                        continue

                    fore_delay = max_parent_delay
                    rel_delay = ab_delay - fore_delay
                    if rel_delay < 0:
                        rel_delay = 0
                    if rel_delay < 300:
                        continue
                finally:
                    delay_metric = {
                        "time": metric.timestamp,
                        "database": "monitor_data_metrics",
                        "data_relative_delay": {
                            "ab_delay": int(ab_delay),
                            "window_time": int(window_time),
                            "waiting_time": int(waiting_time),
                            "relative_delay": int(rel_delay),
                            "fore_delay": int(fore_delay),
                            "tags": metric.tags,
                        },
                    }
                    metric_message = json.dumps(delay_metric)
                    self.produce_metric(
                        DMONITOR_TOPICS["data_cleaning"], metric_message
                    )
                    self.produce_metric(
                        DMONITOR_TOPICS["data_delay_metric"], metric_message
                    )

    def clear_cache_metrics(self, now):
        """ """
        timeout_minute_time = str(floor_minute(now - self.METRIC_TIMEOUT))

        timeout_logical_keys = []
        for logical_key, metrics in self._metric_cache.items():
            timeout_minutes = []
            for minute_time in metrics.keys():
                if minute_time <= timeout_minute_time:
                    timeout_minutes.append(minute_time)

            for minute_time in timeout_minutes:
                del self._metric_cache[logical_key][minute_time]

            if len(self._metric_cache[logical_key]) == 0:
                timeout_logical_keys.append(logical_key)

        for logical_key in timeout_logical_keys:
            del self._metric_cache[logical_key]
