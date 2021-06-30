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

from common.mixins.influx_mixin import InfluxSenderMixin
from utils.time import strtotime
from utils.influx_util import init_influx_conn

from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import INFLUXDB_CONFIG, INFLUX_RETENTION_POLICIES

monkey.patch_all()


def metrics_storage(params):
    logging.info("Start to execute metrics storage task")

    perceive_task_config = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "bkdata_monitor_metrics591",
            "partition": params.get("partition", False),
            "group_id": "dmonitor",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
        "output_type": "influx",
    }

    try:
        task = MetricsStorageTaskGreenlet(configs=perceive_task_config)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init metrics storage task".format(error=e),
            exc_info=True,
        )


class MetricsStorageTaskGreenlet(InfluxSenderMixin, BaseDmonitorTaskGreenlet):
    def __init__(self, *args, **kwargs):
        """初始化刷新元数据到Redis任务的配置

        :param perceive_task_config: 缓存同步任务配置
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

        super(MetricsStorageTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._kafka_offsets = {}
        self._cache_limit = configs.get("cache_limit", 500000)
        self._output_type = configs.get("output_type", "kafka")

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新埋点解析依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        pass

    def init_influx(self):
        try:
            conn = init_influx_conn()

            databases = map(lambda x: x["name"], conn.get_list_database())

            for influx_config in INFLUXDB_CONFIG.values():
                database = influx_config["database"]
                if database not in databases:
                    conn.create_database(database)
                    logging.info("Init influxdb %s" % database)

                conn.switch_database(database)
                try:
                    retention_policies = map(
                        lambda x: x["name"], conn.get_list_retention_policies()
                    )
                    for retention_policy, config in INFLUX_RETENTION_POLICIES.items():
                        if retention_policy not in retention_policies:
                            conn.create_retention_policy(**config)
                        else:
                            conn.alter_retention_policy(**config)
                        logging.info(
                            "Create or update rp(%s) success" % retention_policy
                        )
                except Exception:
                    logging.error("Failed to init influxdb %s and its RPs" % database)
        except Exception as e:
            logging.error("Failed to init influxdb database %s" % str(e))
        return True

    def handle_monitor_message(self, message, now):
        """处理各个模块上报的任务埋点

        :param value: 任务埋点的内容
        :param now: 当前处理数据的时间
        """
        partition = message.partition()
        offset = message.offset()
        if not self._kafka_offsets.get(partition, False):
            self._kafka_offsets[partition] = {"max": 0, "min": 0}
        if self._kafka_offsets[partition]["max"] < offset:
            self._kafka_offsets[partition]["max"] = offset
        if (
            self._kafka_offsets[partition]["min"] > offset
            or self._kafka_offsets[partition]["min"] == 0
        ):
            self._kafka_offsets[partition]["min"] = offset
        value = message.value().decode("utf-8").strip().strip(chr(0))

        try:
            self._consume_count += 1
            self.handle_monitor_value(json.loads(value, encoding="utf-8"), now)
        except Exception as e:
            self._handle_error_count += 1
            logging.error(
                "Handle monitor_data failed, value: %s, error: %s" % (value, e)
            )

    def handle_monitor_value(self, value, now):
        """
        {
            "time": timestamp_str,
            "database": "custom",
            "retention_policy": None,
            "realtime_storm_test_metric1_tpCounter": {
                "max_record_time": timestamp,
                "load": random.randint(20, 100),
                "tags": {
                    "ip": "x.x.x.x"
                }
            }
        }
        """
        # 解析出json中的指标信息
        metrics = self.parse_metrics(value)
        if len(metrics) < 1:
            return True
        # 把指标按照influx格式
        cleaned = self.format_metrics(metrics)
        if (not cleaned) or (len(cleaned) < 1):
            return True
        # 输出指标
        for database in cleaned.keys():
            for rp in cleaned[database].keys():
                for msg in cleaned[database][rp]:
                    length = self._influx_sending.qsize()
                    while length > self._cache_limit:
                        gevent.sleep(1)
                        length = self._influx_sending.qsize()
                    self._influx_sending.put(
                        {"database": database, "retention_policy": rp, "message": msg}
                    )
        return True

    def parse_metrics(self, message):
        metrics = []
        # 时间与DB信息
        report_time = message.get("time")
        if isinstance(report_time, str):
            try:
                report_timestamp = strtotime(report_time)
            except Exception:
                report_timestamp = int(report_time)
        else:
            report_timestamp = report_time
        database = message.get("database", "monitor_custom_metrics")
        retention_policy = message.get("retention_policy", None)
        # 指标信息
        for key in message.keys():
            item = message[key]
            if type(item) is not dict:
                # 指标信息都是dict
                continue
            metric_name = key
            fields = {}
            tags = item.get("tags", {})
            for field_key in item.keys():
                if field_key == "tags":
                    continue
                fields[field_key] = item[field_key]
            metrics.append(
                {
                    "time": report_timestamp,
                    "database": database,
                    "retention_policy": retention_policy,
                    "metric_name": metric_name,
                    "fields": fields,
                    "tags": tags,
                }
            )
        return metrics

    def format_metrics(self, metrics):
        influx_metrics = {}
        for metric in metrics:
            try:
                # 格式 [key] [fields] [timestamp]
                # key格式 [metric名,tag1=tag1value,tag2=tag2value]
                metric_name = self.escape_influx_lineprotocal(
                    metric.get("metric_name", "")
                )
                tags = metric.get("tags", {})
                tag_strs = []
                for tag_key in sorted(tags.keys()):
                    tag_value = self.escape_influx_lineprotocal(tags[tag_key])
                    if tag_value == "":
                        tag_value = "unknown"
                    tag_str = "%s=%s" % (
                        self.escape_influx_lineprotocal(tag_key),
                        tag_value,
                    )
                    tag_strs.append(tag_str)
                tag_str = ",".join(tag_strs)
                if tag_str == "":
                    key = metric_name
                else:
                    key = "%s,%s" % (metric_name, tag_str)

                # fields
                field_strs = []
                for field_key in metric.get("fields", {}):
                    field_name = self.escape_influx_lineprotocal(field_key)
                    field_value = self.format_influx_number(
                        metric.get("fields", {}).get(field_key, "")
                    )
                    field_strs.append("%s=%s" % (field_name, field_value))
                field_str = ",".join(field_strs)
                if field_str == "":
                    continue

                final_str = "%s %s" % (key, field_str)
                database = metric.get("database", "")
                retention_policy = metric.get("retention_policy", None)
                if not influx_metrics.get(database, False):
                    influx_metrics[database] = {}
                if not influx_metrics[database].get(retention_policy, None):
                    influx_metrics[database][retention_policy] = []
                # timestamp
                timestamp = metric.get("time", "")
                if timestamp == "":
                    influx_metrics[database][retention_policy].append(final_str)
                    continue
                final_str = "%s %d" % (
                    final_str,
                    self.timestamp_to_nanosecond(timestamp),
                )
                if self._output_type == "kafka":
                    final_str = final_str.encode("utf-8")
                influx_metrics[database][retention_policy].append(final_str)
            except Exception as e:
                logging.error("convert metric %s format exception, %s" % (metric, e))
                continue
        return influx_metrics

    def format_influx_number(self, origin):
        # 判断origin如果不是一个数字， 则返回双引号包围的字符串
        if not self.is_number(origin):
            return '"%s"' % self.escape_custom(origin, '"')
        # 判断是否是小数
        if "." in "%s" % origin:
            return "%s" % origin
        # 整数需要加i后缀
        return "%si" % origin

    def is_number(self, number_str):
        if isinstance(number_str, str):
            return False

        try:
            float(number_str)
            return True
        except Exception:
            pass
        return False

    def escape_influx_lineprotocal(self, origin):
        escaped = origin
        # 转义空格
        escaped = self.escape_custom(escaped, " ")
        escaped = self.escape_custom(escaped, ",")
        return escaped

    def escape_custom(self, origin, custom=" "):
        escaped = "%s" % origin
        # 转义空格
        escaped = escaped.replace(custom, "\\" + custom)
        return escaped

    def timestamp_to_nanosecond(self, timestamp):
        return int(int(timestamp or time.time()) * 1000 * 1000 * 1000 or timestamp)
