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
import copy
import json
import logging
import time

import gevent
from gevent import monkey

from utils.time import timetostr

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.metrics.base import DataLossOutputTotal, KafkaTopicMessageCnt
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def no_data_alert():
    logging.info("Start to execute no data monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "data_io_total",
            "partition": False,
            "group_id": "dmonitor_nodata",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = NoDataAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init no data alert task".format(error=e),
            exc_info=True,
        )


class NoDataAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 60
    PENDING_TIME = 0
    CACHE_REFRESH_INTERVAL = 180
    CACHE_REFRESH_ERROR_INTERVAL = 180

    ALERT_CODE = AlertCode.NO_DATA.value
    ALERT_MESSAGE = "{entity_display}在{time_str}持续{time_display}无数据"
    ALERT_MESSAGE_EN = (
        "There is no output data at {time_str} lasted for {time_display_en}."
    )
    ALERT_FULL_MESSAGE = "{entity_display}({logical_tag})在{time_str}持续{time_display}无数据"
    ALERT_FULL_MESSAGE_EN = (
        "There is no output data about {entity_display_en}({logical_tag}) "
        "at {time_str} lasted for {time_display_en}."
    )

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

        super(NoDataAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._dataflow_metric_cache = {}
        self._rawdata_metric_cache = {}
        self._flow_infos = {}
        self._alert_configs = []
        self._raw_data_topics = {}
        self._last_detect_time = now + self.PENDING_TIME
        self._cache_last_refresh_time = None

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新数据延迟监控依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        if (
            self._cache_last_refresh_time
            and now - self._cache_last_refresh_time < self.CACHE_REFRESH_INTERVAL
        ):
            return

        gevent.spawn(
            self.refresh_metadata,
            self._flow_infos,
            self.fetch_flow_infos_from_redis,
            default=copy.deepcopy(self._flow_infos),
            update=False,
            callback=self.refresh_topic_info,
        )

        self._cache_last_refresh_time = now

    def refresh_topic_info(self, flow_infos):
        """
        刷新数据源的Topic元数据信息
        :param data_sets:
        """
        self._raw_data_topics = {}
        for flow_id, flow_info in flow_infos.items():
            if flow_info.get("flow_type") == "rawdata":
                topic = flow_info.get("topic")
                self._raw_data_topics[topic] = flow_info

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
            self.check_metadata(
                self._flow_infos, self.CACHE_REFRESH_ERROR_INTERVAL, "flow_infos"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = int(time.time())
            return

        try:
            if "data_loss_output_total" in message:
                metric = DataLossOutputTotal.from_message(message)
                self.handle_output_total_metric(metric)
            elif "kafka_topic_message_cnt" in message:
                metric = KafkaTopicMessageCnt.from_message(message)
                self.handle_kafka_message_count_metric(metric)
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def handle_output_total_metric(self, metric):
        flow_id = metric.get_tag("flow_id")
        node_id = metric.get_tag("node_id")
        storage = metric.get_tag("storage")
        logical_key = self.gen_logical_key(metric.tags)
        if not flow_id or str(flow_id) not in self._flow_infos:
            return
        if not storage or storage == "None":
            return
        flow_info = self._flow_infos[str(flow_id)]

        if flow_info.get("flow_type") == "dataflow":
            if flow_id not in self._dataflow_metric_cache:
                self._dataflow_metric_cache[flow_id] = {}
            if node_id not in self._dataflow_metric_cache[flow_id]:
                self._dataflow_metric_cache[flow_id][node_id] = {}

            if metric.get_metric("data_inc", 0) == 0:
                if logical_key not in self._dataflow_metric_cache[flow_id][node_id]:
                    self._dataflow_metric_cache[flow_id][node_id][logical_key] = {
                        "start": metric,
                        "end": metric,
                    }
                else:
                    self._dataflow_metric_cache[flow_id][node_id][logical_key][
                        "end"
                    ] = metric
            else:
                if logical_key in self._dataflow_metric_cache[flow_id][node_id]:
                    del self._dataflow_metric_cache[flow_id][node_id][logical_key]
        else:
            if flow_id not in self._rawdata_metric_cache:
                self._rawdata_metric_cache[flow_id] = {
                    "nodes": {},
                    "first_detect_time": None,
                }
            if node_id not in self._rawdata_metric_cache[flow_id]["nodes"]:
                self._rawdata_metric_cache[flow_id]["nodes"][node_id] = {}

            if logical_key in self._rawdata_metric_cache[flow_id]["nodes"][node_id]:
                if (
                    metric.timestamp
                    < self._rawdata_metric_cache[flow_id]["nodes"][node_id][
                        logical_key
                    ].timestamp
                ):
                    return

            if (
                metric.get_tag("module") == "collector"
                and metric.get_metric("data_inc") == 0
            ):
                return
            self._rawdata_metric_cache[flow_id]["nodes"][node_id][logical_key] = metric

    def handle_kafka_message_count_metric(self, metric):
        topic = metric.get_tag("topic")
        flow_info = self._raw_data_topics.get(topic, {})

        if not flow_info:
            return

        logical_key = "access_{}".format(flow_info.get("id"))
        flow_id = flow_info.get("flow_id")
        # 这里设置默认node_id为None，保证后面进行检测的逻辑跟dataflow的统一
        node_id = None

        if flow_id not in self._rawdata_metric_cache:
            self._rawdata_metric_cache[flow_id] = {
                "nodes": {},
                "first_detect_time": None,
            }

        if node_id not in self._rawdata_metric_cache[flow_id]["nodes"]:
            self._rawdata_metric_cache[flow_id]["nodes"][node_id] = {}

        if logical_key in self._rawdata_metric_cache[flow_id]["nodes"][node_id]:
            if (
                metric.timestamp
                < self._rawdata_metric_cache[flow_id]["nodes"][node_id][
                    logical_key
                ].timestamp
            ):
                return

        if metric.get_metric("cnt") == 0:
            return

        self._rawdata_metric_cache[flow_id]["nodes"][node_id][logical_key] = metric

    def do_monitor(self, now, task_pool):
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self._alert_configs = self.fetch_alert_configs()
            self.detect_metrics(now)
            self._last_detect_time = now

    def detect_metrics(self, now):
        for alert_config in self._alert_configs:
            for target in alert_config.get("monitor_target", []):
                monitor_config = alert_config.get("monitor_config", {})
                if (
                    self.ALERT_CODE not in monitor_config
                    or monitor_config[self.ALERT_CODE]["monitor_status"] == "off"
                ):
                    continue

                flow_id, node_id = self.get_flow_node_by_target(target)
                if flow_id is not None:
                    self.monitor_flow_metrics(
                        flow_id, node_id, alert_config, target, now
                    )

    def monitor_flow_metrics(self, flow_id, node_id, alert_config, target, now):
        try:
            self.check_metadata(
                self._flow_infos, self.CACHE_REFRESH_ERROR_INTERVAL, "flow_infos"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = now
            return

        no_data_interval = alert_config["monitor_config"]["no_data"].get(
            "no_data_interval", 600
        )
        if not flow_id or str(flow_id) not in self._flow_infos:
            return

        flow_info = self._flow_infos[str(flow_id)]

        if flow_info.get("flow_type") == "dataflow":
            self.monitor_dataflow_no_data(
                flow_id, node_id, flow_info, alert_config, no_data_interval, target, now
            )
        else:
            self.monitor_rawdata_no_data(
                flow_id, node_id, flow_info, alert_config, no_data_interval, target, now
            )

    def monitor_dataflow_no_data(
        self, flow_id, node_id, flow_info, alert_config, no_data_interval, target, now
    ):
        """dataflow 无数据检测"""
        if flow_info.get("status") != "running":
            if flow_id in self._dataflow_metric_cache:
                del self._dataflow_metric_cache[flow_id]
            return

        if flow_id not in self._dataflow_metric_cache:
            return

        # 如果flow在running状态中超过指定时间没有任何数据量超过0的打点出现，触发告警
        if node_id is None:
            for node_id, node in self._dataflow_metric_cache[flow_id].items():
                for logical_key, logical_metric in node.items():
                    no_data_time = (
                        logical_metric["end"].timestamp
                        - logical_metric["start"].timestamp
                    )
                    if no_data_time >= no_data_interval:
                        self.generate_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            logical_metric["end"],
                            no_data_time,
                            now,
                        )
        else:
            node_info = flow_info["nodes"].get(str(node_id), {})
            if node_info.get("status") not in ("running", "started"):
                return

            if node_id in self._dataflow_metric_cache[flow_id]:
                for logical_key, logical_metric in self._dataflow_metric_cache[flow_id][
                    node_id
                ].items():
                    no_data_time = (
                        logical_metric["end"].timestamp
                        - logical_metric["start"].timestamp
                    )
                    if no_data_time >= no_data_interval:
                        self.generate_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            logical_metric["end"],
                            no_data_time,
                            now,
                        )

    def monitor_rawdata_no_data(
        self, flow_id, node_id, flow_info, alert_config, no_data_interval, target, now
    ):
        """rawdataflow 无数据检测"""
        # 记录flow第一次进行检测的时间
        if flow_id not in self._rawdata_metric_cache:
            self._rawdata_metric_cache[flow_id] = {
                "nodes": {},
                "first_detect_time": now,
            }
            return
        if self._rawdata_metric_cache[flow_id]["first_detect_time"] is None:
            self._rawdata_metric_cache[flow_id]["first_detect_time"] = now

        # 如果flow在running状态中超过指定时间没有任何打点出现，触发告警
        metric = None
        if node_id is None:
            for node_id, node in self._rawdata_metric_cache[flow_id]["nodes"].items():
                for logical_key, logical_metric in node.items():
                    metric = logical_metric
                    no_data_time = now - logical_metric.timestamp
                    if no_data_time >= no_data_interval:
                        self.generate_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            metric,
                            no_data_time,
                            now,
                        )

            if metric is None:
                no_data_time = (
                    now - self._rawdata_metric_cache[flow_id]["first_detect_time"]
                )
                if no_data_time >= no_data_interval:
                    self.generate_alert(
                        alert_config,
                        target,
                        flow_id,
                        node_id,
                        metric,
                        no_data_time,
                        now,
                    )
        else:
            node_info = flow_info["nodes"].get(str(node_id), {})
            if node_info.get("status") not in ("running", "started"):
                return

            if node_id not in self._rawdata_metric_cache[flow_id]["nodes"]:
                no_data_time = (
                    now - self._rawdata_metric_cache[flow_id]["first_detect_time"]
                )
                if no_data_time >= no_data_interval:
                    self.generate_alert(
                        alert_config,
                        target,
                        flow_id,
                        node_id,
                        metric,
                        no_data_time,
                        now,
                    )
            else:
                for logical_key, logical_metric in self._rawdata_metric_cache[flow_id][
                    "nodes"
                ][node_id].items():
                    metric = logical_metric
                    no_data_time = now - logical_metric.timestamp
                    if no_data_time >= no_data_interval:
                        self.generate_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            metric,
                            no_data_time,
                            now,
                        )

                if metric is None:
                    no_data_time = (
                        now - self._rawdata_metric_cache[flow_id]["first_detect_time"]
                    )
                    if no_data_time >= no_data_interval:
                        self.generate_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            metric,
                            no_data_time,
                            now,
                        )

    def generate_alert(
        self, alert_config, target, flow_id, node_id, metric, no_data_time, now
    ):
        flow_info = self._flow_infos.get(str(flow_id), {})

        time_display, time_display_en = self.convert_display_time(no_data_time)
        if metric is None:
            time_str = timetostr(now)
            if str(flow_id).startswith("rawdata"):
                logical_tag = flow_id[7:]
            else:
                self._svr.logger.error(
                    "There is a dataflow({flow_id}) which detected no data with no any metrics".format(
                        flow_id=flow_id
                    )
                )
                return
        else:
            time_str = timetostr(metric.timestamp)
            if isinstance(metric, KafkaTopicMessageCnt):
                logical_tag = flow_id[7:]
            else:
                logical_tag = str(metric.get_tag("logical_tag"))
        entity_display, entity_display_en = self.get_logical_tag_display(
            logical_tag, metric.tags if metric is not None else None, flow_info
        )

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": self.ALERT_MESSAGE.format(
                    entity_display=entity_display,
                    time_display=time_display,
                    time_str=time_str,
                ),
                "message_en": self.ALERT_MESSAGE_EN.format(
                    time_display_en=time_display_en, time_str=time_str
                ),
                "full_message": self.ALERT_FULL_MESSAGE.format(
                    entity_display=entity_display,
                    time_display=time_display,
                    time_str=time_str,
                    logical_tag=logical_tag,
                ),
                "full_message_en": self.ALERT_FULL_MESSAGE_EN.format(
                    entity_display_en=entity_display_en,
                    time_display_en=time_display_en,
                    time_str=time_str,
                    logical_tag=logical_tag,
                ),
                "alert_status": AlertStatus.INIT.value,
                "tags": {
                    "alert_level": AlertLevel.WARNING.value,
                    "alert_code": AlertCode.NO_DATA.value,
                    "alert_type": AlertType.DATA_MONITOR.value,
                    "alert_config_id": alert_config.get("id"),
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "data_set_id": logical_tag,
                    "generate_type": alert_config.get("generate_type"),
                },
            },
        }
        if target.get("target_type") == "dataflow":
            if flow_info:
                alert_info["dmonitor_alerts"]["tags"].update(
                    {
                        "project_id": flow_info.get("project_id"),
                        "bk_app_code": flow_info.get("bk_app_code"),
                    }
                )
            if metric is not None:
                alert_info["dmonitor_alerts"]["tags"].update(metric.tags)
        elif target.get("target_type") == "rawdata":
            if flow_info:
                alert_info["dmonitor_alerts"]["tags"].update(
                    {
                        "bk_biz_id": flow_info.get("bk_biz_id"),
                        "bk_app_code": flow_info.get("bk_app_code"),
                        "raw_data_id": flow_info.get("id"),
                    }
                )
            if metric is not None:
                alert_info["dmonitor_alerts"]["tags"].update(metric.tags)
        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)
