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

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.metrics.base import DataLossDropRate
from dmonitor.settings import DMONITOR_TOPICS
from utils.time import timetostr

monkey.patch_all()


def data_drop_alert():
    logging.info("Start to execute data drop monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "data_drop_metric",
            "partition": False,
            "group_id": "dmonitor",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = DataDropAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init data drop alert task".format(error=e),
            exc_info=True,
        )


class DataDropAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 60
    PENDING_TIME = 60
    CACHE_REFRESH_INTERVAL = 60

    ALERT_CODE = AlertCode.DATA_DROP.value
    ALERT_MESSAGE = "{entity_display}在{time_str}数据无效率超过{drop_rate_threshold}%，当前无效数据率: {drop_rate}%， 无效条数: {drop_cnt}"
    ALERT_MESSAGE_EN = (
        "Rate of invalid data about {entity_display_en} had exceeded more than {drop_rate_threshold}% at ${time_str}. "
        "The rate of invalid data is {drop_rate}% and the amount of invalid is {drop_cnt} currently."
    )
    ALERT_FULL_MESSAGE = (
        "{entity_display}({logical_tag})在{time_str}数据无效率超过{drop_rate_threshold}%，"
        "当前无效数据率: {drop_rate}%， 无效条数: {drop_cnt}"
    )
    ALERT_FULL_MESSAGE_EN = (
        "Rate of invalid data about {entity_display_en}({logical_tag}) had exceeded more than {drop_rate_threshold}% "
        "at ${time_str}. The rate of invalid data is {drop_rate}% and the amount of invalid is {drop_cnt} currently."
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

        super(DataDropAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._alert_config_slots = {"platform_configs": {}, "flow_configs": {}}
        self._alert_configs = []
        self._flow_infos = {}
        self._metric_cache = {}
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

        gevent.joinall(
            [
                gevent.spawn(
                    self.refresh_metadata,
                    self._flow_infos,
                    self.fetch_flow_infos,
                    args={"with_nodes": True},
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._alert_configs,
                    self.fetch_alert_configs,
                    update=False,
                ),
            ]
        )
        self.generate_metrics_slots(self._alert_configs)

        if self._cache_last_refresh_time:
            self.clear_metrics_slots(int(float(now - self._cache_last_refresh_time)))

        self._cache_last_refresh_time = now

    def generate_metrics_slots(self, now):
        for alert_config in self._alert_configs:
            for target in alert_config.get("monitor_target", []):
                flow_id, node_id = self.get_flow_node_by_target(target)

                if (not self.check_alert_config_valid(alert_config)) or (
                    not self.check_flow_valid(flow_id)
                ):
                    self.remove_alert_config_by_flow_id(flow_id)
                    continue

                # 生成flow的指标槽位
                if flow_id not in self._metric_cache:
                    self._metric_cache[flow_id] = {
                        "alert_configs": {},
                        "nodes": {},
                    }
                if node_id is None:
                    self.add_alert_config(
                        self._metric_cache[flow_id]["alert_configs"],
                        target,
                        alert_config,
                    )
                else:
                    if node_id not in self._metric_cache[flow_id]["nodes"]:
                        self._metric_cache[flow_id]["nodes"][node_id] = {
                            "alert_configs": {},
                        }
                    self.add_alert_config(
                        self._metric_cache[flow_id]["nodes"][node_id]["alert_configs"],
                        target,
                        alert_config,
                    )

    def clear_metrics_slots(self, recent_updated):
        disabled_alert_configs = self.fetch_disabled_alert_configs(
            recent_updated=recent_updated
        )
        for alert_config in disabled_alert_configs:
            for target in alert_config.get("monitor_target", []):
                flow_id, node_id = self.get_flow_node_by_target(target)

                if not alert_config.get("active"):
                    self.remove_alert_config_by_flow_id(flow_id)

    def check_alert_config_valid(self, alert_config):
        if self.ALERT_CODE not in alert_config["monitor_config"]:
            return False

        if (
            alert_config["monitor_config"][self.ALERT_CODE].get("monitor_status", "off")
            == "off"
        ):
            return False

        return True

    def check_flow_valid(self, flow_id):
        if not flow_id or str(flow_id) not in self._flow_infos:
            return False

        flow_info = self._flow_infos[str(flow_id)]
        # 如果flow不在运行中，则删除该flow的指标缓存
        if (
            flow_info.get("flow_type") == "dataflow"
            and flow_info.get("status") != "running"
        ):
            return False

        return True

    def remove_alert_config_by_flow_id(self, flow_id):
        if flow_id in self._metric_cache:
            del self._metric_cache[flow_id]

    def add_alert_config(self, alert_configs, target, alert_config):
        alert_config_id = alert_config.get("id")
        if alert_config_id in alert_configs:
            if not self.same_alert_config(
                alert_configs[alert_config_id]["config"], alert_config
            ):
                alert_configs[alert_config_id] = {
                    "target": target,
                    "config": alert_config,
                }
        else:
            alert_configs[alert_config_id] = {"target": target, "config": alert_config}

    def same_alert_config(self, alert_config, other_alert_config):
        data_drop_config = alert_config["monitor_config"].get(self.ALERT_CODE, {})
        other_data_drop_config = other_alert_config["monitor_config"].get(
            self.ALERT_CODE, {}
        )
        for key in data_drop_config.keys():
            if data_drop_config[key] != other_data_drop_config[key]:
                return False
        return True

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
            if "data_loss_drop_rate" in message:
                metric = DataLossDropRate.from_message(message)
                flow_id = metric.get_tag("flow_id")
                node_id = metric.get_tag("node_id")
                storage = metric.get_tag("storage")
                if not storage or storage == "None":
                    return
                logical_key = self.gen_logical_key(metric.tags)
                self.monitor_metric(flow_id, node_id, logical_key, metric, now)
        except Exception as e:
            logging.error(
                "Combine data error: {}, message: {}".format(e, json.dumps(message)),
                exc_info=True,
            )

    def monitor_metric(self, flow_id, node_id, logical_key, metric, now):
        if flow_id not in self._metric_cache:
            return
        for alert_config_item in self._metric_cache[flow_id]["alert_configs"].values():
            alert_config = alert_config_item.get("config", {})
            target = alert_config_item.get("target", {})
            max_drop_rate = alert_config["monitor_config"][self.ALERT_CODE].get(
                "drop_rate", 30
            )

            if metric.get_metric("drop_rate") > max_drop_rate:
                self.generate_alert(
                    alert_config, target, flow_id, node_id, max_drop_rate, metric, now
                )

        if node_id not in self._metric_cache[flow_id]["nodes"]:
            return
        for alert_config_item in self._metric_cache[flow_id]["nodes"][node_id][
            "alert_configs"
        ].values():
            alert_config = alert_config_item.get("config", {})
            target = alert_config_item.get("target", {})
            max_drop_rate = alert_config["monitor_config"][self.ALERT_CODE].get(
                "drop_rate", 30
            )

            if metric.get_metric("drop_rate") > max_drop_rate:
                self.generate_alert(
                    alert_config, target, flow_id, node_id, max_drop_rate, metric, now
                )

    def generate_alert(
        self, alert_config, target, flow_id, node_id, max_drop_rate, metric, now
    ):
        flow_info = self._flow_infos.get(str(flow_id), {})

        logical_tag = str(metric.get_tag("logical_tag"))
        entity_display, entity_display_en = self.get_logical_tag_display(
            logical_tag, metric.tags, flow_info
        )
        time_str = timetostr(now)

        message = self.ALERT_MESSAGE.format(
            entity_display=entity_display,
            time_str=time_str,
            drop_rate_threshold=max_drop_rate,
            drop_cnt=metric.get_metric("drop_cnt"),
            drop_rate=metric.get_metric("drop_rate"),
        )
        message_en = self.ALERT_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            time_str=time_str,
            drop_rate_threshold=max_drop_rate,
            drop_cnt=metric.get_metric("drop_cnt"),
            drop_rate=metric.get_metric("drop_rate"),
        )
        full_message = self.ALERT_FULL_MESSAGE.format(
            entity_display=entity_display,
            logical_tag=logical_tag,
            time_str=time_str,
            drop_rate_threshold=max_drop_rate,
            drop_cnt=metric.get_metric("drop_cnt"),
            drop_rate=metric.get_metric("drop_rate"),
        )
        full_message_en = self.ALERT_FULL_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            logical_tag=logical_tag,
            time_str=time_str,
            drop_rate_threshold=max_drop_rate,
            drop_cnt=metric.get_metric("drop_cnt"),
            drop_rate=metric.get_metric("drop_rate"),
        )

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": message,
                "message_en": message_en,
                "full_message": full_message,
                "full_message_en": full_message_en,
                "alert_status": AlertStatus.INIT.value,
                "tags": {
                    "alert_level": AlertLevel.WARNING.value,
                    "alert_code": AlertCode.DATA_DROP.value,
                    "alert_type": AlertType.DATA_MONITOR.value,
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "alert_config_id": alert_config.get("id"),
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
            alert_info["dmonitor_alerts"]["tags"].update(metric.tags)
        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)
