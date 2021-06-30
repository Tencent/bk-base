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

from dmonitor.metrics.base import DmonitorAlerts
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def alert_match():
    logging.info("Start to execute alert match task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_raw_alert",
            "partition": False,
            "group_id": "dmonitor",
            "batch_message_max_count": 5000,
            "batch_message_timeout": 0.1,
        },
        "task_pool_size": 100,
    }

    try:
        task = AlertMatchTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init alert match task".format(error=e),
            exc_info=True,
        )


class AlertMatchTaskGreenlet(BaseDmonitorTaskGreenlet):
    CACHE_REFRESH_INTERVAL = 60

    def __init__(self, *args, **kwargs):
        """初始化告警匹配策略的任务

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

        super(AlertMatchTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._alert_config_slots = {"platform_configs": {}, "flow_configs": {}}
        self._alert_configs = []
        self._flow_infos = {}

        self._cache_last_refresh_time = None

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新告警匹配任务依赖的元数据信息

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
                    self._alert_configs,
                    self.fetch_alert_configs,
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._flow_infos,
                    self.fetch_flow_infos,
                    update=False,
                ),
            ]
        )

        self.generate_slots(self._alert_configs)

        self._cache_last_refresh_time = now

    def handle_monitor_value(self, message, now):
        """处理原始告警信息

        :param message: 原始告警信息
        :param now: 当前处理数据的时间
        """
        try:
            if "dmonitor_alerts" in message:
                logging.info("Get raw alert: %s" % message)
                raw_alert = DmonitorAlerts.from_message(message)
                self.check_alert_match(raw_alert, now)
        except Exception as e:
            logging.error(
                "Parse raw alert error: %s, message: %s" % (e, json.dumps(message))
            )

    def check_alert_match(self, raw_alert, now):
        """检查告警是否匹配当前的告警策略

        :param raw_alert: 原始告警
        :param now: 当前时间
        """
        alert_config_slots = self._alert_config_slots

        # 检测所有全局告警配置是否匹配
        for alert_config in alert_config_slots["platform_configs"].values():
            if self.check_platform_alert_match(
                raw_alert, alert_config["target"], alert_config["config"]
            ):
                self.generate_standard_alert(
                    raw_alert, alert_config["target"], alert_config["config"], now
                )

        # 检测flow告警配置是否匹配
        if "flow_id" in raw_alert.tags:
            flow_id = raw_alert.get_tag("flow_id")
            node_id = raw_alert.get_tag("node_id")

            if flow_id not in alert_config_slots["flow_configs"]:
                return

            for alert_config in alert_config_slots["flow_configs"][flow_id][
                "configs"
            ].values():
                if self.check_flow_alert_match(
                    raw_alert, alert_config["target"], alert_config["config"]
                ):
                    self.generate_standard_alert(
                        raw_alert, alert_config["target"], alert_config["config"], now
                    )

            if node_id not in alert_config_slots["flow_configs"][flow_id]["nodes"]:
                return
            for alert_config in alert_config_slots["flow_configs"][flow_id]["nodes"][
                node_id
            ]["alert_configs"].values():
                if self.check_flow_alert_match(
                    raw_alert, alert_config["target"], alert_config["config"]
                ):
                    self.generate_standard_alert(
                        raw_alert, alert_config["target"], alert_config["config"], now
                    )

    def check_platform_alert_match(self, raw_alert, target, config):
        """检查是否匹配上平台的告警策略

        :param raw_alert: 原始告警
        :param target: 告警对象
        :param config: 告警策略配置
        """
        alert_code = raw_alert.get_tag("alert_code")
        if alert_code not in config.get("monitor_config", {}):
            return False

        if (
            config.get("monitor_config", {})
            .get(alert_code, {})
            .get("monitor_status", "off")
            == "off"
        ):
            return False

        dimensions = target.get("dimensions", {})

        for key, value in dimensions.items():
            if key not in raw_alert.tags:
                return False

            if str(raw_alert.get_tag(key)) != str(value):
                return False

        return True

    def check_flow_alert_match(self, raw_alert, target, config):
        """检查是否匹配任务的告警策略

        :param raw_alert: 原始告警
        :param target: 告警对象
        :param config: 告警策略配置
        """
        self.check_metadata(self._flow_infos, self.CACHE_REFRESH_INTERVAL, "flows")

        flow_id = raw_alert.get_tag("flow_id")

        if not flow_id or str(flow_id) not in self._flow_infos:
            return False
        flow_info = self._flow_infos[str(flow_id)]

        # 如果flow不在运行中，则删除该flow的指标缓存
        if (
            flow_info.get("flow_type") == "dataflow"
            and flow_info.get("status") != "running"
        ):
            if flow_id in self._alert_config_slots["flow_configs"]:
                del self._alert_config_slots["flow_configs"][flow_id]
            return False

        alert_code = raw_alert.get_tag("alert_code")
        if alert_code not in config.get("monitor_config", {}):
            return False

        if (
            config.get("monitor_config", {})
            .get(alert_code, {})
            .get("monitor_status", "off")
            == "off"
        ):
            return False

        return True

    def generate_standard_alert(self, raw_alert, target, config, now):
        """生成已关联到某个具体告警策略的告警

        :param raw_alert: 原始告警
        :param target: 告警对象
        :param config: 告警策略配置
        :param now: 当前时间
        """
        flow_id = raw_alert.get_tag("flow_id", None)
        if flow_id and str(flow_id) in self._flow_infos:
            flow_info = self._flow_infos[str(flow_id)]

            raw_alert.set_tag("bk_app_code", flow_info.get("bk_app_code"))
            if target.get("target_type") == "dataflow":
                raw_alert.set_tag("project_id", flow_info.get("project_id"))
            elif target.get("target_type") == "rawdata":
                raw_alert.set_tag("bk_biz_id", flow_info.get("bk_biz_id"))
                raw_alert.set_tag("raw_data_id", flow_info.get("raw_data_id"))

        raw_alert.set_tag("alert_config_id", config.get("id"))
        raw_alert.set_tag("generate_type", config.get("generate_type"))
        alert_message = raw_alert.as_message()

        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)

    def generate_slots(self, alert_configs):
        """生成告警策略插槽

        :param alert_configs: 告警策略列表
        """
        for alert_config in self._alert_configs:
            for target in alert_config.get("monitor_target", []):
                if target.get("target_type") == "platform":
                    self.add_alert_config(
                        self._alert_config_slots["platform_configs"],
                        target,
                        alert_config,
                    )
                else:
                    flow_id, node_id = self.get_flow_node_by_target(target)

                    # 生成flow的指标槽位
                    if flow_id not in self._alert_config_slots["flow_configs"]:
                        self._alert_config_slots["flow_configs"][flow_id] = {
                            "configs": {},
                            "nodes": {},
                        }
                    if node_id is None:
                        self.add_alert_config(
                            self._alert_config_slots["flow_configs"][flow_id][
                                "configs"
                            ],
                            target,
                            alert_config,
                        )
                    else:
                        if node_id not in self._alert_config_slots[flow_id]["nodes"]:
                            self._alert_config_slots["flow_configs"][flow_id]["nodes"][
                                node_id
                            ] = {
                                "configs": {},
                            }
                        self.add_alert_config(
                            self._alert_config_slots["flow_configs"][flow_id]["nodes"][
                                node_id
                            ]["configs"],
                            target,
                            alert_config,
                        )

    def add_alert_config(self, alert_configs, target, alert_config):
        """增加告警策略

        :param alert_configs: 告警策略列表
        :param target: 告警对象
        :param alert_config: 当前待添加的告警策略
        """
        target_id = self.gen_target_id(target)
        alert_configs[target_id] = {
            "target": target,
            "config": alert_config,
        }
