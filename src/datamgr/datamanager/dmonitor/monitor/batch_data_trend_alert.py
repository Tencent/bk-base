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
from common.influxdb import influxdb_connections
from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.metrics.base import DataLossOutputTotal
from dmonitor.settings import DMONITOR_TOPICS
from gevent import monkey
from utils.time import floor_hour

monkey.patch_all()


def batch_data_trend_alert():
    logging.info("Start to execute batch data trend monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_batch_output_total",
            "partition": False,
            "group_id": "dmonitor_batch_trend",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 300,
    }

    try:
        task = BatchDataTrendAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init batch data trend monitor task".format(
                error=e
            ),
            exc_info=True,
        )


class BatchDataTrendAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    """
    最近M分钟数据量平均值，与N天前同一时间范围内数据量平均值相比
    阈值超过K则告警
    """

    DETECT_INTERVAL = 60
    PENDING_TIME = 900
    CACHE_REFRESH_INTERVAL = 60
    CACHE_REFRESH_ERROR_INTERVAL = 300

    TASK_TIMEOUT = 10
    CLEAR_TASK_INTERVAL = 300
    TASK_THRESHOLD = 10
    JOIN_TASK_INTERVAL = 300
    WINDOW_START_DIFF = -1800
    WINDOW_END_DIFF = 1800

    ALERT_CODE = AlertCode.DATA_TREND.value
    ALERT_LEVEL = AlertLevel.WARNING.value
    ALERT_TYPE = AlertType.DATA_MONITOR.value

    ALERT_MESSAGE = (
        "{entity_display}最近一次调度结果数据量为{data_total_cnt}条，"
        "比{period_time_display}前同比{diff_trend}{diff_count}，{period_time_display}前调度结果数据量为{last_total_cnt}条"
    )
    ALERT_MESSAGE_EN = (
        "The amount of data about {entity_display_en} in the last scheduling is {data_total_cnt}, "
        "{diff_trend_en} {diff_count} compared with the amount of data which is {last_total_cnt} "
        "in previous scheduling {period_time_display_en} ago."
    )
    ALERT_FULL_MESSAGE = (
        "{entity_display}({logical_tag})最近一次调度结果数据量为{data_total_cnt}条，"
        "比{period_time_display}前同比{diff_trend}{diff_count}，{period_time_display}前调度结果数据量为{last_total_cnt}条"
    )
    ALERT_FULL_MESSAGE_EN = (
        "The amount of data about {entity_display_en}({logical_tag}) in the last scheduling is {data_total_cnt}, "
        "{diff_trend_en} {diff_count} compared with the amount of data which is {last_total_cnt} "
        "in previous scheduling {period_time_display_en} ago."
    )

    def __init__(self, *args, **kwargs):
        """初始化数据波动告警任务

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

        super(BatchDataTrendAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._flow_infos = {}
        self._alert_configs = []
        self._flow_alert_configs = {}
        self._last_detect_time = now
        self._debug_data_sets = configs.get("debug_data_sets", [])

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
                    self._alert_configs,
                    self.fetch_alert_configs,
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._flow_infos,
                    self.fetch_flow_infos_from_redis,
                    update=False,
                ),
            ]
        )

        self.generate_flow_alert_configs()
        if self._cache_last_refresh_time:
            self.clear_metrics_slots(int(float(now - self._cache_last_refresh_time)))

        self._cache_last_refresh_time = now

    def handle_monitor_value(self, message, now):
        """
        {
            "time": 1542960360.000001,
            "database": "monitor_data_metrics",
            "data_loss_output_total": {
                "data_cnt": 100,
                "data_inc": 10,
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
        """
        try:
            if "data_loss_output_total" in message:
                metric, alert_config = self.extract_metric_and_alert_config(message)

                self.detect_metric_trend(metric, alert_config)
        except Exception as e:
            logging.error(
                "Combine data error: {}, message: {}".format(e, json.dumps(message))
            )

    def extract_metric_and_alert_config(self, message):
        metric = DataLossOutputTotal.from_message(message)
        flow_id = str(metric.get_tag("flow_id"))
        node_id = str(metric.get_tag("node_id"))
        storage = metric.get_tag("storage")
        if not flow_id:
            return
        if not storage or storage == "None":
            return

        flow_alert_config = self._flow_alert_configs.get(flow_id, {})
        if node_id:
            alert_config = flow_alert_config.get("nodes", {}).get(node_id, {})
            if not alert_config:
                alert_config = flow_alert_config.get("alert_config", {})
        else:
            alert_config = flow_alert_config.get("alert_config", {})
        return metric, alert_config

    def detect_metric_trend(self, metric, alert_config):
        flow_id = str(metric.get_tag("flow_id"))
        node_id = str(metric.get_tag("node_id"))
        hourtimestamp = floor_hour(metric.timestamp)

        diff_period = alert_config["monitor_config"][self.ALERT_CODE].get(
            "diff_period", 1
        )
        diff_count = alert_config["monitor_config"][self.ALERT_CODE].get(
            "diff_count", 1
        )
        diff_unit = alert_config["monitor_config"][self.ALERT_CODE].get(
            "diff_unit", "percent"
        )
        diff_trend = alert_config["monitor_config"][self.ALERT_CODE].get(
            "diff_trend", "both"
        )

        if not flow_id or str(flow_id) not in self._flow_infos:
            return
        data_set_id = metric.get_tag("logical_tag")

        recent_cnt = metric.get_metric("data_inc", 0)
        history_cnt = self.get_history_cnt(hourtimestamp, diff_period, metric.tags)

        if data_set_id in self._debug_data_sets:
            logging.info(
                "Data set: {}, recent: {}, history: {}".format(
                    data_set_id, recent_cnt, history_cnt
                )
            )

        if diff_trend == "both":
            diff = abs(recent_cnt - history_cnt)
            diff_trend = "increase" if recent_cnt > history_cnt else "decrease"
        elif diff_trend == "increase":
            if recent_cnt < history_cnt:
                return
            diff = recent_cnt - history_cnt
        elif diff_trend == "decrease":
            if recent_cnt > history_cnt:
                return
            diff = history_cnt - recent_cnt
        else:
            return

        if diff_unit == "percent":
            if history_cnt > 0:
                if diff * 1.0 / history_cnt > diff_count * 0.01:
                    self.generate_alert(
                        alert_config,
                        flow_id,
                        node_id,
                        recent_cnt,
                        history_cnt,
                        diff_period,
                        diff_count,
                        diff_unit,
                        diff_trend,
                        metric.tags,
                    )
        elif diff_unit == "count":
            if diff > diff_count:
                self.generate_alert(
                    alert_config,
                    flow_id,
                    node_id,
                    recent_cnt,
                    history_cnt,
                    diff_period,
                    diff_count,
                    diff_unit,
                    diff_trend,
                    metric.tags,
                )

    def get_history_cnt(self, hourtimestamp, diff_period, tags):
        data_set_id = tags.get("logical_tag")
        logging.info(
            "Start to fetch last {} hours data of the batch({})".format(
                diff_period, data_set_id
            )
        )

        start_time = time.time()
        sql = """
            SELECT SUM(data_inc) as data_total_inc from data_loss_output_total
            WHERE time >= {start_time}s AND time < {end_time}s
            AND logical_tag = '{logical_tag}' AND storage = '{storage}'
            AND module = '{module}' AND component = '{component}' AND cluster = '{cluster}'
        """.format(
            start_time=int(hourtimestamp - diff_period * 3600 + self.WINDOW_START_DIFF),
            end_time=int(hourtimestamp - diff_period * 3600 + self.WINDOW_END_DIFF),
            logical_tag=tags.get("logical_tag"),
            storage=tags.get("storage"),
            module=tags.get("module"),
            component=tags.get("component"),
            cluster=tags.get("cluster"),
        )
        results = influxdb_connections["monitor_data_metrics"].query(
            sql=sql, is_dict=True
        )
        if not results:
            logging.error(
                "Failed to fetch last {} hours data of the data_set({}), sql: {}, cost {:.3f}s".format(
                    diff_period, data_set_id, sql, time.time() - start_time
                )
            )
            return

        history_cnt = 0
        try:
            if len(results) > 0:
                history_cnt = results[0].get("data_total_inc", 0)
            logging.info(
                "Finish fetching last {} hours data of the data_set({}), cost {:.3f}s".format(
                    diff_period, data_set_id, time.time() - start_time
                )
            )
        except Exception as e:
            logging.error(
                "Failed to fetch last {} hours data of the data_set({}), sql: {}, cost {:.3f}s, ERROR: {}".format(
                    diff_period, data_set_id, sql, time.time() - start_time, str(e)
                )
            )
        return history_cnt

    def generate_alert(
        self,
        alert_config,
        flow_id,
        node_id,
        recent_count,
        history_cnt,
        diff_period,
        diff_count,
        diff_unit,
        diff_trend,
        tags,
    ):
        flow_info = self._flow_infos.get(str(flow_id), {})
        tags = tags or {}

        if diff_unit == "percent":
            trend = "上升" if diff_trend == "increase" else "下降"
            trend_en = "{diff_trend}d".format(diff_trend=diff_trend)
            diff_count = "{diff_count:.2f}%".format(
                diff_count=abs(recent_count - history_cnt) * 100.0 / history_cnt
            )
        else:
            trend = "增长" if diff_trend == "increase" else "减少"
            trend_en = "{diff_trend}d".format(diff_trend=diff_trend)
            diff_count = "{diff_count}".format(
                diff_count=abs(recent_count - history_cnt)
            )

        period_time_display, period_time_display_en = self.convert_display_time(
            diff_period * 3600
        )

        logical_tag = str(tags.get("logical_tag", ""))
        entity_display, entity_display_en = self.get_logical_tag_display(
            logical_tag, tags, flow_info
        )

        message = self.ALERT_MESSAGE.format(
            entity_display=entity_display,
            data_total_cnt=recent_count,
            last_total_cnt=history_cnt,
            diff_trend=trend,
            diff_count=diff_count,
            period_time_display=period_time_display,
        )
        message_en = self.ALERT_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            data_total_cnt=recent_count,
            last_total_cnt=history_cnt,
            diff_trend_en=trend_en,
            diff_count=diff_count,
            period_time_display_en=period_time_display_en,
        )
        full_message = self.ALERT_FULL_MESSAGE.format(
            entity_display=entity_display,
            logical_tag=logical_tag,
            data_total_cnt=recent_count,
            last_total_cnt=history_cnt,
            diff_trend=trend,
            diff_count=diff_count,
            period_time_display=period_time_display,
        )
        full_message_en = self.ALERT_FULL_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            logical_tag=logical_tag,
            data_total_cnt=recent_count,
            last_total_cnt=history_cnt,
            diff_trend_en=trend_en,
            diff_count=diff_count,
            period_time_display_en=period_time_display_en,
        )

        alert_info = {
            "time": int(time.time()),
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": message,
                "message_en": message_en,
                "full_message": full_message,
                "full_message_en": full_message_en,
                "alert_status": AlertStatus.INIT,
                "tags": {
                    "alert_level": AlertLevel.WARNING.value,
                    "alert_code": AlertCode.DATA_TREND.value,
                    "alert_type": AlertType.DATA_MONITOR.value,
                    "alert_config_id": alert_config.get("id"),
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "data_set_id": logical_tag,
                    "generate_type": alert_config.get("generate_type"),
                    "project_id": flow_info.get("project_id"),
                    "bk_app_code": flow_info.get("bk_app_code"),
                },
            },
        }

        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)

    def generate_flow_alert_configs(self):
        for alert_config in self._alert_configs:
            for target in alert_config.get("monitor_target", []):
                flow_id, node_id = self.get_flow_node_by_target(target)
                flow_id = str(flow_id)

                if (not self.check_alert_config_valid(alert_config)) or (
                    not self.check_flow_valid(flow_id)
                ):
                    self.remove_alert_config_by_flow_id(flow_id)
                    continue

                # 生成flow的指标槽位
                if flow_id not in self._flow_alert_configs:
                    self._flow_alert_configs[flow_id] = {
                        "alert_config": {},
                        "nodes": {},
                    }
                if node_id is None:
                    self._flow_alert_configs[flow_id]["alert_config"] = alert_config
                else:
                    node_id = str(node_id)
                    if node_id not in self._flow_alert_configs[flow_id]["nodes"]:
                        self._flow_alert_configs[flow_id]["nodes"][
                            node_id
                        ] = alert_config

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
        if flow_id in self._flow_alert_configs:
            del self._flow_alert_configs[flow_id]
