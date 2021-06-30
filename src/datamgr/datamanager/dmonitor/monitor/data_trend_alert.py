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

from common.influxdb import influxdb_connections
from utils.time import floor_minute

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.metrics.base import DataLossOutputTotal
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def data_trend_alert():
    logging.info("Start to execute data trend monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_output_total",
            "partition": False,
            "group_id": "dmonitor_data_trend",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = DataTrendAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init data trend alert task".format(error=e),
            exc_info=True,
        )


class DataTrendAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    """
    最近M分钟数据量平均值，与N天前同一时间范围内数据量平均值相比
    阈值超过K则告警
    """

    DETECT_INTERVAL = 60
    PENDING_TIME = 900
    CACHE_REFRESH_INTERVAL = 180
    CACHE_REFRESH_ERROR_INTERVAL = 180

    WINDOW_START_DIFF = -900
    WINDOW_END_DIFF = -300
    FETCH_HISTORY_DATA_STEP = 60
    FETCH_INTERVAL = 60
    CLEAR_TIMEOUT = 1200
    BUFF_TIME = 61

    ALERT_CODE = AlertCode.DATA_TREND.value
    ALERT_LEVEL = AlertLevel.WARNING.value
    ALERT_TYPE = AlertType.DATA_MONITOR.value

    ALERT_MESSAGE = (
        "{entity_display}最近{window_time_display}数据量平均{data_total_cnt}条/分，"
        "比{period_time_display}前同比{diff_trend}{diff_count}，{period_time_display}前平均{last_total_cnt}条/分"
    )
    ALERT_MESSAGE_EN = (
        "Average data count per minute during recent {window_time_display_en} is {data_total_cnt}. "
        "{diff_trend_en} {diff_count} compared with {period_time_display_en} ago."
    )
    ALERT_FULL_MESSAGE = (
        "{entity_display}({logical_tag})最近{window_time_display}数据量平均{data_total_cnt}条/分，"
        "比{period_time_display}前同比{diff_trend}{diff_count}，{period_time_display}前平均{last_total_cnt}条/分"
    )
    ALERT_FULL_MESSAGE_EN = (
        "Average data about {entity_display_en}({logical_tag}) count per minute during recent {window_time_display_en} "
        "is {data_total_cnt}. {diff_trend_en} {diff_count} compared with {period_time_display_en} ago."
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

        super(DataTrendAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._debug_data_sets = configs.get("debug_data_sets", [])
        self._filter_modules = configs.get("filter_modules", [])
        self._metric_cache = {}
        self._flow_infos = {}
        self._alert_configs = []
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
            self.fetch_flow_infos,
            args={"with_nodes": True},
            default=copy.deepcopy(self._flow_infos),
            update=False,
        )

        self._cache_last_refresh_time = now

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
            if "data_loss_output_total" in message:
                metric = DataLossOutputTotal.from_message(message)

                # 排除掉不进行数据波动检测的模块
                module = metric.get_tag("module")
                if module in self._filter_modules:
                    return

                flow_id = str(metric.get_tag("flow_id"))
                node_id = str(metric.get_tag("node_id"))
                storage = metric.get_tag("storage")
                logical_key = self.gen_logical_key(metric.tags)
                if not flow_id:
                    return
                if not storage or storage == "None":
                    return
                if flow_id not in self._metric_cache:
                    self._metric_cache[flow_id] = {}
                if node_id not in self._metric_cache[flow_id]:
                    self._metric_cache[flow_id][node_id] = {}
                if logical_key not in self._metric_cache[flow_id][node_id]:
                    self._metric_cache[flow_id][node_id][logical_key] = {
                        "tags": metric.tags,
                        "recent": {},
                        "history": {},
                    }

                metric_time = str(floor_minute(metric.timestamp))

                if (
                    metric_time
                    not in self._metric_cache[flow_id][node_id][logical_key]["recent"]
                ):
                    self._metric_cache[flow_id][node_id][logical_key]["recent"][
                        metric_time
                    ] = 0
                self._metric_cache[flow_id][node_id][logical_key]["recent"][
                    metric_time
                ] += metric.get_metric(
                    "data_inc",
                    0,
                )
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def do_monitor(self, now, task_pool):
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self._alert_configs = self.fetch_alert_configs()

            self.collect_detecting_data(now)
            self.detect_metrics(now)
            self.clear_metrics(now)

            self._last_detect_time = now

    def collect_detecting_data(self, now):
        # 统计需要手机的历史数据对应的周期
        detecting_periods = set()
        for alert_config in self._alert_configs:
            data_trend_config = alert_config.get("monitor_config", {}).get(
                self.ALERT_CODE, {}
            )
            if data_trend_config.get("monitor_status", "off") == "on":
                period = data_trend_config.get("diff_period")
                for target in alert_config.get("monitor_target", []):
                    flow_id, node_id = self.get_flow_node_by_target(target)
                    if not flow_id:
                        continue
                    detecting_periods.add((str(flow_id), period))

        detecting_periods = list(detecting_periods)
        index, step = 0, 100
        length = len(detecting_periods)
        history_data = []
        logging.info("Start to fetch these periods(%s)" % json.dumps(detecting_periods))

        # 并发获取各个历史周期的数据量
        while index < length + step:
            tasks = []
            for flow_id, period in detecting_periods[index : min(index + step, length)]:
                tasks.append(
                    gevent.spawn(
                        self.query_history_data_by_period,
                        history_data,
                        flow_id,
                        period,
                        now,
                    )
                )
            gevent.joinall(tasks)
            index += step

        # 收集历史数据
        for data in history_data:
            flow_id = str(data.get("flow_id"))
            node_id = str(data.get("node_id") or None)
            logical_key = self.gen_logical_key(data)

            cnt = int(data.get("data_total_inc") or 0)
            timestamp = str(floor_minute(data.get("time", 0)))

            if flow_id not in self._metric_cache:
                continue
            if node_id not in self._metric_cache[flow_id]:
                continue
            if logical_key not in self._metric_cache[flow_id][node_id]:
                continue

            self._metric_cache[flow_id][node_id][logical_key]["history"][
                timestamp
            ] = cnt

    def query_history_data_by_period(self, history_data, flow_id, period, now):
        # 根据周期获取历史数据
        logging.info(
            "Start to fetch last %s hours data of the flow(%s)" % (period, flow_id)
        )
        start_time = time.time()
        sql = """
            SELECT SUM(data_inc) as data_total_inc from data_loss_output_total
            WHERE time >= {start_time}s AND time < {end_time}s AND flow_id = '{flow_id}'
            GROUP BY time(1m), flow_id, node_id, module, component, cluster, logical_tag, storage
        """.format(
            flow_id=flow_id,
            start_time=int(
                now - period * 3600 + self.WINDOW_START_DIFF - self.BUFF_TIME
            ),
            end_time=int(now - period * 3600 + self.WINDOW_END_DIFF + self.BUFF_TIME),
        )
        results = influxdb_connections["monitor_data_metrics"].query(
            sql=sql, is_dict=True
        )
        if not results:
            logging.error(
                "Failed to fetch last %s hours data of the flow(%s), sql: %s, cost %.3fs"
                % (
                    period,
                    flow_id,
                    sql,
                    time.time() - start_time,
                )
            )
            return

        try:
            history_data.extend(results)
            logging.info(
                "Finish fetching last %s hours data of the flow(%s), length: %s, cost %.3fs"
                % (
                    period,
                    flow_id,
                    len(results),
                    time.time() - start_time,
                )
            )
        except Exception as e:
            logging.error(
                "Failed to fetch last %s hours data of the flow(%s), sql: %s, cost %.3fs, ERROR: %s"
                % (
                    period,
                    flow_id,
                    sql,
                    time.time() - start_time,
                    str(e),
                )
            )

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
                        str(flow_id), str(node_id), alert_config, target, now
                    )

    def monitor_flow_metrics(self, flow_id, node_id, alert_config, target, now):
        try:
            self.check_metadata(
                self._flow_infos, self.CACHE_REFRESH_ERROR_INTERVAL, "flow_infos"
            )
        except Exception:
            self.refresh_cache()
            self.cache_time = now
            return

        if not flow_id or str(flow_id) not in self._flow_infos:
            return
        flow_info = self._flow_infos[str(flow_id)]
        if (
            flow_info.get("flow_type") == "dataflow"
            and flow_info.get("status") != "running"
        ):
            if flow_id in self._metric_cache:
                del self._metric_cache[flow_id]
            return

        if flow_id not in self._metric_cache:
            return
        if node_id not in self._metric_cache[flow_id]:
            return

        for logical_key, logical_metrics in self._metric_cache[flow_id][
            node_id
        ].items():
            # 统计最近有效时间内的数据量
            self.monitor_logical_metrics(
                flow_id,
                node_id,
                target,
                logical_key,
                logical_metrics,
                alert_config,
                now,
            )

    def monitor_logical_metrics(
        self, flow_id, node_id, target, logical_key, logical_metrics, alert_config, now
    ):
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

        recent_cnt = 0
        for timestamp, cnt in logical_metrics["recent"].items():
            if (
                float(timestamp) >= now + self.WINDOW_START_DIFF
                and float(timestamp) < now + self.WINDOW_END_DIFF
            ):
                recent_cnt += cnt

        # 统计待对比的历史时间区间的数据量
        history_cnt = 0
        for timestamp, cnt in logical_metrics["history"].items():
            history_start_time = now - diff_period * 3600 + self.WINDOW_START_DIFF
            history_end_time = now - diff_period * 3600 + self.WINDOW_END_DIFF
            if (
                float(timestamp) >= history_start_time
                and float(timestamp) < history_end_time
            ):
                history_cnt += cnt

        logging.info(
            "Logical key: %s, recent: %s, history: %s"
            % (logical_key, recent_cnt, history_cnt)
        )
        if logical_key in self._debug_data_sets:
            logging.info("Metrics: %s" % json.dumps(logical_metrics))

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
                        target,
                        flow_id,
                        node_id,
                        recent_cnt,
                        history_cnt,
                        diff_period,
                        diff_count,
                        diff_unit,
                        diff_trend,
                        logical_metrics["tags"],
                        now,
                    )
        elif diff_unit == "count":
            if diff > diff_count:
                self.generate_alert(
                    alert_config,
                    target,
                    flow_id,
                    node_id,
                    recent_cnt,
                    history_cnt,
                    diff_period,
                    diff_count,
                    diff_unit,
                    diff_trend,
                    logical_metrics["tags"],
                    now,
                )

    def generate_alert(
        self,
        alert_config,
        target,
        flow_id,
        node_id,
        recent_count,
        history_cnt,
        diff_period,
        diff_count,
        diff_unit,
        diff_trend,
        tags,
        now,
    ):
        flow_info = self._flow_infos.get(str(flow_id), {})
        tags = tags or {}

        if diff_unit == "percent":
            trend = "上升" if diff_trend == "increase" else "下降"
            trend_en = "{diff_trend}d".format(diff_trend=diff_trend)
            diff_count = "{diff_count:.2f}%".format(
                diff_count=abs(recent_count - history_cnt) * 100 / history_cnt
            )
        else:
            trend = "增长" if diff_trend == "increase" else "减少"
            trend_en = "{diff_trend}d".format(diff_trend=diff_trend)
            diff_count = "{diff_count}".format(
                diff_count=abs(recent_count - history_cnt)
            )

        window_time = self.WINDOW_END_DIFF - self.WINDOW_START_DIFF
        window_time_display, window_time_display_en = self.convert_display_time(
            window_time
        )

        period_time_display, period_time_display_en = self.convert_display_time(
            diff_period * 3600
        )

        logical_tag = str(tags.get("logical_tag", ""))
        entity_display, entity_display_en = self.get_logical_tag_display(
            logical_tag, tags, flow_info
        )

        recent_avg = recent_count * 60 // window_time
        history_avg = history_cnt * 60 // window_time

        message = self.ALERT_MESSAGE.format(
            entity_display=entity_display,
            window_time_display=window_time_display,
            data_total_cnt=recent_avg,
            last_total_cnt=history_avg,
            diff_trend=trend,
            diff_count=diff_count,
            period_time_display=period_time_display,
        )
        message_en = self.ALERT_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            window_time_display_en=window_time_display_en,
            data_total_cnt=recent_avg,
            last_total_cnt=history_avg,
            diff_trend_en=trend_en,
            diff_count=diff_count,
            period_time_display_en=period_time_display_en,
        )
        full_message = self.ALERT_FULL_MESSAGE.format(
            entity_display=entity_display,
            logical_tag=logical_tag,
            window_time_display=window_time_display,
            data_total_cnt=recent_avg,
            last_total_cnt=history_avg,
            diff_trend=trend,
            diff_count=diff_count,
            period_time_display=period_time_display,
        )
        full_message_en = self.ALERT_FULL_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            logical_tag=logical_tag,
            window_time_display_en=window_time_display_en,
            data_total_cnt=recent_avg,
            last_total_cnt=history_avg,
            diff_trend_en=trend_en,
            diff_count=diff_count,
            period_time_display_en=period_time_display_en,
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
                    "alert_level": self.ALERT_LEVEL,
                    "alert_code": self.ALERT_CODE,
                    "alert_type": self.ALERT_TYPE,
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
            alert_info["dmonitor_alerts"]["tags"].update(tags)
        elif target.get("target_type") == "rawdata":
            if flow_info:
                alert_info["dmonitor_alerts"]["tags"].update(
                    {
                        "bk_biz_id": flow_info.get("bk_biz_id"),
                        "bk_app_code": flow_info.get("bk_app_code"),
                        "raw_data_id": flow_info.get("id"),
                    }
                )
            alert_info["dmonitor_alerts"]["tags"].update(tags)

        alert_message = json.dumps(alert_info)
        self.produce_metric(
            DMONITOR_TOPICS["dmonitor_alerts"],
            alert_message,
        )
        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            alert_message,
        )

    def clear_metrics(self, now):
        for flow_id, flow_metrics in self._metric_cache.items():
            for node_id, node_metrics in flow_metrics.items():
                for logical_key, logical_metrics in node_metrics.items():
                    logical_metrics["history"] = {}
                    timeout_timestamps = []
                    for timestamp in logical_metrics["recent"].keys():
                        if now - float(timestamp) > self.CLEAR_TIMEOUT:
                            timeout_timestamps.append(timestamp)
                    for timestamp in timeout_timestamps:
                        del logical_metrics["recent"][timestamp]
