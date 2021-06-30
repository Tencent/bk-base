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

from utils.time import timetostr

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.metrics.base import DataLossOutputTotal
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def dataflow_task_alert():
    logging.info("Start to execute dataflow task monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_output_total",
            "partition": False,
            "group_id": "dmonitor_tasks",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = DataflowTaskAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init no data alert task".format(error=e),
            exc_info=True,
        )


class DataflowTaskAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 60
    PENDING_TIME = 0
    CACHE_REFRESH_INTERVAL = 180
    CACHE_REFRESH_ERROR_INTERVAL = 180

    BATCH_RECENT_TIME = 300
    BATCH_NODES_TYPES = ["offline", "tdw_batch", "tdw_jar_batch"]
    STREAM_NODES_TYPES = ["realtime"]

    ALERT_CODE = AlertCode.TASK.value
    STREAM_ALERT_MESSAGE = "{entity_display}从{time_str}开始持续{time_display}没有检测到任何执行信息"
    STREAM_ALERT_MESSAGE_EN = (
        "There is no any execution information about {entity_display_en} at "
        "{time_str} lasted for {time_display_en}."
    )
    STREAM_ALERT_FULL_MESSAGE = (
        "{entity_display}从{time_str}开始持续{time_display}没有检测到任何执行信息"
    )
    STREAM_ALERT_FULL_MESSAGE_EN = (
        "There is no any execution information about {entity_display_en} at "
        "{time_str} lasted for {time_display_en}."
    )
    BATCH_ALERT_MESSAGE = "{entity_display}于{schedule_time}的调度中执行失败, 原因: {reason}"
    BATCH_ALERT_MESSAGE_EN = (
        "Failed to execute {entity_display_en} at {schedule_time} scheduling period. "
        "Reason: {reason}"
    )
    BATCH_ALERT_FULL_MESSAGE = "{entity_display}于{schedule_time}的调度中执行失败, 原因: {reason}"
    BATCH_ALERT_FULL_MESSAGE_EN = (
        "Failed to execute {entity_display_en} at {schedule_time} scheduling period. "
        "Reason: {reason}"
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

        super(DataflowTaskAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._metric_cache = {}
        self._alert_configs = {}
        self._flow_infos = {}
        self._batch_executions = {}
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
            self.fetch_dataflow_infos_from_redis,
            update=False,
        )
        gevent.spawn(
            self.refresh_metadata,
            self._batch_executions,
            self.fetch_batch_executions_by_time,
            {"recent_time": self.BATCH_RECENT_TIME},
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
            self.check_metadata(
                self._flow_infos, self.CACHE_REFRESH_ERROR_INTERVAL, "flows"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = now

        try:
            if "data_loss_output_total" in message:
                metric = DataLossOutputTotal.from_message(message)
                flow_id = metric.get_tag("flow_id")
                node_id = str(metric.get_tag("node_id"))
                if not flow_id or flow_id not in self._flow_infos:
                    return

                if flow_id not in self._metric_cache:
                    self._metric_cache[flow_id] = {
                        "nodes": {},
                        "first_monitor_time": None,
                        "batch_executions": {},
                    }
                if node_id in self._metric_cache[flow_id]["nodes"]:
                    if (
                        metric.timestamp
                        < self._metric_cache[flow_id]["nodes"][node_id].timestamp
                    ):
                        return

                self._metric_cache[flow_id]["nodes"][node_id] = metric
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def do_monitor(self, now, task_pool):
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self._alert_configs = self.fetch_alert_configs()
            self.monitor_tasks(now)
            self._last_detect_time = now

    def monitor_tasks(self, now):
        try:
            self.check_metadata(
                self._flow_infos, self.CACHE_REFRESH_ERROR_INTERVAL, "flows"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = now

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
                        flow_id, str(node_id), alert_config, target, now
                    )

    def monitor_flow_metrics(self, flow_id, node_id, alert_config, target, now):
        # 如果flow没有运行或者flow_id不存在则跳过
        if not flow_id or flow_id not in self._flow_infos:
            return
        # logging.info(u'Begining to monitor flow({flow_id})'.format(flow_id=flow_id))
        flow_info = self._flow_infos[flow_id]

        # 记录第一次监控的时间
        if flow_id not in self._metric_cache:
            self._metric_cache[flow_id] = {
                "nodes": {},
                "first_monitor_time": now,  # 记录监控任务首次检测的时间
                "batch_executions": {},  # 记录离线rt最大的执行id
            }

        if self._metric_cache[flow_id]["first_monitor_time"] is None:
            self._metric_cache[flow_id]["first_monitor_time"] = now

        # 如果flow在running状态中超过指定时间没有任何打点出现，触发告警
        if node_id is not None and node_id != "None":
            # 如果节点没有运行或者不存在，则跳过
            if node_id not in flow_info["nodes"]:
                return
            node_info = flow_info["nodes"][node_id]
            node_type = node_info.get("node_type")
            if node_type in self.STREAM_NODES_TYPES:
                self.monitor_stream_node_metrics(
                    flow_id, node_id, alert_config, target, now
                )
            elif node_type in self.BATCH_NODES_TYPES:
                self.monitor_batch_node_metrics(
                    flow_id, node_id, alert_config, target, now
                )
        else:
            for id, node_info in flow_info["nodes"].items():
                node_type = node_info.get("node_type")
                if node_type in self.STREAM_NODES_TYPES:
                    self.monitor_stream_node_metrics(
                        flow_id, id, alert_config, target, now
                    )
                elif node_type in self.BATCH_NODES_TYPES:
                    self.monitor_batch_node_metrics(
                        flow_id, id, alert_config, target, now
                    )

    def monitor_stream_node_metrics(self, flow_id, node_id, alert_config, target, now):
        no_metrics_interval = alert_config["monitor_config"][self.ALERT_CODE].get(
            "no_metrics_interval", 600
        )

        metric = None
        if node_id not in self._metric_cache[flow_id]["nodes"]:
            # 如果没有接收到任何节点的埋点，则无指标时间以监控任务开始时间为准
            no_metrics_time = now - self._metric_cache[flow_id]["first_monitor_time"]
            metric = self._metric_cache[flow_id]["first_monitor_time"]
        else:
            metric = self._metric_cache[flow_id]["nodes"][node_id]
            no_metrics_time = now - metric.timestamp

        if no_metrics_time >= no_metrics_interval:
            self.generate_stream_alert(
                alert_config, target, flow_id, node_id, metric, no_metrics_time, now
            )

    def monitor_batch_node_metrics(self, flow_id, node_id, alert_config, target, now):
        exception_status = alert_config["monitor_config"][self.ALERT_CODE].get(
            "batch_exception_status", ["failed"]
        )
        flow_info = self._flow_infos[flow_id]

        for relation_info in flow_info["relations"].get(str(node_id), []):
            result_table_id = relation_info.get("result_table_id")

            # 设置最大exec_id为0
            if result_table_id not in self._metric_cache[flow_id]["batch_executions"]:
                self._metric_cache[flow_id]["batch_executions"][result_table_id] = 0

            last_max_exec_id = self._metric_cache[flow_id]["batch_executions"][
                result_table_id
            ]
            for execution in self._batch_executions.get(result_table_id, []):
                # 去重，防止一次执行失败重复告警
                if execution["exec_id"] > last_max_exec_id:
                    if execution["src_status"] in exception_status:
                        self.generate_batch_alert(
                            alert_config,
                            target,
                            flow_id,
                            node_id,
                            result_table_id,
                            execution,
                            now,
                        )

                # 更新rt的最大exec_id以去重
                if (
                    execution["exec_id"]
                    > self._metric_cache[flow_id]["batch_executions"][result_table_id]
                ):
                    self._metric_cache[flow_id]["batch_executions"][
                        result_table_id
                    ] = execution["exec_id"]

    def generate_stream_alert(
        self, alert_config, target, flow_id, node_id, metric, no_metrics_time, now
    ):
        """生成实时任务告警"""
        flow_info = self._flow_infos.get(flow_id, {})
        node_info = flow_info.get("nodes", {}).get(node_id, {})
        result_tables = flow_info.get("relations", {}).get(str(node_id), [])

        time_display, time_display_en = self.convert_display_time(no_metrics_time)
        if isinstance(metric, int) or isinstance(metric, float):
            time_str = timetostr(metric)
        else:
            time_str = timetostr(metric.timestamp)
        entity_display, entity_display_en = self.get_flow_node_display(
            flow_info, node_info
        )

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": self.STREAM_ALERT_MESSAGE.format(
                    entity_display=entity_display,
                    time_display=time_display,
                    time_str=time_str,
                ),
                "message_en": self.STREAM_ALERT_MESSAGE_EN.format(
                    entity_display_en=entity_display_en,
                    time_display_en=time_display_en,
                    time_str=time_str,
                ),
                "full_message": self.STREAM_ALERT_FULL_MESSAGE.format(
                    entity_display=entity_display,
                    time_display=time_display,
                    time_str=time_str,
                ),
                "full_message_en": self.STREAM_ALERT_FULL_MESSAGE_EN.format(
                    entity_display_en=entity_display_en,
                    time_display_en=time_display_en,
                    time_str=time_str,
                ),
                "alert_status": AlertStatus.INIT.value,
                "tags": {
                    "alert_level": AlertLevel.DANGER.value,
                    "alert_code": AlertCode.TASK.value,
                    "alert_type": AlertType.TASK_MONITOR.value,
                    "alert_config_id": alert_config.get("id"),
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "alert_sub_code": "no_metrics",
                    "project_id": flow_info.get("project_id"),
                    "bk_app_code": flow_info.get("bk_app_code"),
                    "generate_type": alert_config.get("generate_type"),
                },
            },
        }
        if len(result_tables) > 0:
            alert_info["dmonitor_alerts"]["tags"]["result_table_id"] = result_tables[
                0
            ].get("result_table_id")
        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)

    def generate_batch_alert(
        self, alert_config, target, flow_id, node_id, result_table_id, execution, now
    ):
        """生成离线任务告警"""
        flow_info = self._flow_infos.get(flow_id, {})
        node_info = flow_info.get("nodes", {}).get(node_id, {})

        entity_display, entity_display_en = self.get_flow_node_display(
            flow_info, node_info
        )

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": self.BATCH_ALERT_MESSAGE.format(
                    entity_display=entity_display,
                    schedule_time=execution["schedule_time"],
                    reason="{err_msg}({err_code})".format(
                        err_msg=execution["err_msg"], err_code=execution["err_code"]
                    ),
                ),
                "message_en": self.BATCH_ALERT_MESSAGE_EN.format(
                    entity_display_en=entity_display_en,
                    schedule_time=execution["schedule_time"],
                    reason="{err_msg}({err_code})".format(
                        err_msg=execution["err_msg"], err_code=execution["err_code"]
                    ),
                ),
                "full_message": self.BATCH_ALERT_FULL_MESSAGE.format(
                    entity_display=entity_display,
                    schedule_time=execution["schedule_time"],
                    reason="{err_msg}({err_code})".format(
                        err_msg=execution["err_msg"], err_code=execution["err_code"]
                    ),
                ),
                "full_message_en": self.BATCH_ALERT_FULL_MESSAGE_EN.format(
                    entity_display_en=entity_display_en,
                    schedule_time=execution["schedule_time"],
                    reason="{err_msg}({err_code})".format(
                        err_msg=execution["err_msg"], err_code=execution["err_code"]
                    ),
                ),
                "alert_status": AlertStatus.INIT.value,
                "tags": {
                    "alert_level": AlertLevel.DANGER.value,
                    "alert_code": AlertCode.TASK.value,
                    "alert_type": AlertType.TASK_MONITOR.value,
                    "alert_config_id": alert_config.get("id"),
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "alert_sub_code": "batch_exception",
                    "project_id": flow_info.get("project_id"),
                    "bk_app_code": flow_info.get("bk_app_code"),
                    "exec_id": execution.get("exec_id"),
                    "status": execution.get("src_status"),
                    "result_table_id": result_table_id,
                    "generate_type": alert_config.get("generate_type"),
                },
            },
        }
        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)
