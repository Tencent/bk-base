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
import datetime
import json
import logging
import time

import gevent
from gevent import monkey

from utils.time import strtotime, timetostr

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def batch_delay_alert():
    logging.info("Start to execute batch delay monitor task")

    task_configs = {
        "task_pool_size": 100,
    }

    try:
        task = BatchDelayAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init no data alert task".format(error=e),
            exc_info=True,
        )


class BatchDelayAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 300
    BATCH_NODES_TYPES = ["offline", "tdw_batch", "tdw_jar_batch"]
    CACHE_REFRESH_INTERVAL = 300

    ALERT_CODE = AlertCode.BATCH_DELAY.value
    ALERT_LEVEL = AlertLevel.WARNING.value
    ALERT_TYPE = AlertType.TASK_MONITOR.value
    ALERT_MESSAGE = (
        "{entity_display}最近一次调度延迟超过{max_delay_time_display}，"
        "当前延迟时间为{cur_delay_time_display}"
    )
    ALERT_MESSAGE_EN = (
        "Processed Time about {entity_display_en} had been delayed more than "
        "{max_delay_time_display_en}. The current delay time is {cur_delay_time_display_en}"
    )
    ALERT_FULL_MESSAGE = (
        "{entity_display}({data_set_id})最近一次调度延迟超过{max_delay_time_display}，"
        "当前延迟时间为{cur_delay_time_display}"
    )
    ALERT_FULL_MESSAGE_EN = (
        "Processed Time about {entity_display_en}({data_set_id}) had been delayed more than "
        "{max_delay_time_display_en}. The current delay time is {cur_delay_time_display_en}"
    )

    def __init__(self, *args, **kwargs):
        """初始化生成延迟指标的任务

        :param task_configs: 缓存同步任务配置
            {
                'task_pool_size': 100
            }
        """
        configs = kwargs.pop("configs", {})

        super(BatchDelayAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        # 监控依赖数据缓存
        self._data_operations = {}
        self._data_sets = {}
        self._flow_infos = {}
        self._alert_configs = []
        self._processing_alert_configs = {}

        # 离线调度信息和调度记录
        self._batch_executions = {}
        self._batch_schedules = {}

        self._last_detect_time = now - self.DETECT_INTERVAL
        self._cache_last_refresh_time = now - self.DETECT_INTERVAL

    def refresh_metadata_cache(self, now):
        """刷新离线延迟依赖的元数据信息

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
                    self._data_operations,
                    self.fetch_data_operations_from_redis,
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._data_sets,
                    self.fetch_data_set_infos_from_redis,
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._flow_infos,
                    self.fetch_dataflow_infos_from_redis,
                    update=False,
                ),
            ]
        )

        flow_alert_configs = self.generate_flow_alert_configs()
        self.generate_processing_alert_configs(flow_alert_configs)

        self._cache_last_refresh_time = now

    def do_monitor(self, now, task_pool):
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self.fetch_batch_information()
            self.detect_batch_delay(now)
            self._last_detect_time = now

    def generate_flow_alert_configs(self):
        flow_alert_configs = {}

        for alert_config in self._alert_configs:
            for target in alert_config.get("monitor_target", []):
                flow_id, node_id = self.get_flow_node_by_target(target)
                flow_id = str(flow_id)

                if not self.check_alert_config_valid(alert_config):
                    continue

                # 生成flow的指标槽位
                if flow_id not in flow_alert_configs:
                    flow_alert_configs[flow_id] = {
                        "alert_config": {},
                        "nodes": {},
                    }
                if node_id is None:
                    flow_alert_configs[flow_id]["alert_config"] = alert_config
                else:
                    node_id = str(node_id)
                    if node_id not in flow_alert_configs[flow_id]["nodes"]:
                        flow_alert_configs[flow_id]["nodes"][node_id] = alert_config

        return flow_alert_configs

    def check_alert_config_valid(self, alert_config):
        if self.ALERT_CODE not in alert_config["monitor_config"]:
            return False

        if (
            alert_config["monitor_config"][self.ALERT_CODE].get("monitor_status", "off")
            == "off"
        ):
            return False

        return True

    def generate_processing_alert_configs(self, flow_alert_configs):
        self._processing_alert_configs = {}
        for processing_id, processing_info in self._data_operations.items():
            for output_info in processing_info.get("outputs", []):
                data_set_id = output_info.get("data_set_id")
                if data_set_id and data_set_id in self._data_sets:
                    self._data_sets[data_set_id]["processing_id"] = processing_id
                else:
                    continue

                data_set_info = self._data_sets[data_set_id]
                if data_set_info.get("processing_type") != "batch":
                    continue

                flow_id = data_set_info.get("flow_id")
                if not flow_id:
                    continue
                flow_id = str(flow_id)
                nodes = data_set_info.get("nodes", {})
                alert_config, node_id = self.get_batch_alert_config(
                    flow_id, nodes, flow_alert_configs
                )
                if not alert_config:
                    continue

                self._processing_alert_configs[processing_id] = {
                    "alert_config": alert_config,
                    "data_set": data_set_info,
                    "flow_id": int(flow_id),
                    "node_id": node_id,
                    "processing_id": processing_id,
                }

    def get_batch_alert_config(self, flow_id, nodes, flow_alert_configs):
        alert_config_info = flow_alert_configs.get(flow_id, {})
        target_node_id = None
        alert_config = alert_config_info.get("alert_config")
        for node_type, node_id in nodes.items():
            if node_type in self.BATCH_NODES_TYPES:
                if str(node_id) in alert_config_info.get("nodes", {}):
                    alert_config = alert_config_info["nodes"][str(node_id)]
                    target_node_id = node_id
        return alert_config, target_node_id

    def fetch_batch_information(self):
        processing_ids = list(self._processing_alert_configs.keys())
        gevent.joinall(
            [
                gevent.spawn(
                    self.refresh_metadata,
                    self._batch_executions,
                    self.fetch_batch_latest_executions,
                    args={"processing_ids": processing_ids},
                    update=False,
                ),
                gevent.spawn(
                    self.refresh_metadata,
                    self._batch_schedules,
                    self.fetch_batch_schedules,
                    args={"processing_ids": processing_ids},
                    update=False,
                ),
            ]
        )

    def detect_batch_delay(self, now):
        for processing_id, processing_info in self._processing_alert_configs.items():
            alert_config = processing_info.get("alert_config", {})
            if processing_id not in self._batch_schedules:
                # self._svr.logger.error('Failed to get batch schedule information about task(%s)' % processing_id)
                continue
            schedule_info = self._batch_schedules[processing_id]

            last_execution = self._batch_executions.get(processing_id, {})

            self.detect_processing(
                processing_info, schedule_info, last_execution, alert_config, now
            )

    def detect_processing(
        self, processing_info, schedule_info, last_execution, alert_config, now
    ):
        schedule_delay = alert_config["monitor_config"][self.ALERT_CODE].get(
            "schedule_delay", 3600
        )
        execute_delay = alert_config["monitor_config"][self.ALERT_CODE].get(
            "execute_delay", 300
        )

        # 等待时间
        delay_hour = int(schedule_info.get("delay", 0))

        # 获取当前最近一次调度最晚开始调度时间
        schedule_period = schedule_info.get("schedule_period")
        accumulate = schedule_info.get("accumulate")
        if accumulate:
            latest_schedule_time = self.get_latest_schedule_time_by_accum(
                schedule_info, now
            )
            period_time = 3600
        elif schedule_period == "hour":
            latest_schedule_time = self.get_latest_schedule_time_by_hour(
                schedule_info, now
            )
            period_time = int(schedule_info["count_freq"]) * 3600
        elif schedule_period == "day":
            latest_schedule_time = self.get_latest_schedule_time_by_day(
                schedule_info, now
            )
            period_time = int(schedule_info["count_freq"]) * 86400
        else:
            return

        # 去掉等待时间或得到实际最新调度所在周期的时间
        latest_schedule_time -= delay_hour * 3600

        # 获取最近一次执行耗时
        last_execution_expend_time = self.get_last_execution_expend_time(
            last_execution, now
        )

        # 获取实际最近一次执行的调度时间
        last_schedule_time = int(last_execution.get("schedule_time", 0)) / 1000

        # 如果 最近一次执行完成时间 > 最晚开始调度时间 + 调度延迟 + 执行延迟
        # 且满足 最近一次完成时间 > 最晚调度时间（不满足的话说明当前调度还没开始，获取的last_execution是上一次调度的记录）
        # 则产生告警
        max_delay_time = schedule_delay + execute_delay
        delay_time = (
            last_schedule_time
            + last_execution_expend_time
            - latest_schedule_time
            - max_delay_time
        )
        self._svr.logger.info(
            (
                "Processing: %s\n"
                "Schedule: %s\n"
                "Last_execution_status: %s\n"
                "Latest_schedule_time: %s\n"
                "Last_schedule_time: %s\n"
                "Last_execution_expend_time: %s\n"
                "Max_delay_time: %s\n"
                "Delay_time: %s"
            )
            % (
                processing_info.get("processing_id"),
                json.dumps(schedule_info),
                last_execution.get("status"),
                timetostr(latest_schedule_time),
                timetostr(last_schedule_time),
                last_execution_expend_time,
                max_delay_time,
                delay_time,
            )
        )
        if (
            delay_time > 0
            or last_execution_expend_time + last_schedule_time
            < latest_schedule_time - period_time
        ):
            self.generate_alert(processing_info, max_delay_time, delay_time)

    def get_last_execution_expend_time(self, last_execution, now):
        """
        获取最近执行时的耗时
        """
        last_start_time = strtotime(
            last_execution.get("created_at"), "%Y-%m-%d %H:%M:%S"
        )
        last_end_time = last_execution.get("updated_at")
        if last_end_time:
            last_end_time = strtotime(last_end_time, "%Y-%m-%d %H:%M:%S")
            return last_end_time - last_start_time
        else:
            # 如果最近一次调度还没有完成，假定其马上完成（如果马上完成都是延迟的，那么任务后面不管什么时候完成，也必然满足延迟的条件）
            return now - last_start_time + 0.1

    def get_latest_schedule_time_by_accum(self, schedule_info, now):
        """
        对于累加窗口的离线任务，调度周期都是1小时，调度时间是用户配置的start+1到end+1，
        比如用户配置了12至16点的累加任务，那么当天第一次调度会[13, 14)点，当天最后一次调度会在[17, 18)点执行
        另外，如果用户配置了延迟时间，也会按照正常调度时间往后延迟
        """
        nowtime = datetime.datetime.fromtimestamp(now)
        latest_start_hour = int(schedule_info.get("data_start")) + 1
        latest_end_hour = int(schedule_info.get("data_end")) + 1

        latest_schedule_time = datetime.datetime(
            year=nowtime.year, month=nowtime.month, day=nowtime.day
        )
        first_schedule_time = datetime.datetime.fromtimestamp(
            schedule_info["first_schedule_time"] / 1000
        )
        while first_schedule_time > latest_schedule_time:
            latest_schedule_time += datetime.timedelta(hours=1)
        while latest_schedule_time.hour < latest_start_hour:
            latest_schedule_time += datetime.timedelta(hours=1)
        while (
            latest_schedule_time + datetime.timedelta(hours=1) < nowtime
            and latest_schedule_time.hour < latest_end_hour
            and latest_schedule_time.hour != 0
        ):
            latest_schedule_time += datetime.timedelta(hours=1)
        return time.mktime(latest_schedule_time.timetuple())

    def get_latest_schedule_time_by_hour(self, schedule_info, now):
        """
        对于按小时为周期的任务，其每次调度时间是从0开始，步长为周期的序列
        比如周期为3小时的任务，其调度时间为0、3、6、9、12、15、18、21
        具体策略涉及随机逻辑
        举个例子：假设一个任务是00:30:00启动，如果调度系统给他随机的调度时间是00:35:00，那么任务启动当天会执行
        但是如果随机的启动时间是00:25:00，那么由于已经错过了当天的调度时间，因此会在明天才执行
        另外，用户通过高级配置也可以主动设置这个第一次启动时间
        这里first_schedule_time的值是一个由随机调度分配的或者用户主动配置得到的13位时间戳
        """
        nowtime = datetime.datetime.fromtimestamp(now)
        count_freq = int(schedule_info.get("count_freq", 0))

        # 分钟的调度安排对用户来说是无感知、且不可配置的，因此这里最晚调度时间会把分钟位抹平
        # 由监控配置中的调度延迟（schedule_delay）来保证不会因为调度较晚而误警
        # 目前schedule_delay默认配置为60分钟，且不可修改，即表示对于所有任务来说，任务每次调度都是在周期的最后一分钟执行
        # 都是可以接受的
        latest_schedule_time = datetime.datetime(
            year=nowtime.year, month=nowtime.month, day=nowtime.day
        )
        first_schedule_time = datetime.datetime.fromtimestamp(
            schedule_info["first_schedule_time"] / 1000
        )
        while first_schedule_time > latest_schedule_time:
            latest_schedule_time += datetime.timedelta(hours=count_freq)
        while latest_schedule_time + datetime.timedelta(hours=count_freq) < nowtime:
            latest_schedule_time += datetime.timedelta(hours=count_freq)
        return time.mktime(latest_schedule_time.timetuple())

    def get_latest_schedule_time_by_day(self, schedule_info, now):
        """
        对于按天为周期的任务，离线调度系统在其启动后会为其分配一个第一次开始调度的时间，
        具体策略涉及随机逻辑
        举个例子：假设一个任务是00:30:00启动，如果调度系统给他随机的调度时间是00:35:00，那么任务启动当天会执行
        但是如果随机的启动时间是00:25:00，那么由于已经错过了当天的调度时间，因此会在明天才执行
        另外，用户通过高级配置也可以主动设置这个第一次启动时间
        这里first_schedule_time的值是一个由随机调度分配的或者用户主动配置得到的13位时间戳
        """
        nowtime = datetime.datetime.fromtimestamp(now)
        delay_hour = int(schedule_info.get("delay", 0))

        # 这里跟小时周期的处理逻辑比较像，唯一不同的是，对于天的任务，实际调度系统会把天任务打散到00:00:00到00:08:00来执行
        # 然而由于这个逻辑对用户来说也是隐藏的
        # 这里我们依旧假定调度延迟最大是1小时，这1小时可以由schedule_delay的默认配置（60分钟）去抵消
        # 而超过一小时的时间都认为是执行延迟
        # 具体实际场景的例子，如果用户需要保证任务在02:00:00之前执行完，否则告警，只需要配置执行延迟1小时的阈值就可以了
        latest_schedule_time = datetime.datetime(
            year=nowtime.year, month=nowtime.month, day=nowtime.day
        )

        first_schedule_time = datetime.datetime.fromtimestamp(
            schedule_info["first_schedule_time"] / 1000
        )
        while first_schedule_time > latest_schedule_time:
            latest_schedule_time += datetime.timedelta(days=1)
        while latest_schedule_time + datetime.timedelta(days=1) < nowtime:
            latest_schedule_time += datetime.timedelta(days=1)
        latest_schedule_time -= datetime.timedelta(hours=delay_hour)
        return time.mktime(latest_schedule_time.timetuple())

    def generate_alert(self, processing_info, max_delay_time, delay_time):
        """
        ALERT_MESSAGE = (
            u'{entity_display}最近一次调度延迟超过{max_delay_time_display}，'
            u'当前延迟时间为{cur_delay_time_display}'
        )
        ALERT_MESSAGE_EN = (
            u'Processed Time about {entity_display_en} had been delayed more than '
            u'{max_delay_time_display_en}. The current delay time is {cur_delay_time_display_en}'
        )
        """
        flow_id = processing_info.get("flow_id")
        node_id = processing_info.get("node_id")
        flow_info = self._flow_infos.get(str(flow_id), {})
        data_set_id = processing_info.get("data_set", {}).get("data_set_id")
        alert_config = processing_info.get("alert_config", {})

        max_delay_time_display, max_delay_time_display_en = self.convert_display_time(
            max_delay_time
        )
        delay_time_display, delay_time_display_en = self.convert_display_time(
            delay_time
        )

        entity_display, entity_display_en = self.get_logical_tag_display(
            data_set_id, {"moduel": "batch"}, flow_info
        )

        message = self.ALERT_MESSAGE.format(
            entity_display=entity_display,
            max_delay_time_display=max_delay_time_display,
            cur_delay_time_display=delay_time_display,
        )
        message_en = self.ALERT_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            max_delay_time_display_en=max_delay_time_display_en,
            cur_delay_time_display_en=delay_time_display_en,
        )
        full_message = self.ALERT_FULL_MESSAGE.format(
            entity_display=entity_display,
            data_set_id=data_set_id,
            max_delay_time_display=max_delay_time_display,
            cur_delay_time_display=delay_time_display,
        )
        full_message_en = self.ALERT_FULL_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            data_set_id=data_set_id,
            max_delay_time_display_en=max_delay_time_display_en,
            cur_delay_time_display_en=delay_time_display_en,
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
                    "alert_level": self.ALERT_LEVEL,
                    "alert_code": self.ALERT_CODE,
                    "alert_type": self.ALERT_TYPE,
                    "alert_config_id": alert_config.get("id"),
                    "flow_id": flow_id,
                    "node_id": node_id,
                    "data_set_id": data_set_id,
                    "generate_type": alert_config.get("generate_type"),
                    "project_id": flow_info.get("project_id"),
                    "bk_app_code": flow_info.get("bk_app_code"),
                    "processing_id": processing_info.get("processing_id"),
                },
            },
        }

        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_alerts"], alert_message)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], alert_message)
