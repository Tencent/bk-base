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
import uuid
from collections import Counter

import gevent
from common.mixins.consumer_mixin import ConsumerMixin
from common.mixins.db_write_mixin import DbWriteMixin
from common.mixins.meta_cache_mixin import MetaCacheMixin
from dataquality.model.base import EventLevel, EventNotifySendStatus, EventNotifyStatus
from gevent import Greenlet, monkey

from api import datamanage_api

monkey.patch_all()

REFRESH_INTERVAL = 60
TICK_INTERVAL = 60


class TrainType(object):
    DANGER = {"code": EventLevel.LARGER.value, "interval": 60}
    WARNING = {"code": EventLevel.COMMON.value, "interval": 10 * 60}


class EventDetailContent(object):
    TITLES = {
        "zh-cn": "《数据平台数据质量事件》",
        "en": "《Data System DataQuality Event》",
    }
    BIZ_TITLES = {
        "zh-cn": "[事件关联业务]",
        "en": "[Alarm Business{plural}]",
    }
    BIZ_SAMPLES = {
        "zh-cn": "{sample_biz}等{count}个业务",
        "en": "{count} businesses such as {sample_biz}",
    }
    ETC = {"zh-cn": "等", "en": ", etc."}


class NotifierCache(object):
    pass


class EventNotifier(ConsumerMixin, DbWriteMixin, MetaCacheMixin, Greenlet):
    REFRESH_INTERVAL = 60
    TICK_INTERVAL = 60
    SAVE_EVENT_INTERVAL = 5
    SAVE_EVENT_THREADHOLD = 100
    EVENT_TRUNK_SIZE = 300

    def __init__(self, *args, **kwargs):
        configs = kwargs.pop("configs", {})

        super(EventNotifier, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))

        now = int(time.time())

        cache = NotifierCache()
        cache.station = EventStation(self)

        cache.event_notify_configs = {}
        cache.event_details = []
        cache.biz_cache = {}
        cache.last_refresh_time = now
        cache.last_tick_time = now
        cache.save_event_time = now

        self.cache = cache
        self.refresh_cache(now)

    def _run(self):
        try:
            while True:
                now = int(time.time())
                if now - self.cache.last_refresh_time > self.REFRESH_INTERVAL:
                    self.refresh_cache(now)

                if now - self.cache.last_tick_time > self.TICK_INTERVAL:
                    self.cache.station.clear_convergence_cache(now)
                    self.cache.station.tick(now)
                    self.cache.last_tick_time = now

                if (
                    len(self.cache.event_details) > self.SAVE_EVENT_THREADHOLD
                    or now - self.cache.save_event_time > self.SAVE_EVENT_INTERVAL
                ):
                    self.cache.event_details = []
                    self.cache.save_event_time = now

                event_data_trunk = self.collect_kafka_data(self.EVENT_TRUNK_SIZE)

                if len(event_data_trunk) == 0:
                    gevent.sleep(1)
                    continue

                for event_data in event_data_trunk:
                    try:
                        event_info = json.loads(event_data)
                    except Exception as e:
                        logging.error(e, exc_info=True)
                        continue
                    self.handle_event_data(event_info)
        except Exception as e:
            logging.error(e, exc_info=True)

    def handle_event_data(self, event_info):
        dimensions = event_info.get("dimensions", {})
        event_instance_id = uuid.uuid4().hex
        event_info["event_instance_id"] = event_instance_id
        event_detail = {
            "event_instance_id": event_instance_id,
            "event_id": event_info.get("event_id"),
            "event_time": event_info.get("time"),
            "event_type": event_info.get("event_type"),
            "event_sub_type": event_info.get("event_sub_type"),
            "event_level": event_info.get("event_level"),
            "event_polarity": event_info.get("event_polarity"),
            "event_detail": event_info.get("event_detail"),
            "event_status_alias": event_info.get("event_status_alias"),
            "event_origin": event_info.get("event_origin"),
            "sensitivity": event_info.get("sensitivity"),
            "platform": event_info.get("platform"),
            "generate_type": event_info.get("generate_type"),
            "data_set_id": dimensions.get("data_set_id"),
            "bk_biz_id": dimensions.get("bk_biz_id"),
            "rule_id": dimensions.get("rule_id"),
        }

        (
            event_notify_status,
            description,
            alert_converged_info,
            notify_config,
        ) = self.cache.station.add_event(event_info)
        event_detail.update(
            {
                "event_notify_status": event_notify_status,
                "receivers": ",".join(notify_config.get("receivers", [])),
                "notify_ways": ",".join(notify_config.get("notify_ways", [])),
                "alert_converged_info": json.dumps(alert_converged_info),
                "description": description,
            }
        )

        self.cache.event_details.append(event_detail)

    def refresh_cache(self, now):
        self.cache.notify_configs = (
            self.fetch_notify_configs() or self.cache.notify_configs
        )
        self.cache.biz_cache = self.fetch_biz_infos() or self.cache.biz_cache
        self.cache.event_types = self.fetch_event_types() or self.cache.event_types
        self.cache.last_refresh_time = now
        for notify_config in self.cache.notify_configs:
            if notify_config.get("active", False):
                self.cache.station.add_notify_config(notify_config)


class EventStation(object):
    def __init__(self, notifier):
        self.trains = {}
        self.convergences = {}
        self.notify_configs = {}
        self.notify_status = {}

        self._notifier = notifier
        # 记录每个alert_config与Train的反向引用， 在alert_config变更时更新train中的alert_config配置
        self.notify_configs_trains = {}

    def add_notify_config(self, notify_config):
        event_id = notify_config.get("event_id")
        self.notify_configs[event_id] = notify_config

        # 生成收敛配置统计的槽位
        if event_id not in self.convergences:
            self.convergences[event_id] = {
                "events": [],
                "last_event_time": 0,
            }

        # 首先根据配置生成列车
        notify_config_trains = []
        receivers = notify_config.get("receivers", [])
        notify_ways = notify_config.get("notify_ways", [])

        for receiver in receivers:
            for notify_way in notify_ways:
                for train_type in [TrainType.DANGER, TrainType.WARNING]:
                    train_id = self.gen_train_id(
                        event_id, receiver, notify_way, train_type["code"]
                    )
                    notify_config_trains.append(train_id)
                    # 如果当前没有此列车， 则新增
                    if train_id not in self.trains:
                        self.trains[train_id] = EventTrain(
                            train_id,
                            event_id,
                            receiver,
                            notify_way,
                            train_type,
                            station=self,
                            notifier=self._notifier,
                        )
                    self.trains[train_id].add_notify_config(notify_config)

        # 如果此alert_config之前已经生成过train了， 则把此次没有生成的train都add一遍， 来去除旧train中的alert_config
        for train_id in self.notify_configs_trains.get(event_id, []):
            if train_id in notify_config_trains:
                continue
            if train_id not in self.trains:
                continue
            self.trains[train_id].add_notify_configs(notify_config)

        self.notify_configs_trains[event_id] = notify_config_trains
        return True

    def gen_train_id(self, event_id, receiver, notify_way, event_level):
        return "&".join(
            [
                event_id,
                "receiver=%s" % receiver,
                "notify_way=%s" % notify_way,
                "event_level=%s" % event_level,
            ]
        )

    def add_event(self, event):
        event_id = event.get("event_id")
        event_instance_id = event.get("event_instance_id")
        event_time = event.get("event_time")
        event_sub_type = event.get("event_sub_type")

        if event_id:
            if event_id not in self.notify_configs:
                return (
                    EventNotifyStatus.CONVERGED,
                    "无法找到该事件通知配置",
                    {"event_id": event_id},
                    {},
                )
            notify_config = self.notify_configs[event_id]

            if event_id not in self.convergences:
                return (
                    EventNotifyStatus.CONVERGED,
                    "无法找到该通知配置的收敛信息",
                    {
                        "event_id": event_id,
                        "event_sub_type": event_sub_type,
                    },
                    notify_config,
                )

            convergence = self.convergences[event_id]

            # 判断告警是否满足触发条件
            convergence_config = self.notify_configs[event_id].get(
                "convergence_config", {}
            )
            alert_threshold = int(convergence_config.get("alert_threshold") or 1)
            duration = int(convergence_config.get("duration") or 1)
            if self.insert_and_check_trigger_count(
                convergence["events"], event, alert_threshold, duration
            ):
                return (
                    EventNotifyStatus.CONVERGED,
                    "没有达到事件通知的触发条件",
                    {
                        "event_id": event_id,
                        "event_sub_type": event_sub_type,
                        "alert_threshold": alert_threshold,
                        "duration": duration,
                    },
                    notify_config,
                )

            # 判断告警是否应该按上次告警时间进行屏蔽
            mask_time = int(convergence_config.get("mask_time") or 60) * 60
            if event_time - convergence["last_event_time"] < mask_time:
                return (
                    EventNotifyStatus.CONVERGED,
                    "事件通知在收敛时间内",
                    {
                        "event_id": event_id,
                        "event_sub_type": event_sub_type,
                        "last_event_time": convergence["last_event_time"],
                        "mask_time": mask_time,
                    },
                    notify_config,
                )

            # 初始化通知信息
            notify_ways = notify_config.get("notify_ways", [])
            receivers = notify_config.get("receivers", [])
            self.notify_status[event_instance_id] = {
                "counter": Counter(),
                "total": 0,
                "info": {},
            }
            for receiver in receivers:
                self.notify_status[event_instance_id]["info"][receiver] = {}
                for notify_way in notify_ways:
                    self.notify_status[event_instance_id]["info"][receiver][
                        notify_way
                    ] = {
                        "status": EventNotifySendStatus.INIT.value,
                    }
                    self.notify_status[event_instance_id]["total"] += 1

            for train_id in self.notify_configs_trains.get(event_id, []):
                self.trains[train_id].add_event(event)
            convergence["last_event_time"] = event_time
            return EventNotifyStatus.NOTIFYING, "", {}, notify_config
        else:
            return EventNotifyStatus.CONVERGED, "该事件缺少通知策略ID", {}, {}

    def insert_and_check_trigger_count(self, events, event, alert_threshold, duration):
        # 如果槽位中无告警，则直接插入槽位中，并根据阈值来判断告警是否需要收敛，True：需要收敛，False：告警
        event_time = int(event.get("event_time"))
        if len(events) == 0:
            events.append(event_time)
            if alert_threshold <= 1:
                return False
            else:
                return True

        # 找到当前告警所在位置，并插入告警
        index = len(events) - 1
        while index >= 0:
            if event_time > events[index]:
                events.insert(index + 1, event_time)
                break
            index -= 1

        # 根据是否达到阈值来决定是否需要收敛
        min_timestamp = events[-1] - duration * 60
        fit_event_count = 0
        for event_timestamp in events:
            if event_timestamp >= min_timestamp:
                fit_event_count += 1
        return fit_event_count < alert_threshold

    def update_notify_send_status(
        self, event_instance_id, receiver, notify_way, status, send_time
    ):
        if event_instance_id in self.notify_status:
            if receiver in self.notify_status[event_instance_id]["info"]:
                if (
                    notify_way
                    in self.notify_status[event_instance_id]["info"][receiver]
                ):
                    self.notify_status[event_instance_id]["info"][receiver][notify_way][
                        "status"
                    ] = status
                    self.notify_status[event_instance_id]["counter"].update([status])

            if (
                sum(self.notify_status[event_instance_id]["counter"].values())
                == self.notify_status[event_instance_id]["total"]
            ):
                if (
                    self.notify_status[event_instance_id]["counter"][
                        EventNotifySendStatus.SUCC.value
                    ]
                    == 0
                ):
                    status = EventNotifySendStatus.ERR.value
                elif (
                    self.notify_status[event_instance_id]["counter"][
                        EventNotifySendStatus.ERR.value
                    ]
                    == 0
                ):
                    status = EventNotifySendStatus.SUCC.value
                else:
                    status = EventNotifySendStatus.PART_ERR.value

                logging.info(
                    "Update alert({event_instance_id}) send status({status})".format(
                        event_instance_id=event_instance_id, status=status
                    )
                )
                # 更新事件通知发送状态
                sql = """
                    update dataquality_event_detail
                    set notify_status=%s, notify_send_error=%s, notify_send_time=%s
                    where event_instance_id=%s
                """
                db_obj = {
                    "notify_status": status,
                    "notify_send_error": json.dumps(
                        self.notify_status[event_instance_id]["info"]
                    ),
                    "notify_send_time": send_time,
                    "event_instance_id": event_instance_id,
                }
                update_storage_obj = {
                    "store_type": "sql",
                    "db_name": "log",
                    "table_name": "datamonitor_event_detail",
                    "sql": sql,
                    "data": [db_obj],
                    "columns": [
                        "notify_status",
                        "notify_send_error",
                        "notify_send_time",
                        "event_instance_id",
                    ],
                }
                self._notifier.push_task(update_storage_obj)
                del self.notify_status[event_instance_id]
        else:
            logging.error(
                "Event({event_instance_id}) not in send status cache".format(
                    event_instance_id=event_instance_id
                )
            )

    def clear_convergence_cache(self, now):
        for event_id, notify_info in self.convergences.items():
            convergence_config = self.notify_configs[event_id].get(
                "convergence_config", {}
            )
            duration = int(convergence_config.get("duration") or 0) * 60
            index = 0
            for event_timestamp in notify_info["events"]:
                # 增加5分钟等待时间
                if now - duration - 300 <= event_timestamp:
                    break
                index += 1
            notify_info["events"] = notify_info["events"][index:]

    def tick(self, now):
        for train in self.trains.values():
            train.tick(now)
        return True


class EventTrain(MetaCacheMixin):
    def __init__(
        self,
        train_id,
        event_id,
        receiver,
        notify_way,
        train_type=TrainType.WARNING,
        station=False,
        notifier=False,
    ):
        self.train_id = train_id
        self.event_id = event_id
        self.receiver = receiver
        self.notify_way = notify_way
        self.train_type = train_type["code"]
        self.train_interval = train_type["interval"]
        self.last_train_time = 0

        self.cells = {}
        self.cells_ids = []
        self.cell_cnt = 0
        self.notify_configs = {}
        self._station = station
        self._notifier = notifier

    def add_notify_config(self, notify_config):
        event_id = notify_config.get("event_id")

        # 判断这个监控配置是否与此列车匹配
        if str(event_id) != str(self.event_id):
            self.remove_notify_config(event_id)
            return False

        receivers = notify_config.get("receivers", [])
        if self.receiver not in receivers:
            # 如果当前列车中已有此alert_config则清除掉（alert_config update时）
            self.remove_notify_config(event_id)
            return False

        notify_ways = notify_config.get("notify_ways", [])
        if self.notify_way not in notify_ways:
            # 如果当前列车中已有此alert_config则清除掉（alert_config update时）
            self.remove_notify_config(event_id)
            return False

        self.notify_configs[event_id] = notify_config
        return True

    def remove_notify_config(self, event_id):
        if event_id in self.notify_configs:
            del self.notify_configs[event_id]
        return True

    def event_match(self, event):
        # 先看alert的级别与本列车是否符合
        event_level = event.get("alert_level", EventLevel.COMMON.value)
        if event_level != self.train_type:
            return False

        # 如果是由该策略生成的告警，默认成功match
        event_id = event.get("event_id")
        if event_id is not None:
            if event_id in self.notify_configs:
                return True
            else:
                return False

        return False

    def add_event(self, event):
        if not self.event_match(event):
            return False
        event_sub_type = event.get("event_sub_type")
        bk_biz_id = event.get("dimensions", {}).get("bk_biz_id")
        if not bk_biz_id:
            return False

        if bk_biz_id not in self.cells:
            self.cells[bk_biz_id] = {}
        if event_sub_type not in self.cells[bk_biz_id]:
            self.cells[bk_biz_id][event_sub_type] = []

        self.cells[bk_biz_id][event_sub_type].append(event)
        self.cells_ids.append(event.get("event_instance_id"))
        self.cell_cnt += 1
        return True

    def tick(self, now):
        if now - self.last_train_time < self.train_interval:
            # 没到发车时间
            logging.info(
                "[WAITING TRAIN] {} waited {} sec, interval {} sec".format(
                    self.train_id, now - self.last_train_time, self.train_interval
                )
            )
            return True
        if self.empty():
            # 空车不发
            logging.info("[EMPTY TRAIN] {} empty".format(self.train_id))
            return True
        # 更新发车时间
        self.last_train_time = now
        # 开车
        return self.send(now)

    def send(self, now):
        logging.info("Sending Train {}".format(self.train_id))
        # 根据车厢中的告警， 生成具体发出的告警信息
        event_notify_msg = self.get_event_notify_msg()
        event_notify_msg_en = self.get_event_notify_msg(language="en")
        # 发送告警
        self.send_event(
            event_notify_msg,
            event_notify_msg_en,
            self.receiver,
            self.notify_way,
            self.train_type,
            now,
        )
        # 清空车厢
        self.cells = {}
        self.cells_ids = []
        self.cell_cnt = 0
        return True

    def send_event(
        self,
        event_notify_msg,
        event_notify_msg_en,
        receiver,
        notify_way,
        event_level,
        now,
    ):
        response = datamanage_api.alerts.send(
            {
                "title": EventDetailContent.TITLES["zh-cn"],
                "title_en": EventDetailContent.TITLES["en"],
                "notify_way": notify_way,
                "receiver": receiver,
                "message": event_notify_msg,
                "message_en": event_notify_msg_en,
            }
        )

        # 所有收敛后的告警发送状态设置为success

        for event_instance_id in self.cells_ids:
            self._station.update_notify_send_status(
                event_instance_id,
                self.receiver,
                self.notify_way,
                EventNotifySendStatus.SUCC.value
                if response.is_success()
                else EventNotifySendStatus.ERR.value,
                now,
            )

        if not response.is_success():
            logging.error(
                "[Alert Send] ERROR [{}][{}]\n{}".format(
                    notify_way, receiver, event_notify_msg
                )
            )
            return False

        # 记录事件通知log
        insert_storage_obj = {
            "store_type": "insert",
            "db_name": "log",
            "table_name": "dataquality_event_notify_log",
            "data": [
                {
                    "message": event_notify_msg,
                    "message_en": event_notify_msg_en,
                    "notify_time": now,
                    "event_level": event_level,
                    "receiver": receiver,
                    "notify_way": notify_way,
                    "dimensions": json.dumps(self.cells),
                }
            ],
        }
        self._notifier.push_task(insert_storage_obj)
        return True

    def get_event_notify_msg(self, language="zh-cn"):
        bk_biz_ids = list(self.cells.keys())
        messages = []
        if len(bk_biz_ids) > 0:
            # 业务
            biz_info = self._notifier.cache.biz_cache.get(str(bk_biz_ids[0]), {})
            bk_biz_name = biz_info.get("bk_biz_name")
            biz_count = len(bk_biz_ids)

            if biz_count > 1:
                sub_msg = EventDetailContent.BIZ_SAMPLES[language].format(
                    sample_biz=bk_biz_name, count=biz_count
                )
                plural = "es"
            else:
                sub_msg = bk_biz_name
                plural = ""

            message = "{biz_title}: {sub_msg}".format(
                biz_title=EventDetailContent.BIZ_TITLES[language].format(plural=plural),
                sub_msg=sub_msg,
            )

            messages.append(message)

        event_sub_type_cnt = {}
        for bk_biz_id in self.cells.keys():
            for event_sub_type in self.cells[bk_biz_id].keys():
                if event_sub_type not in event_sub_type_cnt:
                    event_sub_type_cnt[event_sub_type] = {
                        "bizs": {},
                        "event_sample": False,
                    }
                event_sub_type_cnt[event_sub_type]["bizs"][bk_biz_id] = True
                if not event_sub_type_cnt[event_sub_type]["event_sample"]:
                    event_sub_type_cnt[event_sub_type]["event_sample"] = self.cells[
                        bk_biz_id
                    ][event_sub_type][0]

        # 任务异常
        for event_type_info in self._notifier.cache.event_types:
            event_type = event_type_info.get("event_type_name")
            if event_type in event_sub_type_cnt:
                event_sub_type_stats = event_sub_type_cnt[event_type]
                messages.append(
                    self.gen_sub_alert(
                        event_type, event_type_info, event_sub_type_stats, language
                    )
                )
        return "\n".join(messages)

    def gen_sub_alert(
        self, alert_code, event_type_info, event_sub_type_stats, language
    ):
        event_sample = event_sub_type_stats.get("event_sample")

        if event_sample is not None:
            if language == "zh-cn":
                message = event_sample.get("event_detail").replace("\\n", "\n")
                title = event_type_info.get("event_type_alias")
            else:
                message = event_sample.get("event_detail").replace("\\n", "\n")
                title = event_type_info.get("event_type_name").upper().replace("_", " ")
            return "[{title}]: \n\t{message}".format(title=title, message=message)

    def empty(self):
        return self.cell_cnt < 1
