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
from functools import lru_cache
from typing import List, Optional

import attr
from cached_property import cached_property_with_ttl

from api import cmdb_api
from collection.common.collect import BKDRawDataCollector
from collection.common.exceptions import CMDBApiResultError
from collection.conf.constants import CMDB_FIELDS_TTL, cmdb_cursor_key_template
from common.redis import connections as redis_connections

logger = logging.getLogger(__name__)

WATCH_CURSOR_TIMEOUT = 3600 * 3


@lru_cache(maxsize=None)
def get_bk_biz_ids():
    return cmdb_api.get_all_business_ids()


@attr.s
class Signal:
    is_active = attr.ib(type=bool)


class CMDBBaseCollector(BKDRawDataCollector):
    object_type = ""
    fields = []
    key_name = None

    @property
    def bk_biz_ids(self):
        return get_bk_biz_ids()

    def filter_columns(self, object_info_dict):
        """
        对于批量采集可能有指定字段以外的用户自定义字段查询到，将其归类到 customize 下
        """
        customize_info = {
            host_key: object_info_dict[host_key]
            for host_key in object_info_dict
            if host_key not in self.fields
        }
        for customize_key in customize_info.keys():
            object_info_dict.pop(customize_key)
        object_info_dict["customize"] = json.dumps(customize_info)
        return object_info_dict

    def report(self, batch_report_default=False):
        """
        进行全量采集上报并衔接增量采集上报
        """
        start_time = int(time.time())

        # 仅对无 cursor / cursor 超时进行全量上报 / 强制要求全量上报
        if not self.exist_watcher() or batch_report_default:
            self.batch_report()

        self.watch_report(start_time)

    def batch_report(self):
        pass

    def watch_report(self, start_time):
        """
        通用的单对象增量采集上报方法
        """
        watcher = CMDBResourceWatcher(
            object_type=self.object_type, fields=self.fields, key_name=self.key_name
        )
        watcher.watch_loop(start_time, self.handle_event)

    def handle_event(self, event):
        """
        由子类负责事件的处理函数

        :param event:  CMDB 同步事件
        """
        pass

    def exist_watcher(self) -> bool:
        """
        是否有激活的游标
        """
        watcher = CMDBResourceWatcher(self.object_type, self.fields)
        return bool(watcher.get_cursor_id())


class CMDBResourceWatcher:
    def __init__(self, object_type: str, fields: Optional[List] = None, key_name=None):
        self.object_type = object_type
        self.fields = fields
        self.redis_connection = redis_connections["default"]
        self.key_name = key_name

    @cached_property_with_ttl(ttl=CMDB_FIELDS_TTL)
    def cmdb_obj_fields(self):
        # 当动态获取出错时使用默认 fields 代替
        try:
            fields_list = cmdb_api.get_object_fields_list(
                bk_obj_id=self.object_type, raise_exception=True
            )
            if self.key_name and self.key_name not in fields_list:
                fields_list.append(self.key_name)
            fields_list.extend(["create_time", "last_time"])
            return fields_list
        except Exception as e:
            logger.warning(
                f"Refresh {self.object_type} fields list from cmdb failed for error {e}"
            )
            return self.fields

    def watch_loop(self, start_time: int, handle_event, signal=None, sync_fields=True):
        """
        使用 cmdb watch 接口进行增量采集

        :param start_time: 开始监听的时间点，毫秒级的时间戳
        :param handle_event: 事件处理函数
        :param signal: 外部信号对象，需要阶段性检查是否有中断标识
        """
        if signal is None:
            signal = Signal(is_active=True)
        base_watch_params = {
            "bk_event_types": ["create", "update", "delete"],
            "bk_resource": self.object_type,
        }
        cursor_id = self.get_cursor_id()
        fail_watch_count = 0
        while signal.is_active:
            base_watch_params["bk_fields"] = (
                self.cmdb_obj_fields if sync_fields else self.fields
            )
            if not cursor_id:
                watch_params = {**base_watch_params, **{"bk_start_from": start_time}}
            else:
                watch_params = {**base_watch_params, **{"bk_cursor": cursor_id}}

            response = cmdb_api.resource_watch(watch_params)
            if response.code == 0 and response.data and response.data["bk_events"]:
                events = response.data["bk_events"]

                # 当 bk_watched 为 true, 表明已经监听到了事件，bk_events 中为事件详情列表
                if response.data["bk_watched"]:
                    success_count = 0
                    for event in events:
                        try:
                            handle_event(event)
                        except Exception as err:
                            logger.exception(
                                f"[Watcher:{self.object_type}] Fail to handler event({event}), {err}"
                            )
                        else:
                            success_count += 1

                    logger.info(
                        f"[Watcher:{self.object_type}] Now process {success_count} events watched."
                    )

                cursor_id = events[-1]["bk_cursor"]
                self.set_cursor_id(cursor_id)
            else:
                logger.exception(
                    f"[Watcher:{self.object_type}] Fail to watch events, {response.response}"
                )
                fail_watch_count += 1
                if fail_watch_count > 120:
                    raise CMDBApiResultError(message=response.response)
                time.sleep(30)

    def get_cursor_id(self):
        """
        获取指定 watch 任务的 cursor, key 为 bkpub_collect_cmdb_cursor_{object_type}, value 为cursor_id
        """
        cursor_id = self.redis_connection.get(
            cmdb_cursor_key_template.format(object_type=self.object_type)
        )
        return cursor_id

    def set_cursor_id(self, cursor_id):
        """
        存入指定 watch 任务的 cursor, key 为 bkpub_collect_cmdb_cursor_{object_type}, value 为 cursor_id
        如 cursor 超时（当前配置为三小时） 将释放
        """
        self.redis_connection.setex(
            name=cmdb_cursor_key_template.format(object_type=self.object_type),
            value=cursor_id,
            time=WATCH_CURSOR_TIMEOUT,
        )
