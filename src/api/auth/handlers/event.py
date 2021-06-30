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

import attr
from auth.utils.redis import connections


@attr.s
class Event:
    change_time = attr.ib(type=str)


@attr.s
class AddManagersEvent(Event):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="add_managers", init=False)


@attr.s
class DeleteManagersEvent(Event):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="delete_managers", init=False)


@attr.s
class UpdateManagersEvent(Event):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="update_managers", init=False)


def get_event_name(event):
    for f in attr.fields(event):
        if f.name == "event_name":
            return f.default
    return ""


class EventController:

    EVENTS = [AddManagersEvent, DeleteManagersEvent]

    EVENT_NAMES = [get_event_name(event) for event in EVENTS]
    EVENT_MAP = {get_event_name(event): event for event in EVENTS}
    EVENT_QUEUE_NAEM = "auth.datamanger_events"

    def __init__(self):
        self._redis_connection = connections["default"]

    def push_event(self, event):
        self._redis_connection.lpush(self.EVENT_QUEUE_NAEM, json.dumps(attr.asdict(event)))


class EventNamespace:
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc):
        pass
