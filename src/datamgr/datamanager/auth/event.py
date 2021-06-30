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
from __future__ import absolute_import, print_function, unicode_literals

import json
import logging

import attr

logger = logging.getLogger(__name__)


@attr.s
class Event(object):
    change_time = attr.ib(type=str)


@attr.s
class DataProcessingRelationEvent(Event):
    id = attr.ib(type=str, converter=str)
    data_directing = attr.ib(type=str, default="")
    data_set_type = attr.ib(type=str, default="")
    data_set_id = attr.ib(type=str, default="")
    processing_id = attr.ib(type=str, default="")


@attr.s
class CreateDataProcessingRelationEvent(DataProcessingRelationEvent):
    event_name = attr.ib(
        type=str, default="create_data_processing_relation", init=False
    )


@attr.s
class DeleteDataProcessingRelationEvent(DataProcessingRelationEvent):
    event_name = attr.ib(
        type=str, default="delete_data_processing_relation", init=False
    )


@attr.s
class BaseManagersEvent(Event):
    pass


@attr.s
class UpdateManagersEvent(BaseManagersEvent):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="update_managers", init=False)


@attr.s
class AddManagersEvent(BaseManagersEvent):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="add_managers", init=False)


@attr.s
class DeleteManagersEvent(BaseManagersEvent):
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    managers = attr.ib(type=list)
    event_name = attr.ib(type=str, default="delete_managers", init=False)


def get_event_name(event):
    for f in attr.fields(event):
        if f.name == "event_name":
            return f.default
    return ""


class EventController(object):

    EVENTS = [
        CreateDataProcessingRelationEvent,
        DeleteDataProcessingRelationEvent,
        UpdateManagersEvent,
        AddManagersEvent,
        DeleteManagersEvent,
    ]

    EVENT_NAMES = [get_event_name(event) for event in EVENTS]
    EVENT_MAP = {get_event_name(event): event for event in EVENTS}
    EVENT_QUEUE_NAEM = "auth.datamanger_events"

    def __init__(self, redis_connection):
        self._redis_connection = redis_connection

    def push_event(self, event):
        self._redis_connection.lpush(
            self.EVENT_QUEUE_NAEM, json.dumps(attr.asdict(event))
        )

    def pop_event(self):
        content = self._redis_connection.rpop(self.EVENT_QUEUE_NAEM)
        if content is None:
            return None

        try:
            content = json.loads(content)
            event_name = content["event_name"]
            del content["event_name"]
            if event_name in self.EVENT_NAMES:
                return self.EVENT_MAP[event_name](**content)
        except Exception as err:
            logger.exception("Invalid event: {}, {}".format(content, err))

        return None
