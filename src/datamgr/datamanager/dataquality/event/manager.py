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
import logging

from sqlalchemy.sql.expression import true

from dataquality import session
from dataquality.event.factory import EventFactory
from dataquality.exceptions import DataQualityError, EventNotSupportedError
from dataquality.model.core import DataQualityEvent


class EventManager(object):
    def __init__(self):
        self._events = {}

        self.init_events()

    def init_events(self):
        event_configs = session.query(DataQualityEvent).filter(
            DataQualityEvent.active == true()
        )

        for event_config in event_configs:
            try:
                self._events[event_config.event_id] = EventFactory(event_config)
            except DataQualityError as e:
                logging.error(e, exc_info=True)

    def get_event(self, event_id):
        if event_id in self._events:
            return self._events[event_id]
        else:
            event_config = (
                session.query(DataQualityEvent)
                .filter(
                    DataQualityEvent.event_id == event_id
                    and DataQualityEvent.active == true()
                )
                .first()
            )

            if not event_config:
                raise EventNotSupportedError(message_kv={"event_id": event_id})

            self._events[event_id] = EventFactory(event_config)
            return self._events[event_id]


# class EventTypesManager(object):
#     def __init__(self):
#         self..
