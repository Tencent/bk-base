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
from datetime import datetime
import json
import logging

from common.event import report_event
from common.exceptions import EventReportError

from utils.time import datetime_to_str
from dmonitor.event.event_dict import (
    EventType,
    EventLevel,
    GenerateType,
    EVENT_TIME_OUT,
)


def report_dmonitor_event(event_message):
    """
    上报监控告警事件
    :return:
    """
    event_info_dict = dict(
        event_type=EventType.DATAMONITOR.value,
        event_level=EventLevel.NORMAL.value,
        generate_type=GenerateType.REPORT.value,
        refer=EventType.DATAMONITOR.value,
        expire=EVENT_TIME_OUT,
        triggered_at=datetime_to_str(datetime.now()),
        description="dmonitor alter event report",
    )
    event_info_dict["event_message"] = json.dumps(event_message)
    try:
        ret = report_event(event_info_dict)
    except EventReportError as e:
        logging.error(
            f"report_events error, event_info:{event_info_dict}, for:{str(e)}"
        )
        return None
    return ret
