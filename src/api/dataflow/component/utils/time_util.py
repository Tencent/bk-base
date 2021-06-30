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
import time

import pytz

import dataflow.component.settings as settings


def timetuple(timestamp=None):
    """
    根据时间戳返回时间9-item信息。会根据系统配置的时区返回对应时间。
    :param timestamp: 时间戳，如果不设置，返回当前时间
    :return:
    """
    return get_datetime(timestamp).timetuple()


def get_datetime(timestamp=None):
    """
    根据时间戳返回时间9-item信息。会根据系统配置的时区返回对应时间。
    :param timestamp: 时间戳，如果不设置，返回当前时间
    :return:
    """
    if timestamp:
        if hasattr(settings, "timezone") and settings.timezone:
            tz = pytz.timezone(settings.timezone)
            return datetime.datetime.fromtimestamp(timestamp, tz)
        else:
            return datetime.datetime.fromtimestamp(timestamp)
    else:
        if hasattr(settings, "timezone") and settings.timezone:
            tz = pytz.timezone(settings.timezone)
            return datetime.datetime.now(tz)
        else:
            return datetime.datetime.now()


def timetuple_utc(timestamp=None):
    return timetuple_timezone(timestamp)


def timetuple_timezone(timestamp=None, timezone="utc"):
    tz = pytz.timezone(timezone)
    if timestamp:
        return datetime.datetime.fromtimestamp(timestamp, tz).timetuple()
    else:
        return datetime.datetime.now(tz).timetuple()


def get_today_zero_hour_timestamp(timestamp):
    date_time = get_datetime(timestamp)
    tz = None
    if hasattr(settings, "timezone") and settings.timezone:
        tz = pytz.timezone(settings.timezone)
    t = datetime.datetime(date_time.year, date_time.month, date_time.day, 0, 0, 0, 0, tz)
    return time.mktime(t.timetuple())


def timestamp():
    """
    当前时间戳
    :return:
    """
    return time.mktime(timetuple())


if __name__ == "__main__":
    print(time.time())
    print(timestamp())
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print(pytz.country_timezones("us"))
    print(time.time())
    print(time.localtime(timestamp()))
    print(time.localtime())
    print(timetuple())
