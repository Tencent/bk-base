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
import time

import arrow
from dateutil import tz


def timestamp_to_arrow(timestamp, timezone):
    """
    Switch timestamp to arrow object with timezone.

    @param {Int} timestamp
    @param {String} timezone Timezone representation, such as 'Asia/Shanghai'
    """
    return arrow.get(timestamp).astimezone(tz.gettz(timezone))


def datetime_to_str(d):
    """
    将datetime转化为str
    :param d: Datetime
    :return: 时间字符串
    :rtype str
    """
    return d.strftime("%Y-%m-%d %H:%M:%S")


def strtotime(string, format="%Y-%m-%d %H:%M:%S"):
    """把时间字符串转换成时间戳

    :param str:
    :param format: 格式
    """
    timestamp = time.mktime(time.strptime(string, format))
    return timestamp


def timetostr(timestamp, format="%Y-%m-%d %H:%M:%S"):
    """把时间戳转换成时间字符串

    :param timestamp: 时间戳
    :param format: 格式
    """
    timestr = time.strftime(format, time.localtime(timestamp))
    return timestr


def floor_minute(timestamp):
    """把时间戳对齐到整分钟

    :param timestamp: 时间戳
    """
    return int(timestamp) // 60 * 60


def floor_hour(timestamp):
    """把时间戳对齐到整小时

    :param timestamp: 时间戳
    """
    return int(timestamp) // 3600 * 3600
