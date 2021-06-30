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

import arrow
from datalab.constants import DATE_TIME_FORMATTER, DEFAULT_MULTIPLICATOR
from django.utils import timezone


def list_year_month_by_time(start_time, end_time=None):
    """
    根据一段起止时间获取这段时间的年月
    :param start_time: 开始时间 2020-12-01 12:00:00
    :param end_time: 结束时间 2021-02-01 12:00:00
    :return: ["202012", "202101", "202102"]
    """

    def format_result(_year, _month):
        return (
            "{year}0{month}".format(year=_year, month=_month)
            if _month < 10
            else "{year}{month}".format(year=_year, month=_month)
        )

    start_time = arrow.get(start_time)
    end_time = arrow.get(end_time)
    if start_time > end_time:
        # 开始时间不能大于结束时间
        return []

    start_year = start_time.year
    end_year = end_time.year
    start_month = start_time.month
    end_month = end_time.month

    result = []

    for year in range(start_year, end_year + 1):
        if start_year == end_year:
            # 开始年份跟结束年份一样，取月份之间的值即可
            range_start_month = start_month
            range_end_month = end_month + 1
        else:
            if year < end_year:
                if year == start_year:
                    # 开始年份
                    range_start_month = start_month
                    range_end_month = 13
                else:
                    # 中间的年份，则补满十二个月
                    range_start_month = 1
                    range_end_month = 13
            else:
                # year == end_year 的情况
                # 轮询到结束时间的年份时，以1月份开头，直到结束月份
                range_start_month = 1
                range_end_month = end_month + 1

        for month in range(range_start_month, range_end_month):
            result.append(format_result(year, month))

    return result


def list_date_by_time(start_time, end_time=None):
    """
    根据一段起止时间获取这段时间的年月日
    :param start_time: 开始时间 2020-12-01 12:00:00
    :param end_time: 结束时间 2020-12-05 12:00:00
    :return: ["20201201", "20201202", "20201203", "20201204", "20201205"]
    """

    def format_result(_year, _month, _day):
        _month = "0{month}".format(month=_month) if _month < 10 else str(_month)
        _day = "0{day}".format(day=_day) if _day < 10 else str(_day)
        return "{year}{month}{day}".format(year=_year, month=_month, day=_day)

    start_time = arrow.get(start_time)
    end_time = arrow.get(end_time)

    result = [format_result(end_time.year, end_time.month, end_time.day)]

    day = 0
    current_date = start_time + datetime.timedelta(days=day)
    while current_date < end_time:
        current_date = start_time + datetime.timedelta(days=day)
        result.append(format_result(current_date.year, current_date.month, current_date.day))
        day += 1

    return sorted(list(set(result)), key=lambda x: x)


def timeformat_to_timestamp(any_format_time, time_multiplicator=DEFAULT_MULTIPLICATOR):
    """
    时间字符串 -> 时间戳
    :param any_format_time: 时间字符串
    :param time_multiplicator: 时间戳乘数
    :return: 时间戳
    """
    if not any_format_time:
        return None
    current_timezone = timezone.get_current_timezone()
    return arrow.get(any_format_time).replace(tzinfo=str(current_timezone)).timestamp * time_multiplicator


def timestamp_to_timeformat(timestamp, time_multiplicator=DEFAULT_MULTIPLICATOR):
    """
    时间戳 -> 时间字符串
    :param timestamp: 时间戳
    :param time_multiplicator: 时间戳乘数
    :return: 时间字符串
    """
    timestamp = int(timestamp / time_multiplicator)
    tzformat = datetime.datetime.fromtimestamp(timestamp, tz=timezone.get_current_timezone()).strftime(
        DATE_TIME_FORMATTER
    )
    return tzformat


def strftime_local(aware_time, fmt=DATE_TIME_FORMATTER):
    """
    格式化aware_time为本地时间
    """
    if not aware_time:
        # 当时间字段允许为NULL时，直接返回None
        return None
    if timezone.is_aware(aware_time):
        # translate to time in local timezone
        aware_time = timezone.localtime(aware_time)
    return aware_time.strftime(fmt)
