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

import dataflow.batch.settings as settings
from dataflow.batch.exceptions.comp_execptions import BatchTimeCompareError, BatchUnsupportedOperationError


class BatchTimeTuple(object):
    def __init__(self, month=0, week=0, day=0, hour=0):
        self.month = month
        self.week = week
        self.day = day
        self.hour = hour

    def from_jobnavi_format(self, string_time_format):
        array_split_with_plus = string_time_format.split("+")
        for item0 in array_split_with_plus:
            array_split_with_minus = item0.split("-")
            minus_array_count = 0
            for item1 in array_split_with_minus:
                if minus_array_count == 0 and item1 == "":
                    minus_array_count = minus_array_count + 1
                    continue
                elif minus_array_count == 0 and item1 != "":
                    self.__set_single_jobnavi_format(item1, False)
                else:
                    self.__set_single_jobnavi_format(item1, True)
                minus_array_count = minus_array_count + 1

    def __set_single_jobnavi_format(self, time_str, is_minus):
        if str(time_str).upper().endswith("M"):
            time_value = int(time_str[:-1])
            self.month = time_value if not is_minus else -time_value
        elif str(time_str).upper().endswith("W"):
            time_value = int(time_str[:-1])
            self.week = time_value if not is_minus else -time_value
        elif str(time_str).upper().endswith("D"):
            time_value = int(time_str[:-1])
            self.day = time_value if not is_minus else -time_value
        elif str(time_str).upper().endswith("H"):
            time_value = int(time_str[:-1])
            self.hour = time_value if not is_minus else -time_value
        else:
            raise BatchUnsupportedOperationError("Found unsupported jobnavi time format")

    def set_time_with_unit(self, time_value, time_unit):
        if time_unit.lower() == "hour":
            self.hour = int(time_value)
        elif time_unit.lower() == "day":
            self.day = int(time_value)
        elif time_unit.lower() == "week":
            self.week = int(time_value)
        elif time_unit.lower() == "month":
            self.month = int(time_value)
        else:
            raise BatchUnsupportedOperationError("Found time unit {}".format(time_unit))

    def __add__(self, other):
        if isinstance(other, BatchTimeTuple):
            tmp_time_tuple = BatchTimeTuple()
            tmp_time_tuple.month = self.month + other.month
            tmp_time_tuple.week = self.week + other.week
            tmp_time_tuple.day = self.day + other.day
            tmp_time_tuple.hour = self.hour + other.hour
            return tmp_time_tuple
        else:
            raise BatchUnsupportedOperationError("Only support BatchTimeTuple plus/minus BatchTimeTuple")

    def __sub__(self, other):
        if isinstance(other, BatchTimeTuple):
            tmp_time_tuple = BatchTimeTuple()
            tmp_time_tuple.month = self.month - other.month
            tmp_time_tuple.week = self.week - other.week
            tmp_time_tuple.day = self.day - other.day
            tmp_time_tuple.hour = self.hour - other.hour
            return tmp_time_tuple
        else:
            raise BatchUnsupportedOperationError("Only support BatchTimeTuple plus/minus BatchTimeTuple")

    def __eq__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.month == other.month and self.get_hour_except_month() == other.get_hour_except_month():
                return True
        return False

    def __ne__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.month == other.month and self.get_hour_except_month() == other.get_hour_except_month():
                return False
        return True

    def __gt__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.get_hour_except_month() >= other.get_hour_except_month() and self.month > other.month:
                return True

            if self.month >= other.month and self.get_hour_except_month() > other.get_hour_except_month():
                return True

            if self.get_min_hour_count() > other.get_max_hour_count():
                return True

            if self.get_hour_except_month() <= other.get_hour_except_month() and self.month <= other.month:
                return False

            if self.get_max_hour_count() <= other.get_min_hour_count():
                return False
        raise BatchTimeCompareError("Current value doesn't support compare")

    def __lt__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.get_hour_except_month() <= other.get_hour_except_month() and self.month < other.month:
                return True

            if self.month <= other.month and self.get_hour_except_month() < other.get_hour_except_month():
                return True

            if self.get_max_hour_count() < other.get_min_hour_count():
                return True

            if self.get_hour_except_month() >= other.get_hour_except_month() and self.month >= other.month:
                return False

            if self.get_min_hour_count() >= other.get_max_hour_count():
                return False
        raise BatchTimeCompareError("Current value doesn't support compare")

    def __ge__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.get_hour_except_month() >= other.get_hour_except_month() and self.month >= other.month:
                return True

            if self.get_min_hour_count() >= other.get_max_hour_count():
                return True

            if self.get_hour_except_month() < other.get_hour_except_month() and self.month <= other.month:
                return False

            if self.get_hour_except_month() <= other.get_hour_except_month() and self.month < other.month:
                return False

            if self.get_max_hour_count() < other.get_min_hour_count():
                return False

        raise BatchTimeCompareError("Current value doesn't support compare")

    def __le__(self, other):
        if isinstance(other, BatchTimeTuple):
            if self.get_hour_except_month() <= other.get_hour_except_month() and self.month <= other.month:
                return True

            if self.get_max_hour_count() <= other.get_min_hour_count():
                return True

            if self.get_hour_except_month() > other.get_hour_except_month() and self.month >= other.month:
                return False

            if self.get_hour_except_month() >= other.get_hour_except_month() and self.month > other.month:
                return False

            if self.get_min_hour_count() > other.get_max_hour_count():
                return False

        raise BatchTimeCompareError("Current value doesn't support compare")

    def to_jobnavi_string(self):
        to_hour = self.get_hour_except_month()
        hour_str = "{}H".format(to_hour)
        if self.month != 0:
            month_str = "{}M".format(self.month)
            if to_hour > 0:
                result_str = month_str + "+" + hour_str
                return result_str
            elif to_hour < 0:
                result_str = month_str + hour_str
                return result_str
            else:
                return month_str
        return hour_str

    def get_min_hour_count(self):
        return self.__get_min_month_day_count() * 24 + self.get_hour_except_month()

    def get_max_hour_count(self):
        return self.__get_max_month_day_count() * 24 + self.get_hour_except_month()

    def get_hour_except_month(self):
        return self.week * 7 * 24 + self.day * 24 + self.hour

    def __get_min_month_day_count(self):
        return self.month * 28

    def __get_max_month_day_count(self):
        return self.month * 31


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


def get_monday_timestamp(timestamp):
    date_time = get_datetime(timestamp)
    tz = None
    if hasattr(settings, "timezone") and settings.timezone:
        tz = pytz.timezone(settings.timezone)
    date_time = date_time - datetime.timedelta(days=date_time.weekday())
    t = datetime.datetime(date_time.year, date_time.month, date_time.day, 0, 0, 0, 0, tz)
    return time.mktime(t.timetuple())


def get_month_first_day_timestamp(timestamp):
    date_time = get_datetime(timestamp)
    tz = None
    if hasattr(settings, "timezone") and settings.timezone:
        tz = pytz.timezone(settings.timezone)
    t = datetime.datetime(date_time.year, date_time.month, 1, 0, 0, 0, 0, tz)
    return time.mktime(t.timetuple())


def get_current_hour_with_tz():
    _EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    date_time = get_datetime()
    date_time = date_time.replace(minute=0, second=0, microsecond=0)
    milliceconds_timestamp = int((date_time - _EPOCH).total_seconds() * 1000)
    return milliceconds_timestamp


def get_today_0am_timestamp_with_tz(timestamp):
    _EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    date_time = get_datetime(timestamp)
    date_time = date_time.replace(hour=0, minute=0, second=0, microsecond=0)
    milliceconds_timestamp = int((date_time - _EPOCH).total_seconds() * 1000)
    return milliceconds_timestamp


def get_monday_timestamp_with_tz(timestamp):
    _EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    date_time = get_datetime(timestamp)
    date_time = date_time - datetime.timedelta(days=date_time.weekday())
    date_time = date_time.replace(hour=0, minute=0, second=0, microsecond=0)
    milliceconds_timestamp = int((date_time - _EPOCH).total_seconds() * 1000)
    return milliceconds_timestamp


def get_month_1st_timestamp_with_tz(timestamp):
    _EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    date_time = get_datetime(timestamp)
    date_time = date_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    milliceconds_timestamp = int((date_time - _EPOCH).total_seconds() * 1000)
    return milliceconds_timestamp


def to_milliseconds_timestamp_in_hour(time_string, time_format):
    # datetime.timetuple in python2.7 won't have timezone info in it. time.mktime() will always use system timezone
    # here we manually minus _EPOCH to calculate timestamp with timezone
    _EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    tz = pytz.timezone(settings.timezone)
    datetime_obj = datetime.datetime.strptime(time_string, time_format).replace(minute=0, second=0, microsecond=0)
    milliceconds_timestamp = int((tz.localize(datetime_obj) - _EPOCH).total_seconds() * 1000)
    return milliceconds_timestamp


def timestamp():
    """
    当前时间戳
    :return:
    """
    return time.mktime(timetuple())
