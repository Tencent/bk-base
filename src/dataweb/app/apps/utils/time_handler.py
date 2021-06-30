# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import datetime
import time

import arrow
import pytz
from django.conf import settings
from django.utils import timezone
from rest_framework import serializers

# 默认时间戳乘数
DEFAULT_MULTIPLICATOR = 1
# dtEventTimeStamp时间戳乘数
DTEVENTTIMESTAMP_MULTIPLICATOR = 1000
# INFLUXDB时间戳乘数
INFLUXDB_MULTIPLICATOR = 1000000000

# 一周时间
WEEK_DELTA_TIME = 7 * 24 * 60 * 60
DAY = 86400

SHOW_TZ = False
FMT_LENGTH = None if SHOW_TZ else 16


def list_year_month_by_a_period_of_time(start_time, end_time=None):
    """
    根据一段起止时间获取这段时间的年月
    :param start_time: 开始时间 2018-12-01 12:00:00
    :param end_time: 结束时间 2019-02-01 12:00:00
    :return: ["201812", "201901", "201902"]
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


def list_date_by_a_period_of_time(start_time, end_time=None):
    """
    根据一段起止时间获取这段时间的年月日
    :param start_time: 开始时间 2019-02-01 12:00:00
    :param end_time: 结束时间 2019-02-05 12:00:00
    :return: ["20190201", "20190202", "20190203", "20190204", "20190205"]
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
    时间格式 -> 时间戳
    :param any_format_time:
    :param time_multiplicator:
    :return:
    """
    if not any_format_time:
        return None
    return arrow.get(any_format_time).timestamp * time_multiplicator


def timestamp_to_timeformat(timestamp, time_multiplicator=DEFAULT_MULTIPLICATOR):
    timestamp = int(timestamp / time_multiplicator)
    timestamp = time.localtime(timestamp)
    timeformat = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
    tzformat = api_time_local(timeformat, get_dataapi_tz())
    return tzformat


def datetime_to_timestamp(_datetime):
    return timeformat_to_timestamp(_datetime)


def timestamp_to_datetime(from_timestamp, time_multiplicator=DEFAULT_MULTIPLICATOR):
    """
    timestamp -> aware datetime
    """
    utc_tz = pytz.timezone("UTC")
    utc_dt = utc_tz.localize(datetime.datetime.utcfromtimestamp(int(from_timestamp) / time_multiplicator))
    return utc_dt


def generate_influxdb_time_range(start_timestamp, end_timestamp):
    """
    生成influxdb需要的时间段
    """
    if end_timestamp > time.time():
        end_timestamp = time.time()
    if start_timestamp < time.time() - DAY * 30:
        start_timestamp = time.time() - DAY * 30
    return int(start_timestamp) * INFLUXDB_MULTIPLICATOR, int(end_timestamp) * INFLUXDB_MULTIPLICATOR


def time_format(l_time, is_tz=False):
    """
    把时间戳列表根据时间间隔转为转为可读的时间格式
    @param {datetime} l_time 时间戳列表
    @param {Boolean} is_tz 是否显示时区
    """
    if l_time:
        difference = l_time[-1] - l_time[0]
        count = len(l_time)
        if count > 1:
            frequency = difference / (count - 1)
            if difference < DAY and frequency < DAY:
                start = 11
                end = None if is_tz else FMT_LENGTH
            elif frequency < DAY <= difference:
                start = 5
                end = None if is_tz else FMT_LENGTH
            elif difference >= DAY and frequency >= DAY:
                start = 5
                end = 10
            else:
                start = None
                end = None if is_tz else FMT_LENGTH
            formated_time = [timestamp_to_timeformat(t)[start:end] for t in l_time]
        else:
            formated_time = [timestamp_to_timeformat(l_time[0])]
    else:
        formated_time = []
    return formated_time


def format_datetime(o_datetime):
    """
    格式化日志对象展示格式

    @param {datetime} o_dateitime
    """
    return o_datetime.strftime("%Y-%m-%d %H:%M:%S%z")


def get_dataapi_tz():
    """
    获取当前dataapi系统的时区
    """
    return settings.DATAAPI_TIME_ZONE


def get_delta_time():
    """
    获取app时间与dataapi时间差
    """
    sys_offset = datetime.datetime.now(pytz.timezone(settings.TIME_ZONE)).strftime("%z")
    dataapi_offset = datetime.datetime.now(pytz.timezone(settings.DATAAPI_TIME_ZONE)).strftime("%z")
    return (int(dataapi_offset) - int(sys_offset)) / 100 * 3600


def get_pizza_timestamp():
    return time.time() + get_delta_time()


def get_active_timezone_offset():
    """
    获取当前用户时区偏移量
    """
    tz = str(timezone.get_current_timezone())
    offset = datetime.datetime.now(pytz.timezone(tz)).strftime("%z")
    return offset


def strftime_local(aware_time, fmt="%Y-%m-%d %H:%M:%S"):
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


def api_time_local(s_time, from_zone=settings.DATAAPI_TIME_ZONE, fmt="%Y-%m-%d %H:%M:%S"):
    """
    将时间字符串根据源时区转为用户时区
    """
    if s_time is None:
        return None
    s_time = datetime.datetime.strptime(s_time, fmt)
    local = pytz.timezone(from_zone)
    s_time = local.localize(s_time)
    return strftime_local(s_time)


def localtime_to_timezone(d_time, to_zone):
    """
    将时间字符串根据源时区转为用户时区
    @param {datetime} d_time 时间
    @param {String} to_zone 时区
    """
    zone = pytz.timezone(to_zone)
    return d_time.astimezone(zone)


class SelfDRFDateTimeField(serializers.DateTimeField):
    def to_representation(self, value):
        if not value:
            return None
        return strftime_local(value)


class AfterRequest(object):
    def __init__(self, time_fields=None, time_format="%Y-%m-%d %H:%M:%S"):
        if time_fields is None:
            time_fields = []
        self.time_fields = time_fields
        self.time_format = time_format
        self.from_zone = settings.DATAAPI_TIME_ZONE

    def easy_dict(self, response_result):
        data = response_result["data"]
        if data is None:
            return response_result
        for field in self.time_fields:
            try:
                data[field] = api_time_local(data[field], self.from_zone, fmt=self.time_format)
            except KeyError:
                continue
        return response_result

    def easy_list(self, response_result):
        data = response_result["data"]
        self._easy_list(data)
        return response_result

    def easy_list_from_dict_results(self, response_result):
        """
        从字典字段 results 中获取列表，再进行替换，这里需要兼容接口是否进行分页操作，主要提供给 DRF 接口使用
        """
        if type(response_result["data"]) is list:
            self._easy_list(response_result["data"])
        else:
            self._easy_list(response_result["data"]["results"])
        return response_result

    def after_list_deploy_info(self, response_result):
        """
        定制化回调 - 调用 Flow.list_deploy_info 获取最近部署历史，需要处理时间字段
        """
        self.time_fields = ["created_at"]
        self.easy_list_from_dict_results(response_result)

        if type(response_result["data"]) is list:
            data = response_result["data"]
        else:
            data = response_result["data"]["results"]

        for _d in data:
            logs = _d["logs"]
            for _l in logs:
                _l["time"] = api_time_local(_l["time"], self.from_zone, fmt=self.time_format)

        return response_result

    def after_get_latest_deploy_info(self, response_result):
        """
        定制化回调 - 调用 Flow.get_latest_deploy_info 获取最近部署历史，需要处理时间字段
        """
        self.time_fields = ["created_at"]
        self.easy_dict(response_result)

        if response_result["data"] is None:
            return response_result

        logs = response_result["data"]["logs"]
        for _l in logs:
            _l["time"] = api_time_local(_l["time"], self.from_zone, fmt=self.time_format)

        return response_result

    def _easy_list(self, data):
        """
        从定义的时间字段中，把内容替换掉
        """
        for _d in data:
            for field in self.time_fields:
                try:
                    _d[field] = api_time_local(_d[field], self.from_zone, fmt=self.time_format)
                except KeyError:
                    continue
