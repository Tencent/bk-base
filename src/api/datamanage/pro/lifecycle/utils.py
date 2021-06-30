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
import arrow
import pytz
import re

from django.utils import timezone

# INFLUXDB时间戳乘数
INFLUXDB_MULTIPLICATOR = 1000000000
# 一天时间
DAY = 24 * 60 * 60
# 默认时间戳乘数
DEFAULT_MULTIPLICATOR = 1
SHOW_TZ = False
FMT_LENGTH = None if SHOW_TZ else 16


def transpose(matrix):
    """
    不定长矩阵转置
    :param matrix: [['z', 'r', 'p'], ['z', 'x', 's', 'a']]
    :return:[['z', 'z'], ['r', 'x'], ['p', 's'], ['a']]
    """
    maxLineLen = 0
    tLine = []
    tMatrix = []
    # compute max length in lines
    for i in matrix:
        if maxLineLen < len(i):
            maxLineLen = len(i)
    # transpose matrix
    for i in range(0, maxLineLen):
        for j in matrix:
            # prevent out of index
            if i <= len(j) - 1:
                tLine.append(j[i])
        tMatrix.append(tLine[:])
        del tLine[:]
    return tMatrix


def get_es_indexes(day=7):
    # 获取es查询的indexes
    now = datetime.datetime.now()
    index_time = now - datetime.timedelta(days=day)
    while index_time <= now:
        yield '{}-{}'.format("bkdata_dataqueryapi_log", index_time.strftime('%Y.%m.%d'))
        index_time += datetime.timedelta(days=1)


def get_duration_timestamp(day=7):
    """
    获取当前时间和7天前的时间戳
    :param day:
    :return:
    """
    # 当前时间对应的时间戳
    end = int(round(time.time() * 1000))
    # 7天前的时间戳
    start = end - day * 24 * 60 * 60 * 1000
    return start, end


def get_timestamp(day=7):
    """
    获取n天前0点的时间戳
    :param day:
    :return:
    """
    # 获取当前的时间戳
    now_timestamp = int(time.time())
    # 获取7天前的时间戳
    past_timestamp = now_timestamp - (now_timestamp - time.timezone) % 86400 - day * 24 * 3600
    return past_timestamp


def get_last_timestamp():
    """
    获取当天23：59：59的时间戳
    :return:
    """
    # 获取明天0点的时间戳
    future_timestamp = get_timestamp(-1)
    # 明天0点的时间戳-1
    last_timestamp = future_timestamp - 1
    return last_timestamp


def generate_influxdb_time_range(start_timestamp, end_timestamp):
    """
    生成influxdb需要的时间段
    """
    if end_timestamp > time.time():
        end_timestamp = time.time()
    if start_timestamp < time.time() - DAY * 30:
        start_timestamp = time.time() - DAY * 30
    return int(start_timestamp) * INFLUXDB_MULTIPLICATOR, int(end_timestamp) * INFLUXDB_MULTIPLICATOR


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
    # 将时间戳转化为时间
    timestamp = int(timestamp / time_multiplicator)
    timestamp = time.localtime(timestamp)
    timeformat = time.strftime('%Y-%m-%d %H:%M:%S', timestamp)
    return timeformat


def time_format(l_time, is_tz=False):
    """
    把时间戳列表根据时间间隔转为转为可读的时间格式
    :param {datetime} l_time 时间戳列表
    :param {Boolean} is_tz 是否显示时区
    """
    if l_time:
        difference = l_time[-1] - l_time[0]
        count = len(l_time)
        if count > 1:
            frequency = int(difference / (count - 1))
            if difference < DAY and frequency < DAY:
                start = 11
                end = None if is_tz else FMT_LENGTH
            elif frequency < DAY <= difference:
                start = 5
                # end = None if is_tz else FMT_LENGTH
                end = 10
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


def get_active_timezone_offset():
    """
    获取当前用户时区偏移量
    """
    tz = str(timezone.get_current_timezone())
    offset = datetime.datetime.now(pytz.timezone(tz)).strftime('%z')
    return offset


def contain_zh(word):
    """
    判断传入字符串是否包含中文
    :param word: 待判断字符串
    :return: True:包含中文  False:不包含中文
    :param word:
    :return:
    """
    zh_pattern = re.compile('[\\u4e00-\\u9fa5]+')
    match = zh_pattern.search(word)

    return match


def get_date(day=8):
    """
    获取n天前的日期，格式：'20200115'
    :return:
    """
    date = ''.join(str(datetime.date.today() + datetime.timedelta(-day)).split('-'))
    return date
