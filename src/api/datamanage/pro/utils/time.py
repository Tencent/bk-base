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

from django.utils import timezone

from common.log import logger


def get_date(day=1, format_str=''):
    """
    获取昨日日期，格式：'20200115'
    :return:
    """
    date = format_str.join(str(datetime.date.today() + datetime.timedelta(-day)).split('-'))
    return date


def utc_to_local(time_str):
    """
    utc时间转化为当地时间
    :param time_str: 各种各式的utc时间字符串
    :return: res {String} 当时时间字符串，例如"%Y-%m-%d %H:%M:%S"
    """
    try:
        res = arrow.get(time_str).to(str(timezone.get_current_timezone())).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning('time_str: [%s] convert utc time to local time error: [%s]' % (time_str, str(e)))
        res = time_str
    return res


def str_to_datetime(time_str, fmt='%Y-%m-%d %H:%M:%S'):
    """
    将时间str转化为datetime
    :param time_str: {String} 时间字符串
    :param fmt: {String} 时间str格式
    :return: {Datetime} datetime
    """
    return datetime.datetime.strptime(time_str, fmt)
