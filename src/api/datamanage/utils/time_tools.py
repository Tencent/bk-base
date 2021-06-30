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
import pytz
import datetime

from django.conf import settings
from common.log import logger


def strtotime(str, format="%Y-%m-%d %H:%M:%S"):
    timestamp = time.mktime(time.strptime(str, format))
    return timestamp


def timetostr(timestamp, format="%Y-%m-%d %H:%M:%S"):
    if isinstance(timestamp, datetime.datetime):
        return timestamp.strftime(format)

    try:
        timestamp = str(timestamp)
        if len(timestamp) >= 10 and len(timestamp) < 13:
            timestamp = int(timestamp)
        elif len(timestamp) >= 13:
            timestamp = int(timestamp) // 1000

        if settings.USE_TZ:
            dt = datetime.datetime.fromtimestamp(timestamp, tz=pytz.timezone(settings.TIME_ZONE))
        else:
            dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime(format)
    except Exception as e:
        logger.error('Convert "%s" to string format error: %s' % (timestamp, e))
        return timestamp


def tznow():
    if settings.USE_TZ:
        return datetime.datetime.now(tz=pytz.timezone(settings.TIME_ZONE))
    else:
        return datetime.datetime.now()
