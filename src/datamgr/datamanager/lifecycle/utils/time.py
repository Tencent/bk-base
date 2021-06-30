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


def get_yesterday():
    """
    获取昨日日期，格式：'20200115'
    :return:
    """
    yesterday = "".join(str(datetime.date.today() + datetime.timedelta(-1)).split("-"))
    return yesterday


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


def get_es_indexes(day=7):
    # 获取es查询的indexes
    now = datetime.datetime.now()
    index_time = now - datetime.timedelta(days=day)
    while index_time <= now:
        yield "{}-{}.*".format(
            "bkdata_dataqueryapi_log", index_time.strftime("%Y.%m.%d")
        )
        index_time += datetime.timedelta(days=1)
