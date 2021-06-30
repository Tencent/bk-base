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
import hashlib

from django.utils import timezone


def parse_in_dict(source, keys):
    """
    字典解析函数
    @param source:
    @param keys:
    @return:
    """
    if isinstance(keys, list):
        result = {}
        for key in keys:
            result[key] = source.get(key)
        return result
    else:
        return {keys: source.get(keys)}


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


def compute_file_md5(file_path):
    with open(file_path, "rb") as fp:
        data = fp.read()
    return hashlib.md5(data).hexdigest()


def local_dgraph_data(data, time_fields):
    """Dgraph 数据中时间字符串本地化"""
    for tf in time_fields:
        if tf in data:
            data[tf] = data[tf].replace("T", " ").replace("+08:00", "")
