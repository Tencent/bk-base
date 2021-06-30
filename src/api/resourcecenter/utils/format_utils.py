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


def covert_mb_size(size):
    """
    转换容量大小显示（默认MB）
    :param size:
    :return:
    """
    if size is None:
        return ""
    if type(size) == str:
        return size
    gb = 1024
    tb = gb * 1024
    pb = tb * 1024
    eb = pb * 1024
    zb = eb * 1024

    if size >= zb:
        return "%.1f ZB" % float(size / zb)
    if size >= eb:
        return "%.1f EB" % float(size / eb)
    if size >= pb:
        return "%.1f PB" % float(size / pb)
    if size >= tb:
        return "%.1f TB" % float(size / tb)
    if size >= gb:
        return "%.1f GB" % float(size / gb)
    if size >= 0:
        return "%d MB" % float(size)


def covert_cpu_size(size):
    """
    转换CPU核心单位显示
    :param size:
    :return:
    """
    if size is None:
        return ""
    if type(size) == str:
        return size

    if size >= 1000000:
        return "%.1fM Core" % float(size / 1000000)
    if size >= 1000:
        return "%.1fK Core" % float(size / 1000)
    return "%.1f Core" % float(size)


def covert_number_size(size):
    """
    转换数值单位显示
    :param size:
    :return:
    """
    if size is None:
        return ""
    if type(size) == str:
        return size

    if size >= 1000000:
        return "%.1fM" % float(size / 1000000)
    if size >= 1000:
        return "%.1fK" % float(size / 1000)
    return "%.1f" % float(size)


def get_history_last_number_val(history, field_name):
    """
    获取最后一组的字段的数值
    :param history:
    :param field_name:
    :return:
    """
    if history and len(history) >= 1:
        return history[-1].get(field_name, 0)
    return 0
