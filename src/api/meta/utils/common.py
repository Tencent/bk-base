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


import re
import sys

import six

time_unit_standard_tdw_map = {"m": "I", "H": "H", "d": "D", "w": "W", "M": "M", "O": "O", "R": "R"}

time_unit_tdw_standard_map = {v: k for k, v in time_unit_standard_tdw_map.items()}
first_cap_re = re.compile("(.)([A-Z][a-z]+)")
all_cap_re = re.compile("([a-z0-9])([A-Z])")


def map_dict_attrs(
    orgi,
    new,
    rules,
):
    """
    复制不同obj 属性 dict。
    :param orgi: 老
    :param new: 新
    :param rules: 统一属性，或（老属性，新属性）
    :return:
    """
    for rule in rules:
        if isinstance(rule, (list, tuple)):
            orgi[rule[0]] = new[rule[1]] = orgi[rule[0]]
        else:
            orgi[rule] = new[rule] = orgi[rule]
    return orgi, new


def camel_to_snake(name):
    """将驼峰式命名变更为下划线小写命名。"""
    s1 = first_cap_re.sub(r"\1_\2", name)
    return all_cap_re.sub(r"\1_\2", s1).lower()


class ObjectProxy(object):
    def __init__(self, proxied_obj):
        self._proxied = proxied_obj

    def __getattribute__(self, attr):
        """
        If an attribute does not exist on this instance, then we also attempt
        to proxy it to the underlying HttpRequest object.
        """
        try:
            return super(ObjectProxy, self).__getattribute__(attr)
        except AttributeError:
            info = sys.exc_info()
            try:
                return getattr(self._proxied, attr)
            except AttributeError:
                six.reraise(info[0], info[1], info[2].tb_next)


def to_pythonic_name(data):
    """
    把java风格数据中的驼峰key转为python风格小写下划线
    @return:
    """
    if isinstance(data, dict):
        for key, value in list(data.items()):
            data[camel_to_snake(key)] = data.pop(key)
            _next = data[camel_to_snake(key)]
            if isinstance(_next, dict):
                for _key, _value in list(_next.items()):
                    to_pythonic_name(_value)
            elif isinstance(_next, list):
                for _data in _next:
                    to_pythonic_name(_data)
    elif isinstance(data, list):
        for _data in data:
            to_pythonic_name(_data)
    return


def paged_sql(sql, page=None, page_size=None):
    """
    sql 分页处理

    :param sql: sql 原文
    :param page: 分页页数
    :param page_size: 分页size
    :return: 分页执行的sql
    """

    sql = str(sql)
    if all([page is not None, page_size is not None, sql.lower().find(" limit ") == -1]):
        page = int(page)
        page_size = int(page_size)
        limit_clause = (
            " limit " + str((page - 1) * page_size) + "," + str(page_size) if all([page >= 0, page_size >= 0]) else ""
        )
        sql += limit_clause
    return sql
