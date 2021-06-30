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

from datahub.common.const import END_DATE, LIMIT, SERIES, START_DATE
from datahub.storekit import settings, util


def get_metrics(params, measurement, condition=None):
    """
    查metrics获取数据
    :param params: 参数
    :param measurement: 表
    :param condition: 查询附加条件
    :return: 结果集
    """
    limit, start_date, end_date = parse_stat_condition_param(params)
    sql = compose_query_sql(measurement, limit, start_date, end_date, condition)
    metric = util.query_metrics(settings.STAT_DATABASE, sql)
    return metric[SERIES]


def parse_stat_condition_param(params):
    """
    解析查询统计信息的参数，返回参数元组
    :param params: 请求参数
    :return: 参数元组
    """
    limit = params.get(LIMIT, 100)
    start_date = params.get(START_DATE, int(util.get_date_by_diff(-1)))
    end_date = params.get(END_DATE, int(util.get_date_by_diff(0)))
    limit = 100 if limit < 1 else limit
    return limit, start_date, end_date


def compose_query_sql(measurement, limit, start_date, end_date, condition=None):
    """
    根据条件构建查询metric db的sql语句
    :param measurement: 表
    :param limit: 条数限制
    :param start_date: 查询起始日期
    :param end_date: 查询结束日期
    :param condition: 查询附带条件
    :return: 查询的sql语句
    """
    # 将年月日转换为时间戳，便于查询
    start = int(time.mktime(time.strptime(f"{start_date}", "%Y%m%d")))
    end = int(time.mktime(time.strptime(f"{end_date}", "%Y%m%d")))
    if condition:
        sql = (
            f"SELECT * FROM {measurement} WHERE {condition} AND time >= {start}s AND time < {end}s "
            f"ORDER BY time DESC LIMIT {limit}"
        )
    else:
        sql = f"SELECT * FROM {measurement} WHERE time >= {start}s AND time < {end}s ORDER BY time DESC LIMIT {limit}"
    return sql
