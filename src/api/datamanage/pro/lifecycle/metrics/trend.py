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
import json
import math

from django.conf import settings

from common.exceptions import ValidationError

from datamanage.utils.api.meta import MetaApi
from datamanage.utils.api.access import AccessApi
from datamanage.pro.lifecycle.utils import generate_influxdb_time_range, time_format, get_active_timezone_offset
from datamanage.utils.dbtools.influx_util import influx_query_by_geog_area

RUN_MODE = getattr(settings, 'RUN_MODE', 'DEVELOP')
# INFLUXDB时间戳乘数
INFLUXDB_MULTIPLICATOR = 1000000000
LIFECYCLE_METRIC_MAP = {
    'asset_value': {'metric': 'asset_value_score', 'measurement': 'asset_value_metric'},
    'assetvalue_to_cost': {'metric': 'assetvalue_to_cost', 'measurement': 'lifecycle_metric'},
    'importance': {'metric': 'importance_score', 'measurement': 'importance_metric'},
}


def format_metric_cond_measurement(choice):
    if choice not in LIFECYCLE_METRIC_MAP:
        return None, None
    metric_condition = 'last({}) as score'.format(LIFECYCLE_METRIC_MAP[choice]['metric'])
    measurement = LIFECYCLE_METRIC_MAP[choice]['measurement']
    return metric_condition, measurement


def get_metric_cond_measurement(choice):
    if choice == 'range':
        metric_condition = (
            "mean(normlized_range_score) as score, last(proj_count) as proj_count, last(biz_count) "
            "as biz_count, last(node_count) as node_count, last(app_code_count) as app_code_count"
        )
        measurement = 'range_metric'
        return metric_condition, measurement
    if choice == 'heat':
        metric_condition = 'last(heat_score) as score, last(query_count) as query_count'
        measurement = 'heat_metrics'
        return metric_condition, measurement
    metric_condition, measurement = format_metric_cond_measurement(choice)
    return metric_condition, measurement


def list_count(choice, dataset_id, start_timestamp, end_timestamp, dataset_type='result_table', frequency='1d'):
    """
    查询生命周期指标的数据趋势
    :param choice:
    :param dataset_id:
    :param start_timestamp:
    :param end_timestamp:
    :param dataset_type:
    :param frequency:
    :return:
    """
    # 生成influxdb需要的时间段
    start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)

    group_by = "GROUP BY time({})".format(frequency) if frequency else ""

    dataset_condition = "dataset_id = '%s'" % dataset_id
    metric_condition, measurement = get_metric_cond_measurement(choice)

    sql = "select {} from {} where {} and time >= {} and  time <= {} {} fill(previous) tz('Asia/Shanghai')".format(
        metric_condition, measurement, dataset_condition, start_timestamp, end_timestamp, group_by
    )

    data_counts = influx_query_by_geog_area(sql, db='monitor_custom_metrics', is_dict=True)

    start_timestamp /= INFLUXDB_MULTIPLICATOR
    end_timestamp /= INFLUXDB_MULTIPLICATOR
    if not data_counts:
        data_counts = []
        if dataset_type == 'result_table':
            search_dict = MetaApi.result_tables.retrieve({'result_table_id': dataset_id}).data
        else:
            search_dict = AccessApi.rawdata.retrieve({'raw_data_id': int(dataset_id), 'bk_username': 'datamanage'}).data
        if not search_dict:
            raise ValidationError
        created_at = search_dict.get('created_at')

        time_array = time.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        # 转为时间戳
        time_stamp = int(time.mktime(time_array))
        if time_stamp > start_timestamp:
            # 向下取整
            start_timestamp_zero = (
                start_timestamp - start_timestamp % 86400 + time.timezone
                if (start_timestamp - time.timezone) % 86400 != 0
                else start_timestamp
            )
            null_days = int(math.floor((time_stamp - start_timestamp_zero) / float(24 * 60 * 60)))
            for i in range(null_days):
                if choice == 'heat':
                    data_count_dict = {'query_count': None, 'score': None, 'time': start_timestamp + i * 24 * 60 * 60}
                elif choice == 'range':
                    data_count_dict = {
                        'biz_count': None,
                        'node_count': None,
                        'proj_count': None,
                        'score': None,
                        'app_code_count': None,
                        'time': start_timestamp + i * 24 * 60 * 60,
                    }
                elif choice == 'asset_value' or choice == 'assetvalue_to_cost' or choice == 'importance':
                    data_count_dict = {'score': None, 'time': start_timestamp + i * 24 * 60 * 60}
                data_counts.append(data_count_dict)
            start_timestamp = time_stamp
        # end_timestamp /= INFLUXDB_MULTIPLICATOR
        start_timestamp_zero = (
            start_timestamp - start_timestamp % 86400 + time.timezone
            if (start_timestamp - time.timezone) % 86400 != 0
            else start_timestamp
        )
        days = int(math.ceil((end_timestamp - start_timestamp_zero) / float(24 * 60 * 60)))
        for i in range(days):
            if choice == 'heat':
                data_count_dict = {'query_count': 0, 'score': 0.0, 'time': start_timestamp + i * 24 * 60 * 60}
            elif choice == 'range':
                data_count_dict = {
                    'biz_count': 1,
                    'node_count': '[]',
                    'proj_count': 1,
                    'score': 13.1,
                    'app_code_count': 0,
                    'time': start_timestamp + i * 24 * 60 * 60,
                }
            elif choice == 'asset_value':
                data_count_dict = {'score': 4.37, 'time': start_timestamp + i * 24 * 60 * 60}
            elif choice == 'assetvalue_to_cost':
                data_count_dict = {'score': -1, 'time': start_timestamp + i * 24 * 60 * 60}
            elif choice == 'importance':
                data_count_dict = {'score': 0, 'time': start_timestamp + i * 24 * 60 * 60}
            data_counts.append(data_count_dict)
    if data_counts:
        # 如果最后一条数据的时间不是当天，将上一条数据补给当天
        # 计算end_timestamp当天的23:59:59点时间戳
        end_timestamp_late = (
            end_timestamp - end_timestamp % 86400 + time.timezone + 24 * 60 * 60 - 1
            if (end_timestamp - time.timezone) % 86400 != 0
            else end_timestamp
        )
        if end_timestamp_late - data_counts[-1]['time'] >= 24 * 60 * 60:
            data_counts.append(data_counts[-1])
            data_counts[-1]['time'] = data_counts[-1]['time'] + 24 * 60 * 60
    scores = [
        round(data['score'], 2) if (type(data['score']) == int or type(data['score']) == float) else data['score']
        for data in data_counts
    ]
    time_list = [data['time'] for data in data_counts]
    formated_time = time_format(time_list)
    return_data = {'score': scores, 'time': formated_time, 'timezone': get_active_timezone_offset()}
    if choice == 'range':
        proj_count = [data['proj_count'] for data in data_counts]
        biz_count = [data['biz_count'] for data in data_counts]
        app_code_count = [data['app_code_count'] for data in data_counts]
        node_count_list = [data['node_count'] for data in data_counts]
        node_count = []
        first_level_node_count_list = []
        for each_node_count in node_count_list:
            sum = 0
            first_level_node_count = 0
            if isinstance(each_node_count, str):
                for each_level in json.loads(each_node_count):
                    sum += each_level
                node_count.append(sum)
                if json.loads(each_node_count):
                    first_level_node_count = json.loads(each_node_count)[0]
                first_level_node_count_list.append(first_level_node_count)
            else:
                node_count.append(None)
                first_level_node_count_list.append(None)
        return_data['proj_count'] = proj_count
        return_data['biz_count'] = biz_count
        return_data['node_count'] = node_count
        return_data['first_level_node_count'] = first_level_node_count_list
        return_data['app_code_count'] = app_code_count
    elif choice == 'heat':
        query_count = [data['query_count'] for data in data_counts]
        return_data['query_count'] = query_count
    return return_data


def list_day_query_count(dataset_id, start_timestamp, end_timestamp):
    """
    查询热度相关指标每天查询量的数据趋势
    """
    # 生成influxdb需要的时间段
    start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)
    start_timestamp /= INFLUXDB_MULTIPLICATOR
    end_timestamp /= INFLUXDB_MULTIPLICATOR
    dataset_condition = "dataset_id = '%s'" % dataset_id
    inner_sql = (
        "select max(day_query_count) as day_query_count, statistics_timestamp from heat_related_metric where "
        "{} and statistics_timestamp >= {} and statistics_timestamp <= {} and statistics_time <> '' GROUP BY "
        "statistics_time, app_code".format(dataset_condition, start_timestamp, end_timestamp)
    )
    sql = (
        "select sum(day_query_count) as day_query_count from ({}) where statistics_time <> '' GROUP BY "
        "statistics_time".format(inner_sql)
    )

    data_counts = influx_query_by_geog_area(sql, db='monitor_custom_metrics', is_dict=True)
    if not data_counts:
        data_counts = []
    day_query_count_dict = {data['statistics_time'][5:10]: data['day_query_count'] for data in data_counts}

    return day_query_count_dict
