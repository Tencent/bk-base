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

from django.conf import settings
from resourcecenter.dataquery.data_helper import DataQueryHelper

window_length = settings.PROCESSING_METRIC_CALCULATION_WINDOW_LENGTH
rt_name = settings.PROCESSING_METRIC_DATA_RT
rt_storage_type = settings.RESOURCE_CENTER_METRIC_STORAGE_TYPE


def get_resource_group_summary(params):
    """
    查询集群组的计算容量总容量(内存和CPU）
    :param params:
    :return:
    """
    _param_time_minus_window_length(params)
    query_sql = """
    SELECT sum(total_memory) as total_memory,
           sum(total_vcores) as total_vcores
    FROM (
        SELECT max(total_memory) as total_memory,
               max(total_vcores) as total_vcores,
               resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               cluster_domain,
               queue_name
        FROM {rt}
        WHERE dtEventTime>='{start_time}' AND dtEventTime<='{end_time}'
        AND resource_group_id='{resource_group_id}'
        AND if ('{geog_area_code}' == '', 1=1, geog_area_code='{geog_area_code}')
        GROUP BY resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               cluster_domain,
               queue_name
    ) t
    """.format(
        rt=rt_name, **params
    )
    data = DataQueryHelper.query_data(sql=query_sql, prefer_storage=rt_storage_type)
    result = {}
    if data:
        if data.get("list", None):
            _list = data.get("list", [])
            if len(_list) > 0:
                result["memory"] = _list[0].get("total_memory", 0)
                result["cpu"] = _list[0].get("total_vcores", 0)
    return result


def get_resource_group_cpu_history(params):
    """
    查询集群组cpu历史
    按5分钟峰值（max）进行采样，然后按时间单位取平均值（avg），然后按资源组汇总。
    :param params:
    :return:
    """
    _param_time_minus_window_length(params)
    _parser_time_unit(params)
    params["window_length"] = window_length
    query_sql = """
    SELECT sum(total) as total,
           sum(used) as used,
           sum(total) - sum(used) as available,
           resource_group_id,
           time
    FROM
      (SELECT avg(total) as total,
              avg(used) as used,
              avg(total) - avg(used) as available,
              resource_group_id,
              geog_area_code,
              service_type,
              cluster_id,
              cluster_type,
              cluster_domain,
              queue_name,
              FROM_UNIXTIME((floor(time_5min/{time_unit_seconds}) * {time_unit_seconds}), '{time_fm}') as time
       FROM
         (SELECT ifnull(max(total_vcores), 0) as total,
                 ifnull(max(used_vcores), 0) as used,
                 ifnull(max(total_vcores), 0) - ifnull(max(used_vcores), 0) as available,
                 resource_group_id,
                 geog_area_code,
                 service_type,
                 cluster_id,
                 cluster_type,
                 cluster_domain,
                 queue_name,
                 (floor(dtEventTimeStamp/1000/300) * 300 + {window_length}) as time_5min
          FROM {rt}
          WHERE dtEventTime>='{start_time}'
            AND dtEventTime<='{end_time}'
            AND resource_type='processing'
            AND resource_group_id='{resource_group_id}'
            AND geog_area_code='{geog_area_code}'
            AND service_type='{service_type}'
            AND if ('{cluster_id}' == 'all',
                    1=1,
                    cluster_id='{cluster_id}')
          GROUP BY resource_group_id,
                   geog_area_code,
                   service_type,
                   cluster_id,
                   cluster_type,
                   cluster_domain,
                   queue_name,
                   time_5min) t1
       GROUP BY resource_group_id,
                geog_area_code,
                service_type,
                cluster_id,
                cluster_type,
                cluster_domain,
                queue_name,
                time) t
    group by resource_group_id,
             time
    order by time limit 5000
    """.format(
        rt=rt_name, **params
    )
    data = DataQueryHelper.query_data(sql=query_sql, prefer_storage=rt_storage_type)
    empty_result = []
    if data:
        if data.get("list", None):
            _list = data.get("list", [])
            return _list
    return empty_result


def get_resource_group_memory_history(params):
    """
    查询集群组内存历史
    按5分钟峰值（max）进行采样，然后按时间单位取平均值（avg），然后按资源组汇总。
    :param params:
    :return:
    """
    _param_time_minus_window_length(params)
    _parser_time_unit(params)
    params["window_length"] = window_length
    query_sql = """
    SELECT sum(total) as total,
           sum(used) as used,
           sum(total) - sum(used) as available,
           resource_group_id,
           time
    FROM
      (SELECT avg(total) as total,
              avg(used) as used,
              avg(total) - avg(used) as available,
              resource_group_id,
              geog_area_code,
              service_type,
              cluster_id,
              cluster_type,
              cluster_domain,
              queue_name,
              FROM_UNIXTIME((floor(time_5min/{time_unit_seconds}) * {time_unit_seconds}), '{time_fm}') as time
       FROM
         (SELECT ifnull(max(total_memory), 0) as total,
                 ifnull(max(used_memory), 0) as used,
                 ifnull(max(total_memory), 0) - ifnull(max(used_memory), 0) as available,
                 resource_group_id,
                 geog_area_code,
                 service_type,
                 cluster_id,
                 cluster_type,
                 cluster_domain,
                 queue_name,
                 (floor(dtEventTimeStamp/1000/300) * 300 + {window_length}) as time_5min
          FROM {rt}
          WHERE dtEventTime>='{start_time}'
            AND dtEventTime<='{end_time}'
            AND resource_type='processing'
            AND resource_group_id='{resource_group_id}'
            AND geog_area_code='{geog_area_code}'
            AND service_type='{service_type}'
            AND if ('{cluster_id}' == 'all',
                    1=1,
                    cluster_id='{cluster_id}')
          GROUP BY resource_group_id,
                   geog_area_code,
                   service_type,
                   cluster_id,
                   cluster_type,
                   cluster_domain,
                   queue_name,
                   time_5min) t1
       GROUP BY resource_group_id,
                geog_area_code,
                service_type,
                cluster_id,
                cluster_type,
                cluster_domain,
                queue_name,
                time) t
    group by resource_group_id,
             time
    order by time limit 5000
    """.format(
        rt=rt_name, **params
    )
    data = DataQueryHelper.query_data(sql=query_sql, prefer_storage=rt_storage_type)
    empty_result = []
    if data:
        if data.get("list", None):
            _list = data.get("list", [])
            return _list
    return empty_result


def get_resource_group_apps_history(params):
    """
    查询集群组APP数量历史
    按5分钟峰值（max）进行采样，然后按时间单位取平均值（avg），然后按资源组汇总。
    :param params:
    :return:
    """
    _param_time_minus_window_length(params)
    _parser_time_unit(params)
    params["window_length"] = window_length
    query_sql = """
    SELECT sum(total) as total,
           sum(used) as used,
           sum(total) - sum(used) as available,
           resource_group_id,
           time
    FROM
      (SELECT avg(total) as total,
              avg(used) as used,
              avg(total) - avg(used) as available,
              resource_group_id,
              geog_area_code,
              service_type,
              cluster_id,
              cluster_type,
              cluster_domain,
              queue_name,
              FROM_UNIXTIME((floor(time_5min/{time_unit_seconds}) * {time_unit_seconds}), '{time_fm}') as time
       FROM
         (SELECT ifnull(max(applications), 0) as total,
                 ifnull(max(active_applications), 0) as used,
                 ifnull(max(applications), 0) - ifnull(max(active_applications), 0) as available,
                 resource_group_id,
                 geog_area_code,
                 service_type,
                 cluster_id,
                 cluster_type,
                 cluster_domain,
                 queue_name,
                 (floor(dtEventTimeStamp/1000/300) * 300 + {window_length}) as time_5min
          FROM {rt}
          WHERE dtEventTime>='{start_time}'
            AND dtEventTime<='{end_time}'
            AND resource_type='processing'
            AND resource_group_id='{resource_group_id}'
            AND geog_area_code='{geog_area_code}'
            AND service_type='{service_type}'
            AND if ('{cluster_id}' == 'all',
                    1=1,
                    cluster_id='{cluster_id}')
          GROUP BY resource_group_id,
                   geog_area_code,
                   service_type,
                   cluster_id,
                   cluster_type,
                   cluster_domain,
                   queue_name,
                   time_5min) t1
       GROUP BY resource_group_id,
                geog_area_code,
                service_type,
                cluster_id,
                cluster_type,
                cluster_domain,
                queue_name,
                time) t
    group by resource_group_id,
             time
    order by time limit 5000
    """.format(
        rt=rt_name, **params
    )
    data = DataQueryHelper.query_data(sql=query_sql, prefer_storage=rt_storage_type)
    empty_result = []
    if data:
        if data.get("list", None):
            _list = data.get("list", [])
            return _list
    return empty_result


def _param_time_minus_window_length(params):
    """
    因数据使用滑动窗口统计，窗口长度（默认1小时）
    :param params:
    :return:
    """
    start_time = datetime.datetime.strptime(params["start_time"], "%Y-%m-%d %H:%M:%S")
    start_time_1hour = (start_time - datetime.timedelta(seconds=window_length)).strftime("%Y-%m-%d %H:%M:%S")
    # 时间减减窗口长度
    params["start_time"] = start_time_1hour

    end_time = datetime.datetime.strptime(params["end_time"], "%Y-%m-%d %H:%M:%S")
    end_time_1hour = (end_time - datetime.timedelta(seconds=window_length)).strftime("%Y-%m-%d %H:%M:%S")
    # 时间减窗口长度
    params["end_time"] = end_time_1hour


def get_processing_all_cluster_summary(params):
    """
    查询所有集群的计算容量总容量(内存和CPU）
    :param params:
    :return:
    """
    _param_time_minus_window_length(params)
    query_sql = """
    SELECT sum(total_memory) as total_memory,
           sum(total_vcores) as total_vcores,
           cluster_id
    FROM (
        SELECT max(total_memory) as total_memory,
               max(total_vcores) as total_vcores,
               resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               cluster_domain,
               queue_name
        FROM {rt}
        WHERE dtEventTime>='{start_time}' AND dtEventTime<='{end_time}'
        GROUP BY resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               cluster_domain,
               queue_name
        ) t
    GROUP BY cluster_id
    """.format(
        rt=rt_name, **params
    )
    data = DataQueryHelper.query_data(sql=query_sql, prefer_storage=rt_storage_type)
    result = {}
    if data:
        if data.get("list", None):
            _list = data.get("list", [])
            for e in _list:
                result[e.get("cluster_id", "")] = {"memory": e.get("total_memory", 0), "cpu": e.get("total_vcores", 0)}
    return result


def _parser_time_unit(params):
    """
    解析时间单位
    :param params:
    :return:
    """
    time_unit = params.get("time_unit", "hour")
    time_unit_seconds = 3600
    interval = 3600
    time_fm = "%Y-%m-%d %H:%i:%s"
    if time_unit == "5min":
        time_unit_seconds = 300
        interval = 300
    elif time_unit == "10min":
        time_unit_seconds = 600
        interval = 600
    elif time_unit == "30min":
        time_unit_seconds = 1800
        interval = 1800
    elif time_unit == "hour":
        time_unit_seconds = 3600
        interval = 3600
    elif time_unit == "day":
        time_unit_seconds = 1
        interval = 86400
        # 天通过format处理
        time_fm = "%Y-%m-%d 00:00:00"
    params["time_unit_seconds"] = time_unit_seconds
    params["time_fm"] = time_fm
    params["interval"] = interval
