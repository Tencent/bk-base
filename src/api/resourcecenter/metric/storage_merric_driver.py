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
from django.conf import settings
from resourcecenter.dataquery.data_helper import DataQueryHelper

# 存储运营数据的RT
rt_name = settings.STORAGE_METRIC_DATA_RT
rt_storage_type = settings.RESOURCE_CENTER_METRIC_STORAGE_TYPE


def get_resource_group_storage_summary(params):
    """
    查询集群组的存储容量总容量(主要是磁盘）
    :param params:
    :return:
    """
    query_sql = """
    SELECT sum(total_mb) as total_mb,
           sum(use_mb) as use_mb
    FROM (
        SELECT max(CapacityUsedMB) as use_mb,
               max(CapacityTotalMB) as total_mb,
               resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               component_type,
               src_cluster_id
        FROM {rt}
        WHERE dtEventTime>='{start_time}' AND dtEventTime<='{end_time}'
        AND resource_group_id='{resource_group_id}'
        AND if ('{geog_area_code}' == '', 1=1, geog_area_code='{geog_area_code}')
        GROUP BY resource_group_id,
               geog_area_code,
               cluster_id,
               cluster_type,
               component_type,
               src_cluster_id
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
                result["use_mb"] = _list[0].get("use_mb", 0)
                result["total_mb"] = _list[0].get("total_mb", 0)
    return result


def get_resource_group_disk_history(params):
    """
    查询集群组存储历史
    :param params:
    :return:
    """
    query_sql = """
    SELECT
        sum(total) as total,
        sum(used) as used,
        sum(available) as available,
        resource_group_id,
        time
    FROM(
    SELECT ifnull(max(CapacityTotalMB), 0) as total,
           ifnull(max(CapacityUsedMB),  0) as used,
           ifnull(max(CapacityTotalMB), 0) - ifnull(max(CapacityUsedMB), 0) as available,
           resource_group_id,
           geog_area_code,
           service_type,
           cluster_id,
           cluster_type,
           component_type,
           src_cluster_id,
           dtEventTime as time
    FROM {rt}
    WHERE dtEventTime>='{start_time}' AND dtEventTime<='{end_time}'
    AND resource_type='storage'
    AND resource_group_id='{resource_group_id}'
    AND geog_area_code='{geog_area_code}'
    AND service_type='{service_type}'
    AND if ('{cluster_id}' == 'all', 1=1, cluster_id='{cluster_id}')
    GROUP BY resource_group_id,
           geog_area_code,
           service_type,
           cluster_id,
           cluster_type,
           component_type,
           src_cluster_id,
           time
    ) t
    group by resource_group_id, time
    order by time
    limit 5000
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
