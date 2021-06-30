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


from datamanage.pizza_settings import DATAMANAGE_API_ROOT, DATAMANAGE_API_ROOT_PRODUCT

from common.api.base import DataAPI


class _DatamanageApi(object):
    def __init__(self):

        self.dataset_list = DataAPI(
            url=DATAMANAGE_API_ROOT + "/datamap/retrieve/get_basic_list/",
            method="POST",
            module="datamanage",
            description="获取基础列表信息",
            default_timeout=300,
        )

        self.dataset_list_product = DataAPI(
            url=DATAMANAGE_API_ROOT_PRODUCT + "/datamap/retrieve/get_basic_list/",
            method="POST",
            module="datamanage",
            description="获取基础列表信息",
            default_timeout=300,
        )

        self.get_data_dict_count = DataAPI(
            url=DATAMANAGE_API_ROOT + "/datamap/retrieve/get_data_dict_count/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表统计数据",
            default_timeout=300,
        )

        self.get_data_dict_count_product = DataAPI(
            url=DATAMANAGE_API_ROOT_PRODUCT + "/datamap/retrieve/get_data_dict_count/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表统计数据",
            default_timeout=300,
        )

        self.get_data_dict_list = DataAPI(
            url=DATAMANAGE_API_ROOT + "/datamap/retrieve/get_data_dict_list/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表数据列表",
            default_timeout=300,
        )

        self.get_data_dict_list_product = DataAPI(
            url=DATAMANAGE_API_ROOT_PRODUCT + "/datamap/retrieve/get_data_dict_list/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表数据列表",
            default_timeout=300,
        )

        self.list_heat_metric_by_influxdb = DataAPI(
            url=DATAMANAGE_API_ROOT + "/lifecycle/heat/list_heat_metric_by_influxdb/",
            method="GET",
            module="datamanage",
            description="获取日查询量",
            default_timeout=300,
        )

        # 从influxdb里面拿到的广度指标
        self.range_metric_by_influxdb = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/lifecycle/range/range_metric_by_influxdb/",
            module="datamanage",
            description="range_metric_by_influxdb",
            default_timeout=300,
        )

        # 广度指标变化趋势
        self.list_range_metric_by_influxdb = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/lifecycle/range/list_range_metric_by_influxdb/",
            module="datamanage",
            description="list_range_metric_by_influxdb",
            default_timeout=300,
        )

        # 热度指标变化趋势
        self.list_heat_metric_by_influxdb = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/lifecycle/heat/list_heat_metric_by_influxdb/",
            module="datamanage",
            description="list_heat_metric_by_influxdb",
            default_timeout=300,
        )

        # dmonitor_metrics_query接口
        self.dmonitor_metrics_query = DataAPI(
            url=DATAMANAGE_API_ROOT + "/dmonitor/metrics/query/",
            method="POST",
            module="datamanage",
            description="dmonitor_metrics_query",
            default_timeout=300,
        )

        self.dmonitor_metrics_query_product = DataAPI(
            url=DATAMANAGE_API_ROOT_PRODUCT + "/dmonitor/metrics/query/",
            method="POST",
            module="datamanage",
            description="dmonitor_metrics_query",
            default_timeout=300,
        )

        self.get_standard = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/dstan/standard/get_standard/",
            module="datamanage",
            description="get_standard",
            default_timeout=300,
        )

        self.standard_search = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/dstan/standard/search/",
            module="datamanage",
            description="standard_search",
            default_timeout=300,
        )

        self.get_unit_list = DataAPI(
            method="GET",
            url=DATAMANAGE_API_ROOT + "/dstan/standard/get_unit_list/",
            module="datamanage",
            description="get_unit_list",
            default_timeout=300,
        )

        self.alert_count = DataAPI(
            url=DATAMANAGE_API_ROOT + "/dmonitor/metrics/alert_count/",
            method="GET",
            module="datamanage",
            description="dmonitor_metrics_alert_count",
            default_timeout=300,
        )


DatamanageApi = _DatamanageApi()
