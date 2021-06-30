# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import DATAMANAGE_APIGATEWAY_ROOT

from ..base import DataAPI, DataDRFAPISet, DRFActionAPI, PassThroughAPI


class _DataManageApi(object):
    MODULE = _("数据平台数据管理模块")
    URL_PREFIX = DATAMANAGE_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI

        # 当请求参数sql中包含GROUP BY, data_format返回字段"series",
        # 不包含GROUP BY时，data_format返回字段"simple",
        # 两者返回的格式不同，需注意区分
        self.dmonitor_metrics = DataDRFAPISet(
            url=DATAMANAGE_APIGATEWAY_ROOT + "dmonitor/metrics/",
            module=self.MODULE,
            primary_key="",
            before_request=add_esb_info_before_request,
            custom_config={
                "query": DRFActionAPI(method="POST", detail=False, default_timeout=10, description="查询数据质量TSDB中的监控指标"),
                "output_count": DRFActionAPI(method="GET", detail=False, default_timeout=10, description="查询数据集的数据输出量"),
                "input_count": DRFActionAPI(method="GET", detail=False, default_timeout=10, description="查询数据集的数据输入量"),
            },
        )
        # 标签按数据集个数排序的接口
        self.tag_sort_count = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/get_tag_sort_count/",
            module=self.MODULE,
            description="get_tag_sort_count",
            before_request=add_esb_info_before_request,
        )
        # 获得map树形结构
        self.data_map_tree = DataAPI(
            method="POST",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/search/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_data_map_tree",
        )
        # 获得数据集的列表(数据字典列表)，原接口
        self.dataset_list = DataAPI(
            method="POST",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/get_basic_list/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_dataset_list",
        )
        # 数据字典列表，拆分接口
        self.get_data_dict_list = DataAPI(
            method="POST",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/get_data_dict_list/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_data_dict_list",
        )
        # 数据字典列表的统计，拆分接口
        self.get_data_dict_count = DataAPI(
            method="POST",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/get_data_dict_count/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_data_dict_count",
        )
        # 获取数据地图右侧的统计数据
        self.statistic_data = DataAPI(
            method="POST",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamap/retrieve/search_summary/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_statistic_data",
        )
        # tag类型配置
        self.tag_type_info = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datamart/tag_type/info/",
            module=self.MODULE,
            description="tag_type_info",
            before_request=add_esb_info_before_request,
        )
        # 热度得分
        self.heat_metric_by_influxdb = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "lifecycle/heat/heat_metric_by_influxdb/",
            module=self.MODULE,
            description="heat_metric_by_influxdb",
            before_request=add_esb_info_before_request,
        )
        # 广度得分
        self.range_metric_by_influxdb = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "lifecycle/range/range_metric_by_influxdb/",
            module=self.MODULE,
            description="range_metric_by_influxdb",
            before_request=add_esb_info_before_request,
        )

        # 最近热门查询
        self.popular_query = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "datastocktake/query/popular_query/",
            module=self.MODULE,
            description="popular_query",
            before_request=add_esb_info_before_request,
        )

        # 数据标准列表，some new simple api will tabe place of this one
        self.standard_search = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "dstan/standard/search/",
            module=self.MODULE,
            description="standard_search",
            before_request=add_esb_info_before_request,
        )

        # 所有标签字典
        self.tag_dict = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "dstan/standard_publicity/tag_dict/",
            module=self.MODULE,
            description="tag_dict",
            before_request=add_esb_info_before_request,
        )

        # 获取标准相关属性
        self.get_standard = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "/dstan/standard/get_standard/",
            module=self.MODULE,
            description="get_standard",
            before_request=add_esb_info_before_request,
        )
        # 判断数据集是否标准
        self.is_standard = DataAPI(
            method="GET",
            url=DATAMANAGE_APIGATEWAY_ROOT + "/dstan/standard_publicity/is_standard/",
            module=self.MODULE,
            description="is_standard",
            before_request=add_esb_info_before_request,
        )
