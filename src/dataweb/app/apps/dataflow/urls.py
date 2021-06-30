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

from django.conf.urls import include, url
from rest_framework import routers

from apps.dataflow import views
from apps.dataflow.views import (
    algorithm_model_views,
    auth_views,
    biz_views,
    cache_views,
    data_dict_views,
    data_map_views,
    data_stocktake_views,
    dataid_views,
    etl_views,
    flow_views,
    project_views,
    result_table_views,
    tool_views,
)

app_name = "dataflow"

router = routers.DefaultRouter(trailing_slash=True)
router.register(r"projects", project_views.ProjectViewSet, base_name="project")

router.register(r"bizs", biz_views.BizSet, base_name="biz")
router.register(r"dataids", dataid_views.DataIdSet, base_name="dataid")
router.register(r"etls", etl_views.EtlViewSet, base_name="etl")
router.register(r"result_tables", result_table_views.ResultTableSet, base_name="result_table")
router.register(r"flows", flow_views.FlowViewSet, base_name="flow")
router.register(r"algorithm_models", algorithm_model_views.AlgorithmModelViewSet, base_name="algorithm_model")
router.register(r"tools", tool_views.ToolsViewSet, base_name="tool")
router.register(r"auth", auth_views.AuthViewSet, base_name="auth")
router.register(r"cache", cache_views.CacheViewSet, base_name="cache")
router.register(r"datamart/datamap/tree", data_map_views.TreeViewSet, base_name="datamart")
router.register(r"datamart/datamap/tag", data_map_views.TagViewSet, base_name="datamart")
router.register(r"datamart/datamap/statistic", data_map_views.StatisticDataViewSet, base_name="datamart")
router.register(r"datamart/datadict", data_dict_views.ResultTabelInfoViewSet, base_name="datadict")
router.register(r"datamart/datadict/dataset_list", data_dict_views.DataSetListViewSet, base_name="datadict")
router.register(r"datamart/datadict/rt_field_edit", data_dict_views.ResultTabelFieldViewSet, base_name="datadict")
router.register(r"datamart/datadict", data_dict_views.LineageListViewSet, base_name="datadict")
router.register(r"datamart/datastocktake", data_stocktake_views.DataStockTakeViewSet, base_name="datastocktake")

urlpatterns = [
    url(r"^$", views.home, name="home"),
    url(r"^signature.png$", views.signature, name="signature"),
    url(r"^", include(router.urls)),
    url(r"^v3/(?P<module>[^/.]+)/(?P<sub_url>.+)", views.pass_through),
    # OAuth
    url(r"^oauth/authorize/$", auth_views.OauthAuthorizeView.as_view()),
]
