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


from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from datamanage.pro.datastocktake.views import (
    QueryView,
    DataSourceDistributionView,
    DataTypeDistributionView,
    SankeyDiagramView,
    TagTypeView,
    NodeCountDistributionView,
    ScoreDistributionView,
    QueryCountDistributionView,
    ApplicationDistributionView,
    LevelDistributionView,
    CostDistribution,
    ImportanceDistribution,
)


router = DefaultRouter(trailing_slash=True)

router.register(r'query', QueryView, basename='query')
router.register(r'data_source', DataSourceDistributionView, basename='data_source')
router.register(r'data_type', DataTypeDistributionView, basename='data_type')
router.register(r'sankey_diagram', SankeyDiagramView, basename='sankey_diagram')
router.register(r'tag_type', TagTypeView, basename='tag_type')
router.register(r'node_count', NodeCountDistributionView, basename='node_count')
router.register(r'score_distribution', ScoreDistributionView, basename='score_distribution')
router.register(r'level_distribution', LevelDistributionView, basename='level_distribution')
router.register(r'cost_distribution', CostDistribution, basename='cost_distribution')
router.register(r'importance_distribution', ImportanceDistribution, basename='importance_distribution')
router.register(r'day_query_count', QueryCountDistributionView, basename='day_query_count')
router.register(r'application', ApplicationDistributionView, basename='application')


urlpatterns = [
    url(r'', include(router.urls)),
]
