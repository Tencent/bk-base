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

from common.log import logger
from datahub.storekit import (
    clickhouse_views,
    cluster_views,
    druid_views,
    es_view,
    hdfs_view,
    iceberg_views,
    ignite_view,
    init_storekit,
    mysql_views,
    result_table_views,
    scenario_views,
    stat_views,
)
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

try:
    from datahub.storekit.extend import urls as extend_urls

    extend_urlpatterns = extend_urls.urlpatterns
except ImportError:
    extend_urlpatterns = []
    logger.error("failed to import extend urls", exc_info=True)


router = DefaultRouter(trailing_slash=True)

router.register(r"iceberg", iceberg_views.IcebergSet, basename="iceberg")
router.register(r"mysql", mysql_views.MysqlSet, basename="mysql")
router.register(r"hdfs", hdfs_view.HdfsSet, basename="hdfs")
router.register(r"es", es_view.EsSet, basename="es")
router.register(r"druid", druid_views.DruidSet, basename="druid")
router.register(r"clickhouse", clickhouse_views.ClickHouseSet, basename="clickhouse")
router.register(r"ignite", ignite_view.IgniteSet, basename="ignite")


# 二级资源 TODO 校验cluster_type，cluster_name只能包含字母、数字、横线、下划线等字符
router.register(r"clusters", cluster_views.AllClusterSet, basename="clusters")
router.register(r"clusters/(?P<cluster_type>\w+)", cluster_views.ClusterConfigSet, basename="type_clusters")
router.register(r"scenarios", scenario_views.AllScenariosSet, basename="scenarios")
router.register(r"scenarios/(?P<cluster_type>\w+)", scenario_views.ScenariosSet, basename="type_scenarios")
router.register(r"result_tables", result_table_views.AllResultTablesSet, basename="all_result_tables")
router.register(r"result_tables/(?P<result_table_id>\w+)", result_table_views.ResultTablesSet, basename="result_tables")
router.register(r"stat/bizs", stat_views.StatBizSet, basename="stat_bizs")
router.register(r"stat/projects", stat_views.StatProjectSet, basename="stat_projects")
router.register(r"stat/components", stat_views.StatComponentSet, basename="stat_components")
router.register(r"stat/result_tables", stat_views.StatRtSet, basename="stat_rts")
router.register(r"init", init_storekit.InitStorekitViewset, basename="init")

urlpatterns = extend_urlpatterns + [
    # 注册旧代码的url
    url(r"^", include(router.urls)),
]
