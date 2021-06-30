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

from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

from dataflow.component.views import cluster_config_views, hdfs_views, views, yarn_views
from dataflow.flow.views import job_views

router = DefaultRouter(trailing_slash=True)
router.register(r"hdfs/namenode", hdfs_views.NameNodeViewSet, basename="namenode")
router.register(r"hdfs", hdfs_views.HDFSUtilViewSet, basename="hdfs_liststatus")
router.register(
    r"hdfs/result_tables/(?P<result_table_id>\w+)",
    hdfs_views.ResultTableViewSet,
    basename="hdfs_result_table",
)
router.register(
    r"yarn/(?P<yarn_id>\w+)/resourcemanager",
    yarn_views.ResourceManagerViewSet,
    basename="resourcemanager",
)

router.register(
    r"cluster_configs",
    cluster_config_views.ClusterConfigViewSet,
    basename="component_cluster_config",
)

# 注意：component中原有的jobs相关接口实现已转移至flow/views/job_views内，
# 但为了兼容旧有的调用，在component的urlpatterns中仍旧保留jobs，将其直接指向flow/views/job_views
urlpatterns = [
    url(r"^", include(router.urls)),
    url(
        r"^healthz/$",
        views.HealthCheckView.as_view({"get": "healthz"}),
        name="common_healthz",
    ),
    url(
        r"^jobs/(?P<job_id>.*)/$",
        job_views.JobViewSet.as_view({"get": "retrieve"}),
        name="retrieve",
    ),
]
