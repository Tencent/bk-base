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

from dataflow.batch.views import (
    custom_calculates_views,
    custom_jobs_views,
    data_makeup_views,
    debug_views,
    hdfs_views,
    interactive_servers_views,
    job_views,
    processings_views,
    views,
    yarn_views,
)

router = DefaultRouter(trailing_slash=True)
router.register(r"hdfs/namenode", hdfs_views.NameNodeViewSet, basename="namenode")
router.register(r"hdfs/(?P<hdfs_ns_id>\w+)", hdfs_views.HDFSUtilViewSet, basename="hdfs_liststatus")
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

router.register(r"jobs", job_views.JobViewSet, basename="batch_job")
router.register(
    r"jobs/(?P<job_id>\w+)/processings",
    job_views.JobProcessingsViewSet,
    basename="batch_job_processings",
)
router.register(r"processings", processings_views.ProcessingsViewSet, basename="batch_processings")
# router.register(r'adhoc', adhoc_views.ADHocViewSet, basename='batch_adhoc')
router.register(r"debugs", debug_views.DebugViewSet, basename="batch_debug")
router.register(
    r"custom_calculates",
    custom_calculates_views.CustomCalculatesViewSet,
    basename="batch_recalculates",
)
router.register(r"cluster_config", views.ConfigViewSet, basename="batch_cluster_config")
router.register(r"data_makeup", data_makeup_views.DataMakeupViewSet, basename="batch_data_makeup")
router.register(
    r"interactive_servers",
    interactive_servers_views.InteractiveServersViewSet,
    basename="batch_interactive_server",
)
router.register(
    r"interactive_servers/(?P<server_id>\w+)/codes",
    interactive_servers_views.InteractiveServersCodeViewSet,
    basename="batch_interactive_server",
)
router.register(r"custom_jobs", custom_jobs_views.CustomJobsViewSet, basename="batch_custom_jobs")

urlpatterns = [
    url(r"^", include(router.urls)),
    url(
        r"^healthz/$",
        views.HealthCheckView.as_view({"get": "healthz"}),
        name="batch_healthz",
    ),
    url(r"^deploy/$", views.DeployView.as_view({"get": "deploy"}), name="batch_deploy"),
    url(
        r"^errorcodes/$",
        views.BatchErrorCodesView.as_view({"get": "errorcodes"}),
        name="batch_errorcodes",
    ),
]
