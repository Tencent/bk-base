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

from dataflow.shared.log import stream_logger as logger
from dataflow.stream.views import (
    base_views,
    cluster_config_views,
    debug_views,
    job_views,
    monitor_views,
    processing_views,
    result_table_views,
    version_config_views,
)

router = DefaultRouter(trailing_slash=True)

router.register(r"processings", processing_views.ProcessingViewSet, basename="stream_processing")

router.register(r"jobs", job_views.JobViewSet, basename="stream_job")

router.register(
    r"result_tables",
    result_table_views.ResultTableViewSet,
    basename="stream_result_table",
)

router.register(r"debugs", debug_views.DebugViewSet, basename="stream_debug")

router.register(
    r"cluster_config",
    cluster_config_views.ClusterConfigViewSet,
    basename="stream_cluster_config",
)

router.register(
    r"version_config",
    version_config_views.VersionConfigViewSet,
    basename="stream_version_config",
)

router.register(r"monitor_flink", monitor_views.MonitorViewSet, basename="stream_monitor_flink")

try:
    from dataflow.stream.extend.views import yaml_views

    router.register(r"yamls", yaml_views.YamlViewSet, basename="stream_yaml")
except Exception as e:
    logger.exception("Failed to import yaml_views, {}".format(e))

urlpatterns = [
    url(r"^", include(router.urls)),
    url(r"^healthz/$", base_views.HealthCheckView.as_view({"get": "healthz"})),
    # error code
    url(r"^errorcodes/$", base_views.StreamErrorCodesView.as_view({"get": "get"})),
]
