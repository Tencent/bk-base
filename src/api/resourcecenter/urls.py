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

from resourcecenter.views import (
    common_views,
    resource_group_views,
    resource_group_geog_branch_views,
    group_capacity_views,
    cluster_views,
    resource_register_views,
    project_auth_views,
    processing_metrics_views,
    storage_metrics_views,
    databus_metrics_views,
    job_submit_views,
    job_metrics_views,
    healthz_views,
)


"""
URL routing patterns for the Engine API.
"""
router = DefaultRouter(trailing_slash=True)

router.register(r"resource_units", common_views.ResourceUnitConfigViewSet, basename="resource_units_api")
router.register(r"service_configs", common_views.ResourceServiceConfigViewSet, basename="resource_service_configs_api")
router.register(r"resource_groups", resource_group_views.ResourceGroupViewSet, basename="resource_groups_api")
router.register(
    r"resource_geog_area_cluster_group",
    resource_group_geog_branch_views.ResourceGroupGeogBranchViewSet,
    basename="resource_group_geogs_api",
)
router.register(
    r"group_capacity_applys",
    group_capacity_views.GroupCapacityApplyViewSet,
    basename="resource_group_capacity_apply_api",
)
router.register(r"clusters", cluster_views.ResourceClusterViewSet, basename="resource_clusters_api")
router.register(r"cluster_register", resource_register_views.ResourceRegisterViewSet, basename="cluster_register_api")
router.register(r"project_auth", project_auth_views.ProjectAuthViewSet, basename="project_auth_api")
router.register(
    r"processing_metrics", processing_metrics_views.ProcessingMetricsViewSet, basename="processing_metrics_api"
)
router.register(r"storage_metrics", storage_metrics_views.StorageMetricsViewSet, basename="storage_metrics_api")
router.register(r"databus_metrics", databus_metrics_views.DatabusMetricsViewSet, basename="databus_metrics_api")
router.register(r"job_submit", job_submit_views.JobSubmitViewSet, basename="resource_job_submit_api")
router.register(r"job_metrics", job_metrics_views.JobMetricsViewSet, basename="job_metrics_api")
router.register(r"errorcodes", common_views.ErrorCodeViewSet, basename="errorcodes")

urlpatterns = [
    url(r"^", include(router.urls)),
    url(r"^healthz/$", healthz_views.HealthCheckViewSet.as_view({"get": "healthz"})),
]
