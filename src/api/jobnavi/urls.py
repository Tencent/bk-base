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
from .views import (
    schedule_views,
    execute_views,
    event_views,
    task_type_views,
    admin_schedule_views,
    data_makeup_views,
    runner_views,
    task_log_views,
    node_label_views,
    config_views,
    error_code_views,
    health_check_views
)

"""
URL routing patterns for the dataflow Apis
"""
router = DefaultRouter(trailing_slash=True)
router.register(
    r"cluster/(?P<cluster_id>\w+)/schedule",
    schedule_views.ScheduleViewSet,
    basename="jobnavi_schedule"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/execute",
    execute_views.ExecuteViewSet,
    basename="jobnavi_execute"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/event",
    event_views.EventViewSet,
    basename="jobnavi_event"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/task_type",
    task_type_views.TaskTypeViewSet,
    basename="jobnavi_task_type"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/admin/schedule",
    admin_schedule_views.AdminScheduleViewSet,
    basename="jobnavi_admin_schedule"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/data_makeup",
    data_makeup_views.DataMakeupViewSet,
    basename="jobnavi_data_makeup"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/runner",
    runner_views.RunnerViewSet,
    basename="jobnavi_runner"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/task_log",
    task_log_views.TaskLogViewSet,
    basename="jobnavi_task_log"
)
router.register(
    r"cluster/(?P<cluster_id>\w+)/node_label",
    node_label_views.NodeLabelViewSet,
    basename="jobnavi_node_label"
)
router.register(
    r"cluster_config", config_views.ConfigViewSet, basename="jobnavi_config"
)
router.register(
    r"errorcodes", error_code_views.ErrorCodeViewSet, basename="errorcodes"
)

urlpatterns = [
    url(r"^", include(router.urls)),
    # health check
    url(
        r"^healthz/$",
        health_check_views.HealthCheckViewSet.as_view({"get": "healthz"}),
        name="jobnavi_healthz"),
]
