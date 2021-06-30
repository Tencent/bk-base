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

from datamanage.lite.dmonitor.flow_views import (
    DmonitorFlowViewSet,
    DmonitorDatasetViewSet,
    DmonitorDataOperationsViewSet,
)
from datamanage.lite.dmonitor.alert_views import (
    AlertViewSet,
    AlertDetailViewSet,
    AlertShieldViewSet,
    AlertNotifyWayViewSet,
    AlertTargetViewSet,
)
from datamanage.lite.dmonitor.monitor_views import AlertConfigViewSet
from datamanage.lite.dmonitor.metric_views import DmonitorMetricsViewSet
from datamanage.lite.dmonitor.task_views import BatchExecutionsViewSet, BatchScheduleViewSet
from datamanage.lite.dmonitor.views import (
    DmonitorResultTableViewSet,
)

router = DefaultRouter(trailing_slash=True)

router.register(r'metrics', DmonitorMetricsViewSet, basename='dmonitor_metrics')
router.register(r'result_tables', DmonitorResultTableViewSet, basename='dmonitor_result_tables')
router.register(r'batch_executions', BatchExecutionsViewSet, basename='dmonitor_batch_executions')
router.register(r'batch_schedules', BatchScheduleViewSet, basename='dmonitor_batch_schedules')
router.register(r'alert_configs', AlertConfigViewSet, basename='alert_configs')
router.register(r'alerts', AlertViewSet, basename='alerts')
router.register(r'alert_details', AlertDetailViewSet, basename='alert_details')
router.register(r'alert_targets', AlertTargetViewSet, basename='alert_targets')
router.register(r'alert_shields', AlertShieldViewSet, basename='alert_shields')
router.register(r'flows', DmonitorFlowViewSet, basename='dmonitor_flow')
router.register(r'data_sets', DmonitorDatasetViewSet, basename='dmonitor_data_set')
router.register(r'data_operations', DmonitorDataOperationsViewSet, basename='dmonitor_data_operation')
router.register(r'notify_ways', AlertNotifyWayViewSet, basename='dmonitor_notify_ways')


urlpatterns = [
    url(r'', include(router.urls)),
]
