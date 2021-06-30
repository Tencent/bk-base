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

from datamanage.pro.dataquality.profiling_views import DataProfilingViewSet
from datamanage.pro.dataquality.sampling_views import DataSetSamplingViewSet
from datamanage.pro.dataquality.summary_views import DataqualitySummaryViewSet
from datamanage.pro.dataquality.event_views import (
    DataQualityEventViewSet,
    DataQualityEventTypeViewSet,
    DataQualityEventTemplateVariableViewSet,
)
from datamanage.pro.dataquality.metric_views import DataQualityMetricsViewSet
from datamanage.pro.dataquality.rule_views import (
    DataQualityRuleViewSet,
    DataQualityRuleTemplateViewSet,
    DataQualityAuditFunctionViewSet,
    DataQualityAuditTaskViewSet,
)
from datamanage.pro.dataquality.correction_views import (
    DataQualityCorrectConfigViewSet,
    DataQualityCorrectConditionViewSet,
    DataQualityCorrectHandlerViewSet,
)

router = DefaultRouter(trailing_slash=True)

router.register(r'profiling', DataProfilingViewSet, basename='data_quality_profiling')
router.register(r'sampling', DataSetSamplingViewSet, basename='data_profling_sampling')
router.register(r'summary', DataqualitySummaryViewSet, basename='data_quality_summary')
router.register(r'events', DataQualityEventViewSet, basename='data_quality_events')
router.register(r'event_types', DataQualityEventTypeViewSet, basename='data_quality_event_types')
router.register(
    r'event_template_variables',
    DataQualityEventTemplateVariableViewSet,
    basename='data_quality_event_template_variables',
)
router.register(r'rules', DataQualityRuleViewSet, basename='data_quality_rules')
router.register(r'audit_tasks', DataQualityAuditTaskViewSet, basename='data_quality_audit_tasks')
router.register(r'rule_templates', DataQualityRuleTemplateViewSet, basename='data_quality_rule_templates')
router.register(r'functions', DataQualityAuditFunctionViewSet, basename='data_quality_functions')
router.register(r'metrics', DataQualityMetricsViewSet, basename='data_quality_metrics')
router.register(r'correct_configs', DataQualityCorrectConfigViewSet, basename='data_quality_correct_configs')
router.register(r'correct_conditions', DataQualityCorrectConditionViewSet, basename='data_quality_correct_conditions')
router.register(r'correct_handlers', DataQualityCorrectHandlerViewSet, basename='data_quality_correct_handlers')


urlpatterns = [
    url(r'', include(router.urls)),
]
