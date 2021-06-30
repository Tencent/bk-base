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

from dataflow.modeling.views import (
    algorithm_views,
    base_views,
    basic_model_views,
    debug_views,
    experiment_views,
    job_views,
    model_views,
    processing_views,
    queryset_views,
)

router = DefaultRouter(trailing_slash=True)
router.register(r"jobs", job_views.JobViewSet, basename="model_jobs")
router.register(r"processings", processing_views.ProcessingViewSet, basename="model_processing")
router.register(
    r"basic_models",
    basic_model_views.BasicModelViewSet,
    basename="modeling_basic_model",
)
router.register(r"querysets", queryset_views.QuerySetViewSet, basename="model_queryset")
router.register(r"models", model_views.ModelViewSet, basename="models")
router.register(r"debugs", debug_views.DebugViewSet, basename="debug")
router.register(r"algorithms", algorithm_views.AlgorithmViewSet, basename="algorithm")
router.register(
    r"experiments/(?P<model_id>\w+)",
    experiment_views.ModelExperimentViewSet,
    basename="experiment",
)

urlpatterns = [
    url(r"^", include(router.urls)),
    url(r"^healthz/$", base_views.HealthCheckView.as_view({"get": "healthz"})),
    # error code
    url(r"^errorcodes/$", base_views.ModelingErrorCodesView.as_view({"get": "get"})),
]
