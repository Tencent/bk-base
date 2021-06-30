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

from meta.configs import views

router = DefaultRouter(trailing_slash=True)

router.register(r"belongs_to_configs", views.BelongsToConfigViewSet, basename="belongs_to_config")
router.register(r"cluster_group_configs", views.ClusterGroupConfigViewSet, basename="cluster_group_config")
router.register(r"content_language_configs", views.ContentLanguageConfigViewSet, basename="content_language_config")
router.register(r"data_category_configs", views.DataCategoryConfigViewSet, basename="data_category_config")
router.register(r"encoding_configs", views.EncodingConfigViewSet, basename="encoding_config")
router.register(r"field_type_configs", views.FieldTypeConfigViewSet, basename="field_type_config")
router.register(r"job_status_configs", views.JobStatusConfigViewSet, basename="job_status_config")
router.register(r"operation_configs", views.OperationConfigViewSet, basename="operation_config")
router.register(r"processing_type_configs", views.ProcessingTypeConfigViewSet, basename="processing_type_config")
router.register(r"result_table_type_configs", views.ResultTableTypeConfigViewSet, basename="result_table_type_config")
router.register(r"time_format_configs", views.TimeformatConfigViewSet, basename="time_format_config")
router.register(r"transferring_type_configs", views.TransferringTypeConfigViewSet, basename="transferring_type_config")
router.register(r"platform_configs", views.PlatformConfigViewSet, basename="platform_configs")

urlpatterns = [
    url(r"", include(router.urls)),
]
