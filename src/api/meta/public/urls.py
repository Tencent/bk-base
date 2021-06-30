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

from meta.public.views import (
    biz,
    data_processing,
    data_transferring,
    error_code,
    event,
    import_sync,
    meta_sync,
    miscellaneous,
    project,
    result_table,
    transaction,
)

router = DefaultRouter(trailing_slash=True)

router.register(r"bizs", biz.BizViewSet, basename="biz")
router.register(r"projects", project.ProjectViewSet, basename="project")
router.register(r"result_tables", result_table.ResultTableViewSet, basename="result_table")
router.register(r"data_processings", data_processing.DataProcessingViewSet, basename="data_processing")
router.register(r"data_transferrings", data_transferring.DataTransferringViewSet, basename="data_transferring")
router.register(r"data_sets", miscellaneous.DataSetViewSet, basename="data_set")
router.register(r"lineage", miscellaneous.LineageViewSet, basename="lineage")
router.register(r"dm_category_configs", miscellaneous.DMCategoryConfigViewSet, basename="dm_category_config")
router.register(r"dm_layer_configs", miscellaneous.DMLayerConfigViewSet, basename="dm_layer_config")
router.register(r"errorcodes", error_code.ErrorCodesViewSet, basename="errorcode")
router.register(r"events", event.EventViewSet, basename="event")
router.register(r"event_supports", event.EventSupportViewSet, basename="event_support")


urlpatterns = [
    url(r"", include(router.urls)),
    url(r"meta_transaction/$", transaction.MetaTransactionView.as_view(), name="meta_transaction"),
    url(r"sync_hook/$", meta_sync.SyncHookView.as_view(), name="meta_sync"),
    url(r"sync/$", meta_sync.SyncHookView.as_view()),
    url(r"import_hook/$", import_sync.ImportHookView.as_view()),
]
