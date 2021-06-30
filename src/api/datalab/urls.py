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

from datalab.codes import views as codes_views
from datalab.es_query.views import DatalabEsQueryViewSet
from datalab.favorites.views import DatalabFavoriteViewSet
from datalab.healthz import views as healthz_views
from datalab.notebooks.views import (
    DatalabNotebookTaskViewSet,
    NotebookCellRtViewSet,
    NotebookOutputViewSet,
    NotebookRelateOutputViewSet,
)
from datalab.projects.views import DatalabProjectViewSet
from datalab.queries.views import DatalabQueryTaskViewSet
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

router = DefaultRouter(trailing_slash=True)

router.register("projects", DatalabProjectViewSet, basename="projects")
router.register("favorites", DatalabFavoriteViewSet, basename="favorites")
router.register("queries", DatalabQueryTaskViewSet, basename="queries")
router.register("es_query", DatalabEsQueryViewSet, basename="es_query")
router.register("notebooks", DatalabNotebookTaskViewSet, basename="notebooks")
router.register(r"notebooks/(?P<notebook_id>\w+)/outputs", NotebookOutputViewSet, basename="notebooks_outputs")
router.register(
    r"notebooks/(?P<notebook_id>\w+)/cells/(?P<cell_id>\w+)/outputs",
    NotebookRelateOutputViewSet,
    basename="notebooks_relate_outputs",
)
router.register(
    r"notebooks/(?P<notebook_id>\w+)/cells/(?P<cell_id>\w+)/result_tables",
    NotebookCellRtViewSet,
    basename="notebooks_cells_result_tables",
)

urlpatterns = [
    url(r"^healthz/?$", healthz_views.HealthCheckView.as_view()),
    url(r"^errorcodes/?$", codes_views.ErrorCodesView.as_view()),
    url(r"^", include(router.urls)),
]
