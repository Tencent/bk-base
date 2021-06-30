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
from common.log import logger
from datahub.access import admin_views, deploy_plan_views, raw_data_views, views
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

"""
URL routing patterns for the Engine API.
"""


router = DefaultRouter(trailing_slash=True)

# Add the generated REST URLs
router.register(r"rawdata", raw_data_views.RawDataViewSet, basename="rawdata")
router.register(r"source", views.DataSourceConfigViewSet, basename="data_source")
router.register(r"scenario", views.DataScenarioConfigViewSet, basename="scenario")
router.register(r"encode", views.EncodingConfigViewSet, basename="encode")
router.register(r"time_format", views.TimeFormatConfigViewSet, basename="time_format")
router.register(r"field_type", views.FieldTypeConfigViewSet, basename="field_type")
router.register(r"delimiter", views.DelimiterConfigViewSet, basename="field_delimiter")
router.register(r"category", views.DataCategoryConfigViewSet, basename="category")
router.register(r"db_type", views.DbTypeConfigViewSet, basename="db_type")
router.register(r"scenario_storage", views.DataScenarioStorageViewSet, basename="scenario_storage")
router.register(r"host_config", views.AccessHostConfigViewSet, basename="host_config")

# collect-接入采集
router.register(r"collector", views.CollectorViewSet, basename="collector")
router.register(r"deploy_plan", deploy_plan_views.CollectorDeployPlanViewSet, basename="deploy_plan")

router.register(r"collector", views.CheckViewSet, basename="check")
router.register(r"collector/app_key", views.AppKeyViewSet, basename="app_key")

router.register(r"collector/upload", views.FileViewSet, basename="file")
router.register(r"collector/upload", views.FileRawDataViewSet, basename="rawdata_file")

router.register(r"collectorhub", views.CollectorHubPlanViewSet, basename="collectorhub")

router.register(r"migration", views.MigrationViewSet, basename="migration")
router.register(r"admin", admin_views.AdminViewSet, basename="admin")

try:
    from datahub.databus.extend import urls as extend_urls

    extend_urlpatterns = extend_urls.urlpatterns
except ImportError as e:
    extend_urlpatterns = []
    logger.info(u"failed to import extend urls, %s" % (str(e)))

urlpatterns = extend_urlpatterns + [url(r"^", include(router.urls))]
