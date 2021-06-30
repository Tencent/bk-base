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

from common.log import logger

from conf.dataapi_settings import DATAMANAGEAPI_SERVICE_VERSION, APP_NAME, RUN_VERSION
from datamanage.lite.healthz import views as healthz_views
from datamanage.lite.dmonitor import urls as dmonitor_urls
from datamanage.lite.ops import urls as ops_urls
from datamanage.lite.tag import urls as tag_urls
from datamanage.lite.datamap import urls as datamap_urls
from datamanage.lite.datamart import urls as datamart_urls

router = DefaultRouter(trailing_slash=False)

urlpatterns = [
    url(r'^healthz$', healthz_views.HealthCheckView.as_view()),
    url(r'^dmonitor/', include(dmonitor_urls)),
    url(r'^tags/', include(tag_urls)),
    url(r'^ops/', include(ops_urls)),
    url(r'^datamap/', include(datamap_urls)),
    url(r'^datamart/', include(datamart_urls)),
]


try:
    module_str = '{app_name}.extend.versions.{run_ver}.urls'.format(app_name=APP_NAME, run_ver=RUN_VERSION)  # noqa
    _module = __import__(module_str, globals(), locals(), ['*'])
    for _setting in dir(_module):
        if _setting == 'urlpatterns':
            pro_urlpatterns = getattr(_module, _setting)
            urlpatterns.extend(pro_urlpatterns)
except ImportError as e:
    # raise ImportError("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, e))
    logger.error("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, str(e)))


# 加载增值包内容
if DATAMANAGEAPI_SERVICE_VERSION == 'pro':
    try:
        module_str = 'datamanage.pro.urls'
        _module = __import__(module_str, globals(), locals(), ['*'])
        for _setting in dir(_module):
            if _setting == 'urlpatterns':
                pro_urlpatterns = getattr(_module, _setting)
                urlpatterns.extend(pro_urlpatterns)
    except ImportError as e:
        raise ImportError("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, str(e)))
