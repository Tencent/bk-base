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

from django.conf import settings
from django.conf.urls import include, url

from meta.analyse import urls as analyse_urls
from meta.basic import urls as basic_urls
from meta.configs import urls as configs_urls
from meta.health import views as healthz_views
from meta.public import urls as public_urls
from meta.tag import urls as tag_urls

"""
URL routing patterns for the Engine API.
Add the generated REST URLs
"""

urlpatterns = [
    url(r"^healthz$", healthz_views.HealthzViewSet.as_view(), name="meta-healthz"),
    url(r"", include(public_urls)),
    url(r"", include(configs_urls)),
    url(r"^basic/", include(basic_urls)),
    url(r"^tag/", include(tag_urls)),
    url(r"^analyse/", include(analyse_urls)),
]

if settings.ENABLED_TDW:
    from meta.extend.tencent.tdw import urls as tdw_urls

    urlpatterns.append(url(r"^tdw/", include(tdw_urls)))
