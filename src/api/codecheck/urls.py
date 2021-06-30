# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

from codecheck.views import code_check_views, common_views

router = DefaultRouter(trailing_slash=True)

router.register(r"verify", code_check_views.CodeCheckVerifyViewSet, base_name="code_check_verify")
router.register(r"parser_groups", code_check_views.CodeCheckParserGroupViewSet, base_name="code_check_parser_group")
router.register(
    r"blacklist_groups", code_check_views.CodeCheckBlacklistGroupViewSet, base_name="code_check_blacklist_group"
)

urlpatterns = [
    url(r"^", include(router.urls)),
    # error code
    url(r"^errorcodes/$", common_views.CodeCheckErrorCodesView.as_view({"get": "get"})),
]
