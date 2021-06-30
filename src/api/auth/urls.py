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
from auth.views import (
    auth_views,
    object_views,
    project_views,
    raw_data_views,
    resource_group_views,
    role_views,
    sensitivity_views,
    ticket_views,
    token_views,
    user_views,
)
from auth.views.ticketV2_views import ItsmTicketViewSet
from common.log import logger
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

router = DefaultRouter(trailing_slash=True)

# 权限中心接口
router.register(r"sensitivity", sensitivity_views.SensitivityViewSet, basename="sensitivity")
router.register(r"objects", object_views.ObjectViewSet, basename="objects")
router.register(r"roles", role_views.RoleViewSet, basename="roles")
router.register(r"tokens", token_views.TokenPermViewSet, basename="tokens")
router.register(r"users", user_views.UserPermViewSet, basename="users")
router.register(r"projects", project_views.ProjectViewSet, basename="projects")
router.register(r"raw_data", raw_data_views.RawDataViewSet, basename="raw_data")
router.register(r"resource_groups", resource_group_views.ResourceGroupViewSet, basename="resource_groups")

# 单据中心接口
router.register(r"tickets", ticket_views.TicketViewSet, basename="tickets")
router.register(r"ticket_states", ticket_views.TicketStateViewSet, basename="ticket_states")

# 单据中心V2 接口
router.register(r"tickets_v2", ItsmTicketViewSet, basename="tickets_v2")

# TDW相关接口
try:
    from auth.extend.views import tdw_views

    router.register(r"tdw/user", tdw_views.TdwUserViewSet, basename="tdw_user")
except ImportError as e:
    logger.info("环境中不存在 TDW 接口集：{}".format(e))

# 网关控制接口
router.register(r"esb", auth_views.ESBControlViewSet, basename="esb")

urlpatterns = [
    # 检查接口
    url(r"^healthz/$", auth_views.HealthCheckView.as_view()),
    # 错误码接口
    url(r"^errorcodes/$", auth_views.AuthErrorCodesView.as_view()),
    url(r"^common_errorcodes/$", auth_views.CommonErrorCodesView.as_view()),
    # 同步权限
    url(r"^sync/$", auth_views.AuthSyncViewSet.as_view({"post": "post"})),
    # iam 权限中心资源接口
    url(r"^iam/", include("auth.bkiam.urls")),
    # 核心配置接口
    url(r"^core_config/", auth_views.CoreConfigViewSet.as_view({"get": "get"})),
    # 独立的页面
    url(r"^auth_page/$", role_views.roles_page),
    # 自动生成 RESTFUL URLs
    url(r"^", include(router.urls)),
]
