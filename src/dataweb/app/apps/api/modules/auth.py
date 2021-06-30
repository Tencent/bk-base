# -*- coding=utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import AUTH_APIGATEWAY_ROOT

from ..base import DataAPI, DataDRFAPISet, DRFActionAPI, PassThroughAPI


class _AuthApi(object):
    MODULE = _("数据平台权限模块")
    URL_PREFIX = AUTH_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.projects = DataDRFAPISet(
            url=AUTH_APIGATEWAY_ROOT + "projects/",
            module=self.MODULE,
            primary_key="project_id",
            description="项目权限相关",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            custom_config={
                "data": DRFActionAPI(method="GET", detail=True, description="列举项目相关数据"),
            },
        )
        self.check_user_perm = DataAPI(
            method="POST",
            url=AUTH_APIGATEWAY_ROOT + "users/{user_id}/check/",
            module=self.MODULE,
            url_keys=["user_id"],
            description="检查用户是否具有指定权限",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.check_tdw_table_perm = DataAPI(
            method="POST",
            url=AUTH_APIGATEWAY_ROOT + "tdw/user/check_table_perm/",
            module=self.MODULE,
            description="查询用户是否有TDW表权限",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.get_tdw_clusters_by_user = DataAPI(
            method="GET",
            url=AUTH_APIGATEWAY_ROOT + "tdw/user/get_clusters_by_user/",
            module=self.MODULE,
            description="查询用户有有权限的集群",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.get_db_with_create_priv = DataAPI(
            method="GET",
            url=AUTH_APIGATEWAY_ROOT + "tdw/user/get_db_with_create_priv/",
            module=self.MODULE,
            description="查询用户有写表权限的库",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.batch_check_user_perm = DataAPI(
            method="POST",
            url=AUTH_APIGATEWAY_ROOT + "users/batch_check/",
            module=self.MODULE,
            description="批量校验用户与对象权限",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.get_user_perm_scope = DataAPI(
            method="GET",
            url=AUTH_APIGATEWAY_ROOT + "users/{user_id}/scopes/",
            module=self.MODULE,
            url_keys=["user_id"],
            description="查询用户的权限范围",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.list_user_biz = DataAPI(
            method="GET",
            url=AUTH_APIGATEWAY_ROOT + "users/{user_id}/bizs/",
            module=self.MODULE,
            url_keys=["user_id"],
            description="返回用户有权限的业务列表",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
        self.tickets = DataDRFAPISet(
            url=AUTH_APIGATEWAY_ROOT + "tickets/",
            module=self.MODULE,
            primary_key=None,
            before_request=add_esb_info_before_request,
            description="获取单据列表",
            custom_config={},
        )
        self.roles_users = DataDRFAPISet(
            url=AUTH_APIGATEWAY_ROOT + "roles/{role_id}/users/",
            module=self.MODULE,
            url_keys=["role_id"],
            primary_key=None,
            before_request=add_esb_info_before_request,
            description="列举角色用户",
            custom_config={},
        )
        self.tokens = DataDRFAPISet(
            url=AUTH_APIGATEWAY_ROOT + "tokens/",
            module=self.MODULE,
            primary_key="data_token_id",
            before_request=add_esb_info_before_request,
            description="授权码管理",
            custom_config={},
        )
        self.list_operation_configs = DataAPI(
            method="GET",
            url=AUTH_APIGATEWAY_ROOT + "users/operation_configs/",
            module=self.MODULE,
            description="查询当前后台服务列表",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )
