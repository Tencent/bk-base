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
from common.api.base import DataAPI


class _AuthApi:
    def __init__(self):
        auth_api_url = settings.AUTH_API_URL

        self.check_user_perm = DataAPI(
            method="POST",
            url=auth_api_url + "users/{user_id}/check/",
            module="AUTH",
            url_keys=["user_id"],
            description="检查用户是否具有指定权限",
        )
        self.batch_check = DataAPI(
            method="POST", url=auth_api_url + "users/batch_check/", module="AUTH", description="批量检测是否有指定权限"
        )
        self.list_user_perm_scopes = DataAPI(
            method="GET",
            url=auth_api_url + "users/{user_id}/scopes/",
            module="AUTH",
            url_keys=["user_id"],
            description="获取用户有权限的对象范围",
        )
        self.list_user_scope_dimensions = DataAPI(
            method="GET", url=auth_api_url + "users/scope_dimensions/", module="AUTH", description="获取用户有权限的维度"
        )
        self.check_token_perm = DataAPI(
            method="POST", url=auth_api_url + "tokens/check/", module="AUTH", description="检查 Token 授权码的权限"
        )
        self.get_token_info = DataAPI(
            method="GET", url=auth_api_url + "tokens/retrive_by_data_token/", module="AUTH", description="获取 TOKEN 信息"
        )
        self.update_project_role_users = DataAPI(
            method="PUT",
            url=auth_api_url + "projects/{project_id}/role_users/",
            module="AUTH",
            url_keys=["project_id"],
            description="更新项目的用户列表",
        )
        self.check_project_data = DataAPI(
            method="POST",
            url=auth_api_url + "projects/{project_id}/data/check/",
            module="AUTH",
            url_keys=["project_id"],
            description="检查项目的数据权限",
        )
        self.sync = DataAPI(method="POST", url=auth_api_url + "sync/", module="AUTH", description="同步权限")
        self.check_project_cluster_group = DataAPI(
            method="POST",
            url=auth_api_url + "projects/{project_id}/cluster_group/check/",
            module="AUTH",
            url_keys=["project_id"],
            description="检查项目是否有集群组权限",
        )
        self.exchange_default_data_token = DataAPI(
            method="POST",
            url=auth_api_url + "tokens/exchange_default_data_token/",
            module="AUTH",
            description="置换默认的 DATA_TOKEN",
        )
        self.list_public_keys = DataAPI(
            method="GET", url=auth_api_url + "esb/list_public_keys/", module="AUTH", description="获取网关公开密钥"
        )


AuthApi = _AuthApi()
