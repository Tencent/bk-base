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
from common.api.modules.auth import AuthApi
from django.utils.translation import ugettext_lazy as _

from .exceptions import ParamMissError


class UserPerm:
    def __init__(self, bk_username):
        if not bk_username:
            raise ParamMissError(_("bk_username不可为空"))

        self.username = bk_username

    def check(self, bk_app_code, action_id, object_id=None):
        """
        校验操作权限，请查看 AUTHAPI 文档 #api-UserPerm-check_perm

        @param {String} object_id 对象ID
        @param {String} action_id 操作方式
        """
        if not bk_app_code:
            raise ParamMissError(_("bk_app_code不可为空"))

        return AuthApi.check_user_perm(
            {"user_id": self.username, "check_app_code": bk_app_code, "action_id": action_id, "object_id": object_id},
            raise_exception=True,
        ).data

    def batch_check(self, bk_app_code, action_id, object_id_list=None):
        """
        批量校验操作权限, 请查看 AUTHAPI 文档 #api-UserPerm-batch_check_perm

        :param bk_app_code: String bk_app_code
        :param action_id: String 操作方式
        :param object_id_list: 对象ID列表
        :return: list 批量鉴权情况列表
        """
        if not bk_app_code:
            raise ParamMissError(_("bk_app_code不可为空"))
        if not object_id_list:
            return []

        return AuthApi.batch_check(
            {
                "check_app_code": bk_app_code,
                "permissions": [
                    {"user_id": self.username, "action_id": action_id, "object_id": object_id}
                    for object_id in object_id_list
                ],
            },
            raise_exception=True,
        ).data

    def list_scopes(self, action_id, page=1, page_size=100, bk_biz_id=0, tdw_filter=None, search_tdw_std_rt=False):
        """
        获取操作范围，请查看 AUTHAPI 文档 #api-UserPerm-get_scopes
        """
        if search_tdw_std_rt:
            auth_kwargs = {
                "user_id": self.username,
                "action_id": action_id,
                "add_tdw": True,
                "page": page,
                "page_size": page_size,
                "bk_biz_id": bk_biz_id,
            }
            if tdw_filter is not None:
                auth_kwargs["tdw_filter"] = tdw_filter
        else:
            auth_kwargs = {"user_id": self.username, "action_id": action_id}
        return AuthApi.list_user_perm_scopes(auth_kwargs, raise_exception=True).data


class TokenPerm:
    def __init__(self, data_token):
        if not data_token:
            raise ParamMissError(_("data_token不可为空"))

        self.data_token = data_token

    def check(self, bk_app_code, action_id, object_id=None):
        return AuthApi.check_token_perm(
            {
                "check_data_token": self.data_token,
                "check_app_code": bk_app_code,
                "action_id": action_id,
                "object_id": object_id,
            },
            raise_exception=True,
        ).data


class ProjectPerm:
    def __init__(self, project_id):
        self.project_id = project_id

    def update_role_users(self, role_users):
        """
        更新项目用户信息，请查看 AUTHAPI 文档 #api-ProjectUser-update_project_role_user
        """
        AuthApi.update_project_role_users(
            {
                "project_id": self.project_id,
                "role_users": [
                    {"role_id": role_user["role_id"], "user_ids": role_user["user_ids"], "scope_id": self.project_id}
                    for role_user in role_users
                ],
            },
            raise_exception=True,
        )
        return True

    def check_data(self, result_table_id):
        """
        检查项目是否有数据权限，请查看 AUTHAPI 文档 #api-ProjectData-check_project_data
        """
        return AuthApi.check_project_data(
            {"project_id": self.project_id, "result_table_id": result_table_id}, raise_exception=True
        ).data

    def check_cluster_group(self, cluster_group_id):
        """
        检查项目是否有及及群组（资源组）权限，请查看 AUTHAPI 文档 #api-ProjectClusterGroup-check_project_cluster_group
        """
        return AuthApi.check_project_cluster_group(
            {
                "project_id": self.project_id,
                "cluster_group_id": cluster_group_id,
            }
        ).data
