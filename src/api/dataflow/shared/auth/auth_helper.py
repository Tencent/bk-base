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

from dataflow.shared.api.modules.auth import AuthApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class AuthHelper(object):
    @staticmethod
    def list_project_cluster_group(project_id):
        request_params = {"project_id": project_id}
        res = AuthApi.projects.cluster_group(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_tdw_table_perm(cluster_id, db_name, table_name):
        request_params = {
            "cluster_id": cluster_id,
            "db_name": db_name,
            "table_name": table_name,
        }
        res = AuthApi.tdw_user.check_table_perm(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_tdw_app_group_perm(app_group):
        request_params = {"app_group": app_group}
        res = AuthApi.tdw_user.check_app_group_perm(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_project_users(project_id):
        request_params = {
            "scope_id": project_id,
            "object_class": "project",
        }
        res = AuthApi.user_role.list(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def tickets_create_common(
        process_id,
        ticket_type,
        reason,
        content,
        ticket_step_params=None,
        auto_approve=False,
    ):
        """
        创建单据
        @param process_id: 字符串id
        @param ticket_type: 单据类型，如 batch_recalc
        @param reason:
        @param content:
        @param ticket_step_params:
        @param auto_approve: True是自动审核，False不自动审核
        @return:
        """
        request_params = {
            "process_id": process_id,
            "ticket_type": ticket_type,
            "reason": reason,
            "content": content,
            "ticket_step_params": ticket_step_params,
            "auto_approve": auto_approve,
        }
        res = AuthApi.tickets.create_common(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def tickets_withdraw(ticket_id, process_message, check_success=True):
        """
        撤销单据
        @param ticket_id:
        @param check_success:
        @param process_message:
        @return:
        """
        request_params = {"ticket_id": ticket_id, "process_message": process_message}
        res = AuthApi.tickets.withdraw(request_params)
        res_util.check_response(res, check_success=check_success, self_message=False)
        return res.data

    @staticmethod
    def update_scope_role(role_id, scope_id, user_ids):
        request_params = {
            "role_id": role_id,
            "scope_id": scope_id,
            "user_ids": user_ids,
        }
        res = AuthApi.roles.users(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def batch_check_user(user_id, action_id, object_ids):
        """
        批量校验用户与对象权限
        @param user_id:
        @param action_id: result_table.retrieve
        @param object_ids:
        @return:
        """
        if not object_ids:
            return []
        permissions = []
        for object_id in object_ids:
            permissions.append({"action_id": action_id, "user_id": user_id, "object_id": object_id})
        request_params = {"permissions": permissions}
        res = AuthApi.users.batch_check(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def list_project_role_users(project_id):
        """
        获取project相关的角色和用户列表
        @param project_id:
        @return:
        """
        request_params = {"project_id": project_id}
        res = AuthApi.projects.role_users(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
