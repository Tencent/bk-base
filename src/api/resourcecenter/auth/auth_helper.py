# coding=utf-8
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
from resourcecenter.api.api_driver import APIResponseUtil as res_util
from resourcecenter.api.auth import AuthApi
from resourcecenter.pizza_settings import BASE_RESOURCE_CENTER_URL


class AuthHelper(object):
    @staticmethod
    def get_resource_group_manager(resource_group_id):
        """
        获取资源组管理员
        :param resource_group_id:
        :return:
        """
        request_params = {
            "scope_id": resource_group_id,
            "role_id": "resource_group.manager",
        }
        res = AuthApi.roles.users(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_user_manager_resource_groups():
        """
        获取用户管理的资源组
        :param resource_group_id:
        :return:
        """
        request_params = {
            "action_id": "resource_group.manage_info",
        }
        res = AuthApi.users.scopes(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_role_users(role_id):
        """
        获取角色用户列表
        :param role_id:
        :return:
        """
        request_params = {
            "role_id": role_id,
        }
        res = AuthApi.roles.users(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_scope_role(role_id, scope_id, user_ids):
        """
        更新对象角色用户列表
        :param role_id:
        :param scope_id:
        :param user_ids:
        :return:
        """
        request_params = {
            "role_id": role_id,
            "scope_id": scope_id,
            "user_ids": user_ids
        }
        res = AuthApi.roles_update.users(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_user_manager_resource_group(user_id, object_id):
        """
        校验用户是否有权限管理资源组
        :param user_id:
        :param object_id:
        :return:
        """
        request_params = {
            "bk_username": user_id,
            "action_id": "resource_group.manage_info",
            "object_id": object_id
        }
        res = AuthApi.users.check(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def tickets_create_resource_group(resource_group_id, process_id, bk_biz_id, reason, content, auto_approve=False):
        """
        创建单据-【创建资源组】
        @param resource_group_id: 资源组ID
        @param bk_biz_id
        @param process_id
        @param reason:
        @param content:
        @param auto_approve: True是自动审核，False不自动审核
        @return:
        """
        request_params = {
            "process_id": process_id,
            "ticket_type": "create_resource_group",
            "ticket_step_params": {
                "0&process_scope_id": bk_biz_id,  # 表示第1 步的 process_scope_id 参数赋值为 业务ID
            },
            "reason": reason,
            "content": content,
            "callback_url": BASE_RESOURCE_CENTER_URL + "resource_groups/" + resource_group_id + "/approve_result/",
            "auto_approve": False,
        }
        res = AuthApi.tickets.create_common(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def tickets_create_capacity_apply(apply_id, process_id, bk_biz_id, reason, content, auto_approve=False):
        """
        创建单据-【资源扩容】
        @param apply_id: 申请单ID
        @param bk_biz_id
        @param process_id
        @param reason:
        @param content:
        @param auto_approve: True是自动审核，False不自动审核
        @return:
        """
        request_params = {
            "process_id": process_id,
            "ticket_type": "expand_resource_group",
            "ticket_step_params": {
                "0&process_scope_id": bk_biz_id,  # 表示第1 步的 process_scope_id 参数赋值为 业务ID
            },
            "reason": reason,
            "content": content,
            "callback_url": BASE_RESOURCE_CENTER_URL + "group_capacity_applys/" + str(apply_id) + "/approve_result/",
            "auto_approve": False,
        }
        res = AuthApi.tickets.create_common(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def project_apply_resource_group(project_id, resource_group_id, reason):
        """
        项目申请资源组
        @param project_id: 项目ID
        @param resource_group_id： 资源组ID
        @param reason: 申请原因
        @return:
        """
        request_params = {
            "reason": reason,
            "ticket_type": "use_resource_group",
            "permissions": [
                {
                    "subject_id": project_id,
                    "subject_class": "project_info",
                    "action": "resource_group.use",
                    "object_class": "resource_group",
                    "scope": {"resource_group_id": resource_group_id},
                }
            ],
        }
        res = AuthApi.tickets.create(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def tickets_withdraw(process_id, process_message, check_success=True):
        """
        撤销单据
        @param process_id:
        @param check_success:
        @param process_message:
        @return:
        """
        ticket_id = AuthHelper.get_ticket_id_by_process_id(process_id)
        if not ticket_id:
            # 票据不存在, 直接删除
            return []
        request_params = {
            "ticket_id": ticket_id,
            "process_message": process_message
        }
        res = AuthApi.tickets.withdraw(request_params)
        res_util.check_response(res, check_success=check_success, self_message=False)
        return res.data

    @staticmethod
    def get_ticket_id_by_process_id(process_id, check_success=True):
        """
        获取单据ID
        @param process_id:
        @param check_success:
        @return:
        """
        res = AuthApi.tickets.list({"process_id": process_id})
        res_util.check_response(res, check_success=check_success, self_message=False)
        if res.data and len(res.data):
            ticket_id = res.data[0]["id"]
            return ticket_id
        return None

    @staticmethod
    def get_project_resource_group_tickets(project_id, check_success=True):
        """
        获取项目申请资源组单据信息（相同资源组显示最新）
        @param project_id:
        @param check_success:
        @return:
        """
        request_params = {"project_id": project_id}
        res = AuthApi.projects.resource_group_tickets(request_params)
        res_util.check_response(res, check_success=check_success, self_message=False)
        return res.data

    @staticmethod
    def get_resource_group_auth_subjects(resource_group_id, check_success=True):
        """
        查找资源组授权主体
        @param resource_group_id:
        @param check_success:
        @return:
        """
        request_params = {"resource_group_id": resource_group_id}
        res = AuthApi.resource_groups.authorized_subjects(request_params)
        res_util.check_response(res, check_success=check_success, self_message=False)
        return res.data
