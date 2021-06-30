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
from auth.constants import SUCCEEDED
from auth.core.permission import RolePermission as CoreRolePermission
from auth.models.base_models import ActionConfig, RoleConfig, RolePolicyInfo
from auth.permissions import RolePermission
from auth.services.role import RoleService
from auth.services.self_perms import (
    check_role_manage_perm,
    check_role_manage_perm_batch,
)
from auth.templates.template_manage import TemplateManager
from auth.views.auth_serializers import (
    MultiRoleUserUpdateSerializer,
    RoleSerializer,
    RoleUserListParamSerializer,
    RoleUserUpdateParamSerializer,
    RoleUserUpdateSerializer,
    UserRoleSerializer,
)
from common.auth import require_indentities
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIModelViewSet
from django.db import transaction
from django.http import HttpResponse
from rest_framework.response import Response

from common import local


class RoleViewSet(APIModelViewSet):
    queryset = RoleConfig.objects.all()
    serializer_class = RoleSerializer
    permission_classes = (RolePermission,)
    lookup_value_regex = "[a-zA-Z_.]+"
    lookup_field = "role_id"

    def get_object(self):
        filter_kwargs = {self.lookup_field: self.kwargs[self.lookup_field]}
        return RoleConfig.objects.get(**filter_kwargs)

    def list(self, request, *args, **kwargs):
        """
        @api {get} /auth/roles/ 角色列表，再补充审批员的角色
        @apiName list_roles
        @apiGroup Roles
        @apiSuccessExample {json} Success-Response.data
        [
            {
              "updated_by": "admin",
              "created_at": "2018-12-11 12:10:27",
              "updated_at": null,
              "created_by": "admin",
              "role_id": "biz.manager",
              "role_name": "业务管理员",
              "authorizer": null,
              "object_class": "biz",
              "description": null,
              "actions": [],
            },
        ]
        """
        roles = super().list(request, *args, **kwargs).data
        # roles = []
        # for r in self.queryset:
        #     rs = RoleSerializer(r).data
        #     roles.append(rs)
        role_action_map = RolePolicyInfo.get_role_action_map()
        for role in roles:
            # 补充审批角色
            role["authorizer"] = CoreRolePermission.get_authorizers(role["role_id"])
            # 兼容前端
            role["authorizer"] = None if len(role["authorizer"]) == 0 else ",".join(role["authorizer"])
            # 补充角色动作
            role["actions"] = role_action_map.get(role["role_id"], [])

        return Response(roles)

    def retrieve(self, request, *args, **kwargs):
        """
        @api {get} /auth/roles/:role_id/ 角色详情
        @apiName retrieve_roles
        @apiGroup Roles
        @apiSuccessExample {json} Success-Response.data
        {
          "updated_by": "admin",
          "created_at": "2018-12-11 12:10:27",
          "updated_at": null,
          "created_by": "admin",
          "role_id": "biz.manager",
          "role_name": "业务管理员",
          "authorizer": null,
          "object_class": "biz",
          "description": null,
          "actions": [],
        },
        """
        role = super().retrieve(request, *args, **kwargs).data
        role["authorizer"] = CoreRolePermission.get_authorizers(role["role_id"])
        # 兼容前端
        role["authorizer"] = None if len(role["authorizer"]) == 0 else ",".join(role["authorizer"])
        # 补充角色动作
        role_action_map = RolePolicyInfo.get_role_action_map()
        role["actions"] = role_action_map.get(role["role_id"], [])
        return Response(role)

    @list_route(methods=["get"])
    @params_valid(UserRoleSerializer, add_params=True)
    def user_role(self, request, *args, **kwargs):
        """
        @api {get} /auth/roles/user_role/ 用户有权限的所有角色及对象
        @apiName user_role
        @apiGroup Roles
        @apiSuccessExample {json} Success-Response.data
        [
          {
            object_description: "",
            roles: [
              {
                role_id: "result_table.manager",
                role_name: "结果表管理员",
                users: [
                  "user1"
                ],
                can_modify: true
              }
            ],
            object_name: "",
            object_class_name: "结果数据",
            scope_id: "591_result_table",
            object_class: "result_table"
          },
        ]
        """
        show_display = request.cleaned_params.get("show_display", False)
        object_class = request.cleaned_params["object_class"]
        object_name__contains = request.cleaned_params.get("object_name__contains", "")
        scope_id = request.cleaned_params.get("scope_id")
        page = request.cleaned_params.get("page")
        page_size = request.cleaned_params.get("page_size")
        is_user_mode = request.cleaned_params.get("is_user_mode", True)

        if page and page_size:
            start = (page - 1) * page_size
            end = start + page_size
        else:
            start = end = None

        user_roles = RoleService.user_roles(
            get_request_username(),
            object_class,
            show_display=show_display,
            object_name__contains=object_name__contains,
            scope_id=scope_id,
            start=start,
            end=end,
        )

        role_name_map = RoleConfig.get_role_config_map()
        for result in user_roles["data"]:
            # 若没有显示名的，以scope_id作为显示名
            result["object_name"] = result.get("object_name", result["scope_id"])

            roles = result.get("roles", [])
            # 补充role_name
            for _role in roles:
                _role_id = _role["role_id"]
                if _role_id in role_name_map:
                    _role["role_name"] = role_name_map[_role_id]["role_name"]
                    _role["user_mode"] = role_name_map[_role_id]["user_mode"]
                else:
                    _role["role_name"] = _role_id
                    _role["user_mode"] = False

            if is_user_mode:
                result["roles"] = [_r for _r in roles if _r["user_mode"]]

        if page and page_size:
            return Response(user_roles)
        else:
            return Response(user_roles["data"])

    @detail_route(methods=["get", "put"], url_path="users")
    def users(self, request, role_id):
        if request.method in ["GET"]:
            return self._list_users(request, role_id)
        elif request.method in ["PUT"]:
            return self._update_users(request, role_id)

        return Response(None)

    @list_route(methods=["PUT"])
    @params_valid(RoleUserUpdateSerializer, add_params=False)
    def update_roles(self, request):
        """
        @api {put} /auth/roles/update_roles 更新角色相关用户
        @apiName update_roles
        @apiGroup Roles
        @apiParam {String[]} user_ids 用户列表
        @apiParam {String} role_id 用户角色
        @apiParam {String} scope_id 相关资源标识
        @apiParamExample {json} 参数例子
            {
                "role_id": "project.manager",
                "user_ids: ['user00', 'user01'],
                "scope_id": '1'
            }
        @apiSuccessExample {json} Success-Response.data
            {
                "tickets": [],
                "result": "ok"
            }
        """
        username = local.get_request_username()
        user_ids = request.cleaned_params["user_ids"]
        role_id = request.cleaned_params["role_id"]
        scope_id = request.cleaned_params["scope_id"]

        check_role_manage_perm(username, role_id, scope_id, raise_exception=True)

        tickets = RoleService.update_roles(username, role_id, user_ids, scope_id)
        return Response(self.parse_resp_content(tickets))

    @list_route(methods=["PUT"])
    @params_valid(MultiRoleUserUpdateSerializer, add_params=False)
    def multi_update_roles(self, request):
        """
        @api {put} /auth/roles/multi_update_roles 批量更新角色用户
        @apiName multi_update_roles
        @apiGroup Roles
        @apiParam {Dict[]} role_users 角色用户列表

        @apiParam {String[]} user_ids 用户列表
        @apiParam {String} role_id 用户角色
        @apiParam {String} scope_id 相关资源标识
        @apiParam {String} target_class 相关目标class
        @apiParamExample {json} role_users 参数例子
        [
            {
                "role_id": "project.manager",
                "user_ids": ['user00', 'user01'],
                "scope_id": '1'
            }
        ]
        @apiSuccessExample {json} Success-Response.data
            {
                "tickets": [],
                "result": "ok"
            }
        """

        username = local.get_request_username()

        check_role_manage_perm_batch(username, request.cleaned_params["role_users"], raise_exception=True)

        tickets = {}
        with transaction.atomic(using="basic"):
            for role_user in request.cleaned_params["role_users"]:
                user_ids = role_user["user_ids"]
                role_id = role_user["role_id"]
                scope_id = role_user["scope_id"]
                ticket = RoleService.update_roles(username, role_id, user_ids, scope_id)
                tickets[role_id] = self.parse_resp_content(ticket)
        return Response(tickets)

    @params_valid(RoleUserListParamSerializer, add_params=False)
    def _list_users(self, request, role_id):
        """
        @api {get} /auth/roles/:role_id/users/ 列举角色用户
        @apiName list_role_user
        @apiGroup Role
        @apiParam {string} [scope_id] 部分角色需要传递 scope_id，比如项目管理员需要传递 project_id
        @apiSuccessExample {json} Success-Response.data
            ['user01', 'user01', 'user03']
        """
        scope_id = request.cleaned_params["scope_id"]

        users = RoleService.list_users(role_id, scope_id)
        return Response(users)

    @require_indentities(["inner"])
    @params_valid(RoleUserUpdateParamSerializer, add_params=False)
    def _update_users(self, request, role_id):
        """
        @api {put} /auth/roles/:role_id/users/ 【内部调用】更新角色相关用户
        @apiName inner_update_users
        @apiGroup Role
        @apiParam {string} [scope_id] 部分角色需要传递 scope_id，比如项目管理员需要传递 project_id
        @apiParam {string[]} user_ids 用户列表
        @apiSuccessExample {json} Success-Response.data
            'ok'
        """
        RoleService.update_roles(
            local.get_request_username(),
            role_id,
            request.cleaned_params["user_ids"],
            request.cleaned_params["scope_id"],
            force_no_ticket=True,
        )
        return Response("ok")

    @staticmethod
    def parse_resp_content(ticket_list):
        """
        包裹返回内容
        @param ticket_list:
        @return:
        """
        if isinstance(ticket_list, list):
            if len(ticket_list) > 0 and ticket_list[0].get("status") != SUCCEEDED:
                return {"result": "no", "tickets": ticket_list}
        return {"result": "ok", "tickets": []}


def roles_page(request):
    search_action_id = request.GET.get("action_id")
    search_role_id = request.GET.get("role_id")

    manager = TemplateManager()
    tmp = manager.get_template("/page/roles.mako")

    roles = RoleConfig.objects.values()
    role_action_map = RolePolicyInfo.get_role_action_map()
    actions = ActionConfig.objects.values()

    for role in roles:
        # 补充角色动作
        role["actions"] = role_action_map.get(role["role_id"], [])

    if search_role_id:
        roles = [r for r in roles if search_role_id in r["role_id"]]

    if search_action_id:
        actions = [a for a in actions if search_action_id in a["action_id"]]

    # 优化表格
    display_action_ids = {a["action_id"] for a in actions}
    roles = [r for r in roles if len(set(r["actions"]) & display_action_ids) > 0]
    actions = [a for a in actions if any(a["action_id"] in r["actions"] for r in roles)]

    return HttpResponse(tmp.render(roles=roles, actions=actions))
