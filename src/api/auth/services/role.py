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
from auth.core.permission import RolePermission
from auth.exceptions import UpdateRoleErr
from auth.handlers.object_classes import oFactory
from auth.handlers.role import RoleHandler
from auth.models import UserRole
from auth.models.base_models import RoleConfig
from auth.models.outer_models import ResultTable
from common.log import logger
from django.utils.translation import ugettext as _


class RoleService:
    def __init__(self):
        pass

    @classmethod
    def list_users(cls, role_id, scope_id):
        """
        获取角色成员
        """
        # 查询业务方数据负责人，后续需要根据敏感度动态调整，虚拟角色
        if role_id in ["biz.data_manager"]:
            o_rt = ResultTable.objects.get(result_table_id=scope_id)
            biz_users = set(RoleHandler.list_users("biz.manager", scope_id=o_rt.bk_biz_id))

            return biz_users

        users = set(RoleHandler.list_users(role_id, scope_id=scope_id))
        return list(users)

    @classmethod
    def update_roles(cls, username, role_id, user_ids, scope_id, force_no_ticket=False):
        """
        批量更新角色
        @param username:
        @param role_id:
        @param user_ids:
        @param scope_id:
        @return {Ticket[]} 返回单据列表，目前成功固定为空列表，为了兼容之前的返回结果
        """
        result = RoleHandler.cmp_users(user_ids, role_id, scope_id)

        permissions = []
        permissions.extend(
            [
                {"operate": RoleHandler.GRANT, "user_id": user_id, "role_id": role_id, "scope_id": scope_id}
                for user_id in result["add"]
            ]
        )
        permissions.extend(
            [
                {"operate": RoleHandler.REVOKE, "user_id": user_id, "role_id": role_id, "scope_id": scope_id}
                for user_id in result["delete"]
            ]
        )

        results = RoleHandler.update_role_batch(username, permissions)
        fail_results = [r for r in results if not r[1]]
        fail_userids = [r[0]["user_id"] for r in fail_results]

        if len(fail_results) > 0:
            logger.error(f"[RoleService AddRole] Fail to update role memebers, errors={fail_results}")
            raise UpdateRoleErr(_("更新部分角色成员失败（{}），请联系管理员").format(", ".join(fail_userids)))

        return []

    @classmethod
    def user_roles(
        cls, user_id, object_class, show_display=False, object_name__contains="", scope_id=None, start=None, end=None
    ):
        """
        用户有的角色权限，补充了额外信息及字段
        @param user_id: 用户名
        @param scope_id:
        @param object_class:
        @param show_display: 是否显示展示名
        @param object_name__contains: 显示名包含的字符
        @param start: 起始位置，用于分页
        @param end: 结束位置，用于分页
        @return:
        """
        data = UserRole.user_roles(user_id, scope_id=scope_id, object_class=object_class)

        # 获取数据返回所需要的一些映射关系
        role_object_map = RoleConfig.get_role_object_map()
        roles = cls.list_related_roles(data)

        # 以object_class和scope_id作为唯一键确认一个资源
        uniq_scopes = []
        for _data in data:
            _role_id = _data["role_id"]
            _scope_id = _data["scope_id"]
            _object_class = role_object_map[_role_id]["object_class"]
            _object_name = role_object_map[_role_id]["object_name"]
            uniq_scope = {
                "scope_id": _scope_id,
                "object_class": _object_class,
                "object_class_name": _object_name,
                "object_name": "",
                "object_description": "",
                "roles": [],
            }

            if not cls._is_exist_scope(uniq_scope, uniq_scopes):
                uniq_scope["roles"] = [
                    {
                        "role_id": _role["role_id"],
                        "users": _role["users"],
                    }
                    for _role in roles
                    if _role["scope_id"] == _scope_id and _role["object_class"] == _object_class
                ]
                uniq_scopes.append(uniq_scope)

        if show_display:
            # 补充显示名称
            try:
                o_object_class = oFactory.init_object_by_class(object_class)
            except NotImplementedError:
                # 不支持补全名称的对象，直接忽略
                pass
            else:
                uniq_scopes = o_object_class.wrap_display(
                    uniq_scopes,
                    "scope_id",
                    display_key_maps={"display_name_key": "object_name", "description_key": "object_description"},
                )
            uniq_scopes = [x for x in uniq_scopes if object_name__contains in x.get("object_name", "")]

        # 分页及字段补充
        count = len(uniq_scopes)
        uniq_scopes = uniq_scopes[start:end]
        for _scope in uniq_scopes:
            # 若没有显示名的，以scope_id作为显示名
            _scope["object_name"] = _scope.get("object_name", _scope["scope_id"])
            for _role in _scope.get("roles", []):
                _role["can_modify"] = RolePermission.has_operate_permission(
                    user_id, _scope["scope_id"], _role["role_id"]
                )

        return {"count": count, "data": uniq_scopes}

    @classmethod
    def list_related_roles(cls, scopes):
        """
        列出与该用户有关联的角色，比如用户A是项目管理员，相关联的角色是任务开发员，需把任务开发员的成员也列出来
        @param scopes: 范围列表  [{'role_id: 'project.flow_member', 'scope_id': '1'}]
        @return:
        [
          {
            'authorizer': None,
            'users': [u'admin1', u'admin2'],
            'role_id': u'result_table.manager',
            'scope_id': u'592_test_rt',
            'object_class': u'result_table',
            'can_modify': False
          },
          {
            'authorizer': None,
            'users': [u'admin3'],
            'role_id': u'result_table.manager',
            'scope_id': u'591_test_rt',
            'object_class': u'result_table',
            'can_modify': False
          },
        ]
        """
        if not scopes:
            return []

        # 增加与之关联的角色
        role_object_map = RoleConfig.get_role_object_map()
        object_role_map = RoleConfig.get_object_roles_map()
        cleaned_scopes = cls._cleaned_scopes_for_list_roles(scopes, role_object_map, object_role_map)

        role_id__in = RoleConfig.list_related_roles(cleaned_scopes[0]["role_id"], role_object_map, object_role_map)
        scope_ids = [_scope["scope_id"] for _scope in cleaned_scopes]
        users = UserRole.objects.filter(role_id__in=role_id__in, scope_id__in=scope_ids).values(
            "role_id", "scope_id", "user_id"
        )

        scope_users_map = {}
        for user in users:
            if (user["role_id"], user["scope_id"]) not in scope_users_map:
                scope_users_map[(user["role_id"], user["scope_id"])] = []
            if user["user_id"] not in scope_users_map[(user["role_id"], user["scope_id"])]:
                scope_users_map[(user["role_id"], user["scope_id"])].append(user["user_id"])

        for _scope in cleaned_scopes:
            _scope["object_class"] = role_object_map[_scope["role_id"]]["object_class"]
            # 成员列表
            _scope["users"] = scope_users_map.get((_scope["role_id"], _scope["scope_id"]), [])

        return cleaned_scopes

    @staticmethod
    def _is_exist_scope(scope, scopes):
        """
        判断是否已存在的范围
        @param scope:
        @param scopes:
        @return:
        """
        for _scope in scopes:
            if scope["scope_id"] == _scope["scope_id"] and scope["object_class"] == _scope["object_class"]:
                return True
        return False

    @staticmethod
    def _cleaned_scopes_for_list_roles(scopes, role_object_map, object_role_map):
        """
        清洗scope，提供给list_roles使用
        @param scopes:
        @return:
        """
        additional_scopes = []

        # tuple_scopes 以元组形式记录scope，用于去重
        tuple_scopes = []
        for _scope in scopes:
            related_role_ids = RoleConfig.list_related_roles(_scope["role_id"], role_object_map, object_role_map)
            for _role_id in related_role_ids:
                _tuple_scope = (_role_id, _scope["scope_id"])
                if _tuple_scope in tuple_scopes:
                    continue
                additional_scopes.append(
                    {
                        "scope_id": _scope["scope_id"],
                        "role_id": _role_id,
                    }
                )
                tuple_scopes.append(_tuple_scope)

        # 对scopes去重
        return additional_scopes

    @staticmethod
    def is_done(ticket_list):
        """
        全部单据流程都结束了么
        @param {[AuthTicket]} ticket_list  单据列表
        """
        if isinstance(ticket_list, list):
            if len(ticket_list) > 0 and ticket_list[0].get("status") != SUCCEEDED:
                return False
        return True
