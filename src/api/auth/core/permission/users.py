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
from auth.bkiam.backend import IAMBackend
from auth.core.beauty import hanlder_auth_check_exception
from auth.core.default.backend import RBACBackend
from auth.core.permission.base import BasePermissionMixin
from auth.exceptions import NoFunctionErr, PermissionDeniedError
from auth.handlers.object_classes import oFactory
from auth.handlers.role import RoleHandler
from auth.models.auth_models import UserRole
from auth.models.base_models import ActionConfig, RoleConfig
from common.log import logger

try:
    from auth.extend.core.tdw.backend import DEFAULT_TDW_BIZ_ID, TDWBackend
except ImportError as e:
    logger.info("环境中不存在 TDW 模块：{}".format(e))
    DEFAULT_TDW_BIZ_ID = 0
    TDWBackend = None


class UserPermission(BasePermissionMixin):
    def __init__(self, user_id):
        self.user_id = user_id
        self.user_backends = [IAMBackend(), RBACBackend()]

    @hanlder_auth_check_exception
    def check(self, action_id, object_id=None, bk_app_code="", raise_exception=False, display_detail=False):
        """
        校验主体是否具有操作对象的权限

        @param {Boolean} raise_exception 是否抛出异常
        @param {Boolean} display_detail 是否显示详细的鉴权信息
        """
        ret = self.interpret_check(
            action_id, object_id, bk_app_code, raise_exception=raise_exception, display_detail=display_detail
        )
        if ret is not None:
            return ret

        action_ids = [action_id]
        ActionConfig.get_parent_action_ids(action_id, action_ids)

        # 后续期望去掉父子 Action 减低系统复杂度
        for action_id in action_ids:
            if self.core_check(action_id, object_id):
                return True

        raise PermissionDeniedError()

    def core_check(self, action_id, object_id):
        """
        核心鉴权逻辑
        """
        if TDWBackend is not None:
            if action_id in TDWBackend.TDW_ACTION_IDS:
                return TDWBackend(self.user_id).check(action_id, object_id)

            # 默认标准化的结果表，走 TDW 鉴权
            if action_id in ["result_table.query_data"] and object_id.startswith(str(DEFAULT_TDW_BIZ_ID)):
                return TDWBackend(self.user_id).check_result_table_query_perm(object_id)

        user_backend = self.get_user_backend(action_id)
        return user_backend.check(self.user_id, action_id, object_id)

    def get_scopes(self, action_id):
        """
        返回主体有权限的属性范围
        """
        user_backend = self.get_user_backend(action_id)
        return user_backend.get_scopes(self.user_id, action_id)

    def get_scopes_all(self, action_id, show_admin_scopes=False, add_tdw=False):
        """
        获取主体有权限的实例列表
        @example
            action_id='result_table.query_data'

            [
                {'*': '*'},
                {'result_table_id': '1_xxx'},
                {'result_table_id': '2_xxx'}
            ]
        """
        user_backend = self.get_user_backend(action_id)
        scopes = user_backend.get_scopes(self.user_id, action_id)
        scopes = oFactory.init_object(action_id).get_related_scopes(
            scopes, action_id=action_id, show_admin_scopes=show_admin_scopes
        )
        # 需要考虑加上默认标准化的结果表
        if TDWBackend is not None:
            if action_id in ["result_table.query_data"] and add_tdw:
                result_table_ids = TDWBackend(self.user_id).list_resutl_tables_with_query_perm()
                scopes.extend([{"result_table_id": result_table_id} for result_table_id in result_table_ids])

        return scopes

    def get_user_backend(self, action_id):
        """
        选择当前合适的 backend 处理用户权限问题
        """
        for backend in self.user_backends:
            if backend.can_handle_action(action_id):
                return backend

        raise NoFunctionErr(f"No available backend for action({action_id})")

    @classmethod
    def list_authorized_role_users(cls, action_id, object_id, no_all_scope=False):
        """
        查询针对该功能，有权限角色用户
        """
        action = ActionConfig.objects.get(pk=action_id)
        role_policies = RBACBackend.get_action_role_policies(action, no_all_scope=no_all_scope)

        role_users = dict()

        # 非绑定实例的 action 只需存在角色关系即可
        if not action.has_instance:
            for rp in role_policies:
                _role_id = rp.role_id_id
                role_users[_role_id] = list(
                    UserRole.objects.filter(role_id=_role_id).values_list("user_id", flat=True).distinct()
                )
            return role_users

        checked_obj = oFactory.init_instance(action_id, object_id)

        # 先根据策略内的约束条件，过滤一部分不合规的策略
        target_role_policies = list()
        for rp in role_policies:
            # 不存在有效值则忽略该配置，比如空字符和 None 等值
            if not rp.scope_attr_key:
                target_role_policies.append(rp)
            else:
                _static_scope = {rp.scope_attr_key: rp.scope_attr_value}

                if checked_obj.is_in_scope(_static_scope, action_id):
                    target_role_policies.append(rp)

        for rp in target_role_policies:
            _role_id = rp.role_id_id
            role = RoleConfig.objects.get(role_id=_role_id)

            scope_object_ids = []
            scope_object_ids.extend(checked_obj.retrieve_scope_ids(role.object_class_id, None, is_direct=True))
            scope_object_ids.extend(checked_obj.retrieve_scope_ids(role.object_class_id, None, is_direct=False))
            for scope_object_id in scope_object_ids:
                user_ids = list(
                    UserRole.objects.filter(role_id=_role_id, scope_id=scope_object_id)
                    .values_list("user_id", flat=True)
                    .distinct()
                )
                _index = f"{_role_id}&{scope_object_id}"
                role_users[_index] = user_ids

        return role_users

    def handover(self, receiver):
        """
        权限移交
        """
        return RoleHandler.handover_roles(self.user_id, receiver)


class RolePermission:
    def __init__(self):
        pass

    @classmethod
    def has_operate_permission(cls, username, scope_id, role_id, raise_exception=False):
        """
        判断是用户(username)否有指定权限(scope_id)关于角色(role_id)的操作或审批权限

        @param {String} username
        @param {String} scope_id
        @param {String} role_id
        @param {Boolean} raise_exception
        """
        ret = username in cls.get_authorizers_users(role_id, scope_id)

        if not ret and raise_exception:
            raise PermissionDeniedError()

        return ret

    @classmethod
    def get_authorizers(cls, role_id):
        """
        查询角色的审批者角色

        @param {string} role_id: project.flow_member
        @returnExample
            [project.manager]
        """
        # 暂时需要设定结果数据管理员不可修改
        if role_id in ["result_table.manager"]:
            return []

        authorization_action_id = "{}.manage_auth".format(RoleConfig.objects.get(role_id=role_id).object_class_id)
        action = ActionConfig.objects.get(pk=authorization_action_id)
        role_policies = RBACBackend.get_action_role_policies(action, no_all_scope=True)

        return [rp.role_id_id for rp in role_policies]

    @classmethod
    def get_authorizers_users(cls, role_id, scope_id):
        """
        查询角色的审批者
        """
        # 暂时需要设定结果数据管理员不可修改
        if role_id in ["result_table.manager"]:
            return []

        authorization_action_id = "{}.manage_auth".format(RoleConfig.objects.get(role_id=role_id).object_class_id)

        role_users = UserPermission.list_authorized_role_users(authorization_action_id, scope_id, no_all_scope=True)
        user_ids = set()
        for ids in list(role_users.values()):
            user_ids.update(ids)
        return list(user_ids)

    @classmethod
    def clear(cls, user_id):
        return RoleHandler.clear_roles(user_id)
