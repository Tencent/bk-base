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
from auth.core.backends import BaseUserBackend
from auth.handlers.object_classes import oFactory
from auth.models.auth_models import UserRole
from auth.models.base_models import (
    ActionConfig,
    ObjectConfig,
    RoleConfig,
    RolePolicyInfo,
)
from auth.utils.cmp import cmp_to_key
from common.log import logger
from django.db.models import Q


class RBACBackend(BaseUserBackend):
    """
    平台默认对于用户的鉴权后端，基于角色策略的关系进行鉴权
    """

    def check(self, user_id, action_id, object_id=None, bk_app_code=""):
        """
        核心鉴权逻辑，检查是否存在符合要求的鉴权策略
        """
        action = ActionConfig.objects.get(pk=action_id)
        role_policies = self.get_action_role_policies(action)

        # 非绑定实例的 action 只需存在角色关系即可
        if not action.has_instance:
            role_ids = [rp.role_id_id for rp in role_policies]
            return UserRole.objects.filter(user_id__in=[user_id, "*"]).filter(role_id__in=role_ids).exists()

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

        if len(target_role_policies) == 0:
            return False

        # 整理策略比对范围的前后次序，验证是否有对应用户与角色关系存在
        compressed_roles = self._compress_roles(target_role_policies)

        # 待检索的目标范围，直接关联鉴权 + 间接关联鉴权
        for direct_type in [True, False]:
            for scope_object_class, scope_role_ids in compressed_roles:
                scope_object_ids = checked_obj.retrieve_scope_ids(scope_object_class, action_id, is_direct=direct_type)
                if len(scope_object_ids) == 0:
                    continue

                filters = dict()
                if len(scope_object_ids) > 1:
                    filters["scope_id__in"] = scope_object_ids
                else:
                    filters["scope_id"] = scope_object_ids[0]

                if len(scope_role_ids) > 1:
                    filters["role_id__in"] = scope_role_ids
                else:
                    filters["role_id"] = scope_role_ids[0]

                logger.debug(
                    "[CoreCheck] {user_id}:{action_id}:{object_id}, "
                    "Filters={filters}".format(
                        user_id=user_id, action_id=action_id, object_id=object_id, filters=filters
                    )
                )

                if UserRole.objects.filter(user_id__in=[user_id, "*"]).filter(**filters).exists():
                    return True

        return False

    def get_scopes(self, user_id, action_id):
        action = ActionConfig.objects.get(pk=action_id)
        role_policies = self.get_action_role_policies(action)

        # 非绑定实例的 action 不存在范围的需求
        if not action.has_instance:
            return []

        # 一个角色会有多条策略
        map_role_policies = dict()
        for rp in role_policies:
            _role_id = rp.role_id_id
            if _role_id not in map_role_policies:
                map_role_policies[_role_id] = []
            map_role_policies[_role_id].append(rp)

        # 获取人员->角色的关系
        user_roles = UserRole.objects.filter(Q(user_id=user_id) | Q(user_id="*")).filter(
            role_id__in=list(map_role_policies.keys())
        )
        # 生成 Policy
        raw_scopes = []
        for relation in user_roles:
            raw_scopes.extend(self.build_raw_scopes(action_id, map_role_policies, relation))
        return self.clean_scopes(action_id, raw_scopes)

    def build_raw_scopes(self, action_id, map_role_policies, user_role_relation):
        """
        根据角色策略 + 用户角色关系，组合生成范围
        @param {auth.models.auth_models.RolePolicyInfo} role_policy
        @param {auth.models.auth_models.UserRole} user_role_relation

        @return {[Dict]} 原始 policy 结构，非对象
        """
        role_id = user_role_relation.role_id
        role_policies = map_role_policies[role_id]
        raw_scopes = []

        for rp in role_policies:
            _scope = dict()

            # 角色关系自动生成的范围
            _scope_object_class = RoleConfig.objects.get(pk=role_id).object_class_id
            _relation_key = ObjectConfig.objects.get(pk=_scope_object_class).scope_id_key
            _relation_value = user_role_relation.scope_id

            # 角色策略是否带上属性限定范围，如果有，则需要加上
            _policy_key = rp.scope_attr_key
            _policy_value = rp.scope_attr_value

            if _policy_key == "":
                _scope[_relation_key] = _relation_value
            elif self._in(_relation_key, _policy_key) and self._in(_relation_value, _policy_value):
                _scope[_policy_key] = _policy_value
            elif self._in(_relation_key, _policy_key) and not self._in(_relation_value, _policy_value):
                # 无法得到交集，不匹配，空范围
                pass
            else:
                _scope[_relation_key] = _relation_value
                _scope[_policy_key] = _policy_value

            raw_scopes.append(_scope)

        return raw_scopes

    @staticmethod
    def get_action_role_policies(action, no_all_scope=False):
        """
        通过 action 获取相关角色策略

        @param {Boolean} no_all_scope 是否考虑全量授权的策略，等同步是否需要精准匹配
        """
        if no_all_scope:
            return RolePolicyInfo.objects.filter(action_id=action.action_id)

        # 需要关注 action_id, object_class 存在全量匹配的情况
        role_policy_info = []
        role_policy_info.extend(RolePolicyInfo.objects.filter(action_id=action.action_id))
        role_policy_info.extend(RolePolicyInfo.objects.filter(action_id="*", object_class=action.object_class_id))
        role_policy_info.extend(RolePolicyInfo.objects.filter(action_id="*", object_class="*"))
        return role_policy_info

    @staticmethod
    def clean_scopes(action_id, raw_scopes):
        """
        清洗策略列表
        """
        # 移除相同重复的属性范围
        scopes_set = set()
        for scope in raw_scopes:
            scope_tuple = tuple(oFactory.init_object(action_id).clean_scopes(scope).items())
            scopes_set.add(scope_tuple)
        return [dict(_tuple) for _tuple in scopes_set]

    @staticmethod
    def _compress_roles(role_policies):
        """
        根据角色所属范围 object_class 一致，归为一组

        @returnExample
            [
                ("bkdata",       ["bkdata.superuser", "bkdata.user"]),
                ("result_table", ["result_table.manager", "result_table.viewer"]),
                ("project",      ["project.manager", "project.flow_member"]),
            ]
        """
        mapping = dict()
        for rp in role_policies:
            role = RoleConfig.objects.get(pk=rp.role_id_id)
            if role.object_class_id not in mapping:
                mapping[role.object_class_id] = [rp.role_id_id]
            else:
                mapping[role.object_class_id].append(rp.role_id_id)

        # 按照范围覆盖大小进行排序，优先从大范围开始匹配
        def _get_priority(object_class_id):
            if object_class_id == "bkdata":
                return 100
            elif object_class_id == "biz":
                return 90
            elif object_class_id == "project":
                return 80
            elif object_class_id == "raw_data":
                return 70
            else:
                return 50

        return sorted(list(mapping.items()), key=cmp_to_key(lambda a, b: _get_priority(b[0]) - _get_priority(a[0])))

    @staticmethod
    def _in(a, b):
        """
        判断 b 在不在 a 中，* 表示全部
        """
        if a == "*":
            return True
        if a == b:
            return True
        return False
