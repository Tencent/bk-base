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
import time

import attr
from auth.bkiam.backend import IAMBackend
from auth.exceptions import BKIAMNotSupportAttrAutho, BKIAMSyncAuthoErr
from auth.handlers.resource.manager import AuthResourceManager
from auth.models.base_models import ActionConfig, RoleConfig, RolePolicyInfo
from common.log import logger
from iam import Action, Subject
from iam.auth.models import (
    ApiBatchAuthRequest,
    ApiBatchAuthResourceWithId,
    ApiBatchAuthResourceWithPath,
)


class RoleSync:
    GRANT = "grant"
    REVOKE = "revoke"

    def __init__(self):
        self.backend = IAMBackend()
        self._iam = self.backend._iam
        self.system = self.backend.system

    def sync(self, user_id, role_id, object_id, operate):
        """
        与 IAM 同步权限，operate 可以为 grant 或者 revoke
        """
        role = RoleConfig.objects.get(pk=role_id)
        if not role.to_iam:
            return

        policies = self.get_related_policies_with_role(role_id)
        role_resource = AuthResourceManager.init(role.object_class_id).get(object_id, raise_exception=True)

        instance_policies, path_polices = self.classify_policies(policies, role_id, role_resource.resource_type)

        instance_action_ids = [p.action_id for p in instance_policies]
        path_action_ids = [p.action_id for p in path_polices]
        args = [user_id, role_id, instance_action_ids, path_action_ids, operate]

        start_time = time.time()
        try:
            self.sync_one_resource_more_inst_actions(user_id, role_resource, instance_policies, operate)
            self.sync_one_resource_more_path_actions(user_id, role_resource, path_polices, operate)
        except Exception as err:
            logger.exception(f"[IAM RoleSync] Fail to sync, args={args}, error={err}")
            raise BKIAMSyncAuthoErr()
        finally:
            end_time = time.time()
            logger.info("[IAM RoleSync] Cost {} seconds, args={}".format(end_time - start_time, args))

        logger.info(f"[IAM RoleSync] Succeed to sync, args={args}")

    def classify_policies(self, policies, role_id, resource_type):
        """
        将策略分类，主要分为实例策略和路劲策略
        """
        instance_policies = list()
        path_polices = list()

        for p in policies:
            if not p.has_instance:
                continue

            if not p.to_iam:
                continue

            if p.scope_attr_key:
                raise BKIAMNotSupportAttrAutho(
                    ("Now cannot sync attr_authentication to iam, " "role_id={}, policy={}").format(role_id, p)
                )

            if resource_type == p.object_class:
                instance_policies.append(p)
            else:
                path_polices.append(p)

        return instance_policies, path_polices

    def sync_one_resource_more_inst_actions(self, user_id, role_resource, policies, operate):
        """
        同步一个资源，多个实例动作
        """
        action_ids = [p.action_id for p in policies]

        resources = [
            ApiBatchAuthResourceWithId(
                self.system, role_resource.resource_type, [{"id": role_resource.id, "name": role_resource.display_name}]
            )
        ]
        request = self._make_batch_request_with_resources(user_id, action_ids, resources, operate)
        self._iam.batch_grant_or_revoke_instance_permission(request, bk_username=user_id)

    def sync_one_resource_more_path_actions(self, user_id, role_resource, policies, operate):
        """
        同步一个资源，多个路径动作
        """
        # 生成资源路径
        path = [{"type": role_resource.resource_type, "id": role_resource.id, "name": role_resource.display_name}]

        # 按照 object_class 对 action 分类，仅相同资源类型的 actions 可以合并授权
        actions_mapping = dict()
        for p in policies:
            actions_mapping.setdefault(p.object_class, [])
            actions_mapping[p.object_class].append(p.action_id)

        for action_resource_type, action_ids in list(actions_mapping.items()):
            resources = [ApiBatchAuthResourceWithPath(self.system, action_resource_type, [path])]
            request = self._make_batch_request_with_resources(user_id, action_ids, resources, operate)
            self._iam.batch_grant_or_revoke_path_permission(request, bk_username=user_id)

    def grant(self, user_id, role_id, object_id):
        """
        添加角色授权，同步 IAM
        """
        self.sync(user_id, role_id, object_id, operate=self.GRANT)

    def revoke(self, user_id, role_id, object_id):
        """
        取消角色授权，同步 IAM
        """
        self.sync(user_id, role_id, object_id, operate=self.REVOKE)

    def _make_batch_request_with_resources(self, user_id, action_ids, resources, operate):
        request = ApiBatchAuthRequest(
            self.system,
            Subject("user", user_id),
            [Action(action_id) for action_id in action_ids],
            resources,
            operate=operate,
        )
        return request

    def get_related_policies_with_role(self, role_id, action_id=None, to_iam=True):
        """
        获取与角色关联的操作

        @returnExample  role_id=project.manager 样例
            [
                {
                    'action_id': 'flow.manage',
                    'object_class': 'flow',
                    'scope_attr_key': null,
                    'scope_attr_value': null
                },
                {
                    'action_id': 'result_table.query_data',
                    'object_class': 'result_table',
                    'scope_attr_key': 'sensitivity',
                    'scope_attr_value': 'private'
                },
                {
                    'action_id': 'project.manage',
                    'object_class': 'project',
                    'scope_attr_key': null,
                    'scope_attr_value': null
                }
            ]

        """
        if action_id is None:
            raw_policies = RolePolicyInfo.objects.filter(role_id_id=role_id)
        else:
            raw_policies = RolePolicyInfo.objects.filter(role_id_id=role_id, action_id_id=action_id)
        cleaned_policies = list()
        for p in raw_policies:
            if p.action_id_id == "*":
                actions = ActionConfig.objects.filter(object_class=p.object_class_id, to_iam=to_iam)
                policies = [
                    ActionPolicy(
                        action_id=self.backend.to_iam_action(a.action_id),
                        has_instance=a.has_instance,
                        to_iam=a.to_iam,
                        object_class=p.object_class_id,
                        scope_attr_key=p.scope_attr_key,
                        scope_attr_value=p.scope_attr_value,
                    )
                    for a in actions
                ]
            else:
                action = ActionConfig.objects.get(pk=p.action_id_id)
                if action.to_iam == to_iam:
                    policies = [
                        ActionPolicy(
                            action_id=self.backend.to_iam_action(action.action_id),
                            has_instance=action.has_instance,
                            to_iam=action.to_iam,
                            object_class=p.object_class_id,
                            scope_attr_key=p.scope_attr_key,
                            scope_attr_value=p.scope_attr_value,
                        )
                    ]
                else:
                    policies = []
            cleaned_policies.extend(policies)

        # 去重
        cleaned_policies = list({p.action_id: p for p in cleaned_policies}.values())

        return cleaned_policies


@attr.s
class ActionPolicy:
    action_id = attr.ib(type=str)
    has_instance = attr.ib(type=bool)
    to_iam = attr.ib(type=bool)
    object_class = attr.ib(type=str)
    scope_attr_key = attr.ib(type=str)
    scope_attr_value = attr.ib(type=str)
