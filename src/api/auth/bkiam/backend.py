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


from itertools import product

import attr
from auth.bkiam import (
    IAM_API_HOST,
    IAM_API_PAAS_HOST,
    IAM_REGISTERED_APP_CODE,
    IAM_REGISTERED_APP_SECRET,
    IAM_REGISTERED_SYSTEM,
    SUPPORT_IAM,
)
from auth.bkiam.converter import AuthConverter
from auth.core.backends import BaseUserBackend
from auth.handlers.object_classes import oFactory
from auth.handlers.resource.manager import AuthResourceManager, ResourceIdFilter
from auth.models.auth_models import UserRole
from auth.models.base_models import ActionConfig
from django.db.models import Q
from iam import IAM, Action, Request, Resource, Subject

iam = IAM(IAM_REGISTERED_APP_CODE, IAM_REGISTERED_APP_SECRET, IAM_API_HOST, IAM_API_PAAS_HOST)

# 操作映射，其中 key 为本地的action_id, value 为权限中心的 action_id
action_maps = {
    "raw_data.retrieve_sensitive_information": "raw_data-retrieve_sensitive_info",
    "result_table.retrieve_sensitive_information": "result_table-retrieve_sens_info",
    "biz.job_access": "biz-manage",
    "biz.common_access": "biz-manage",
}


class IAMBackend(BaseUserBackend):
    SUPPORT_IAM = SUPPORT_IAM

    def __init__(self):
        self._iam = iam
        self.system = IAM_REGISTERED_SYSTEM

    @staticmethod
    def to_iam_resource(auth_resource):
        """
        权限模块的内部资源装换为 IAM 资源对象
        """
        content = attr.asdict(auth_resource)
        content["_bk_iam_path_"] = ["/{},{}/".format(parent[0], parent[1].id) for parent in auth_resource.parents]

        return Resource(IAM_REGISTERED_SYSTEM, auth_resource.resource_type, str(auth_resource.id), content)

    @staticmethod
    def to_iam_resource_with_indirect_path(auth_resource, action_id):
        """
        权限模块的内部资源装换为 IAM 资源对象
        """
        indirect_parent_resources = auth_resource.indirect_parent_resources(action_id)
        if len(indirect_parent_resources) == 0:
            return None

        content = attr.asdict(auth_resource)
        content["_bk_iam_path_"] = [f"/{parent.resource_type},{parent.id}/" for parent in indirect_parent_resources]

        return Resource(IAM_REGISTERED_SYSTEM, auth_resource.resource_type, str(auth_resource.id), content)

    @staticmethod
    def to_iam_action(action_id):
        """
        IAM 关于动作的定义有格式要求，不允许有点号，在两个系统间需要做装换
        """
        if action_id in action_maps:
            return action_maps[action_id]
        return action_id.replace(".", "-")

    def can_handle_action(self, action_id):
        action = ActionConfig.objects.get(pk=action_id)
        if not self.SUPPORT_IAM:
            return False

        return action.to_iam or action.action_id in list(action_maps.keys())

    def check(self, user_id, action_id, object_id=None, bk_app_code=""):
        action = ActionConfig.objects.get(pk=action_id)

        # 目前非绑定实例的动作均要通过，期望可以通过 IAM 配置的方式来实现，主要是 project.create, raw_data.create 等新增操作
        if not action.has_instance:
            return True

        # 这些默认有权限的动作，应该在 IAM 中进行配置
        if action.action_id in ["result_table.retrieve", "raw_data.retrieve"]:
            return True

        # Note: 由于数据管理员未同步至 IAM，这里需要补充数据管理员的权限
        if action.object_class_id == "result_table":
            if UserRole.objects.filter(user_id=user_id, role_id="result_table.manager", scope_id=object_id).exists():
                return True

        iam_resource_type = action.object_class_id
        iam_action_id = self.to_iam_action(action_id)

        if not action.has_instance:
            request = self._make_request_without_resources(user_id, iam_action_id)
            return self._iam.is_allowed(request)

        auth_resource = AuthResourceManager.init(iam_resource_type).get(object_id, raise_exception=True)

        request = self._make_request_with_resources(user_id, iam_action_id, [self.to_iam_resource(auth_resource)])
        ret = self._iam.is_allowed(request)

        # 如果构建的直接资源权限通过不了，再考虑间接的资源权限，写法上还可以考虑进一步优化
        if not ret:
            iam_resource = self.to_iam_resource_with_indirect_path(auth_resource, action_id)
            if iam_resource is not None:
                request = self._make_request_with_resources(user_id, iam_action_id, [iam_resource])
                ret = self._iam.is_allowed(request)

        return ret

    def get_scopes(self, user_id, action_id):
        """
        获取用户有权限的范围
        """
        action = ActionConfig.objects.get(pk=action_id)

        iam_resource_type = action.object_class_id

        if not action.has_instance:
            return []

        manager = AuthResourceManager.init(iam_resource_type)

        scopes = []
        filter_groups = self.make_filters(user_id, action_id)

        object_manager = oFactory.init_object(action_id)

        for group in filter_groups:

            arr_ = []
            for filter_ in group:
                arr_.append(manager.to_scopes(filter_))

            for item in product(*arr_):
                scope = dict()
                for i in item:
                    scope.update(i)

                scope = object_manager.clean_scopes(scope)
                scopes.append(scope)

        # Note: 由于数据管理员未同步至 IAM，这里需要补充数据管理员的权限
        if action.object_class_id == "result_table":
            scope_ids = UserRole.objects.filter(user_id=user_id, role_id="result_table.manager").values_list(
                "scope_id", flat=True
            )
            for _id in scope_ids:
                scopes.append({"result_table_id": _id})

        return scopes

    def make_filters(self, user_id, action_id):
        """
        生成范围过滤器

        @example 一般授权返回样例
            [
                [ResourceFilter(resource_type=u'biz', resource_id=u'873'),
                 ResourceAttrFilter(func=u'eq', key=u'sensitivity', value=u'confidential')],

                [ResourceIdFilter(func=u'in', value=[u'101328', u'101386'])],

                [ResourceFilter(resource_type=u'biz', resource_id=u'100413')],

                [ResourceAttrFilter(func=u'eq', key=u'bk_biz_id', value=u'639'),
                 ResourceAttrFilter(func=u'eq', key=u'sensitivity', value=u'private')],

                [ResourceAttrFilter(func=u'eq', key=u'sensitivity', value=u'public')]
            ]

        @example 全局授权返回样例
            [
                [GlobalFilter()]
            ]

        """
        iam_action_id = self.to_iam_action(action_id)
        request = self._make_request_without_resources(user_id, iam_action_id)

        polices = self._iam._do_policy_query(request)
        converter = AuthConverter()
        root = converter.convert(polices)

        if root is None:
            return []

        filter_groups = []

        def to_filter(root, item):
            if isinstance(item, tuple):
                return item[1]

            raise Exception(f"Here must be tuple having filter object, {item}, root={root}")

        for c in root.children:
            if isinstance(c, Q) and c.connector == "AND":
                filter_group = [to_filter(root, sc) for sc in c.children]

                # 如果过滤器分组中有 ResourceIdFilter 的话，则不考虑其他过滤器
                for filter_ in filter_group:
                    if isinstance(filter_, ResourceIdFilter):
                        filter_group = [filter_]
                        break
            else:
                filter_group = [to_filter(root, c)]

            filter_groups.append(filter_group)

        # 如果存在 GlobalFilter 全局范围授权，忽略其他细粒度的授权范围
        # for filter_group in filter_groups:
        #     if len(filter_group) == 1 and isinstance(filter_group[0], GlobalFilter):
        #         return [[GlobalFilter()]]

        return filter_groups

    def get_token(self):
        """
        平台内置在 IAM 上的 basic-auth 的授权码
        """
        return self._iam.get_token(self.system)

    def _make_request_with_resources(self, user_id, action_id, resources):
        request = Request(self.system, Subject("user", user_id), Action(action_id), resources, None)
        return request

    def _make_request_without_resources(self, user_id, action_id):
        request = Request(self.system, Subject("user", user_id), Action(action_id), None, None)
        return request

    @property
    def iam(self):
        return self._iam
