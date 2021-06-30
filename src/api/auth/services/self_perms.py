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
from auth.core.process import RoleProcessMixin
from auth.exceptions import PermissionDeniedError


def check_role_manage_perm(user_id, role_id, scope_id, raise_exception=True):
    """
    检查用户是否对角色有管理权限，管理权限主要指对角色成员是否有变更权限

    @param {String} role_id 角色ID
    @param {String} scope_id 角色所属对象值
    """
    ret = user_id in RoleProcessMixin.get_core_processers(role_id, scope_id)

    if raise_exception and not ret:
        raise PermissionDeniedError()

    return True


def check_role_manage_perm_batch(user_id, scoped_roles, raise_exception=True):
    """
    批量检查用户是否对角色有管理权限

    @paramExcample scoped_roles
        [
            {'role_id': 'project.manage', 'scope_id': 1},
            {'role_id': 'project.flow_member', 'scope_id': 1}
        ]
    """
    ret = all(
        check_role_manage_perm(user_id, sr["role_id"], sr["scope_id"], raise_exception=raise_exception)
        for sr in scoped_roles
    )

    if raise_exception and not ret:
        raise PermissionDeniedError()

    return True
