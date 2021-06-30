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
from __future__ import absolute_import, print_function, unicode_literals

from auth.models import AuthStatus, AuthUserRole


class RoleController(object):
    def __init__(self):
        pass

    def get_role_members_with_all_scope(self, session, role_id):
        """
        指定角色，获取所有角色对象的成员列表

        @returnExample
            {
                '1': ['user01', 'user02', 'user03'],
                '2': ['user01', 'user02', 'user03']
            }
        """
        data = (
            session.query(AuthUserRole)
            .filter(AuthUserRole.role_id == role_id)
            .filter(AuthUserRole.auth_status == AuthStatus.NORMAL.value)
        )

        role_members = dict()
        for d in data:
            _scope_id = d.scope_id
            if _scope_id not in role_members:
                role_members[_scope_id] = list()
            role_members[_scope_id].append(d.user_id)

        return role_members

    def get_role_members(self, session, role_id, scope_id):
        """
        获取角色对象的成员列表

        @returnExample
            ['user01', 'user02', 'user03']
        """
        data = (
            session.query(AuthUserRole)
            .filter(AuthUserRole.role_id == role_id)
            .filter(AuthUserRole.scope_id == str(scope_id))
            .filter(AuthUserRole.auth_status == AuthStatus.NORMAL.value)
        )

        return [d.user_id for d in data]

    def update_role_members(self, session, role_id, scope_id, members):
        """
        更新角色对象成员列表

        @return {Int} 大于 0 则表示有变更
        """
        current_members = self.get_role_members(session, role_id, scope_id)

        old_members = [mem for mem in current_members if mem not in members]
        new_members = [mem for mem in members if mem not in current_members]

        if len(old_members) > 0:
            query = (
                session.query(AuthUserRole)
                .filter(AuthUserRole.role_id == role_id)
                .filter(AuthUserRole.scope_id == str(scope_id))
                .filter(AuthUserRole.user_id.in_(old_members))
            )

            query.delete(synchronize_session=False)

        if len(new_members) > 0:
            users = [
                AuthUserRole(
                    role_id=role_id,
                    scope_id=scope_id,
                    user_id=m,
                    created_by="admin",
                    updated_by="admin",
                )
                for m in new_members
            ]
            session.add_all(users)

        return len(old_members) + len(new_members)
