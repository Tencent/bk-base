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
from unittest import mock

from auth.core.permission import RolePermission, UserPermission
from auth.tests.utils import BaseTestCase


class UserPermissionTestCase(BaseTestCase):
    @mock.patch("auth.core.permission.UserPermission.check")
    def test_interpret_check(self, patch_check):
        user01 = "user01"
        self.assertIs(UserPermission(user01).interpret_check("result_table.query_data", "1_xx", ""), None)
        self.assertTrue(UserPermission(user01).interpret_check("result_table.query_data", "1_system_cpu", ""))

        UserPermission(user01).interpret_check("result_table.manage_task", "591_test_rt", "")
        patch_check.assert_called_with("project.manage_flow", bk_app_code="", object_id="1", raise_exception=True)

        UserPermission(user01).interpret_check("result_table.manage_task", "591_test_rt3", "")
        patch_check.assert_called_with("raw_data.etl", bk_app_code="", object_id="4", raise_exception=True)

    def test_list_authorized_role_users(self):
        authotization_action_id = "project.manage_auth"
        object_id = "1"
        role_users = UserPermission.list_authorized_role_users(authotization_action_id, object_id, no_all_scope=True)
        self.assertEqual(role_users, {"project.manager&1": ["admin", "processor666"]})

        authotization_action_id = "bkdata.manage_auth"
        role_users = UserPermission.list_authorized_role_users(authotization_action_id, None, no_all_scope=True)
        self.assertEqual(role_users, {"bkdata.superuser": ["admin"]})


class RolePermissionTestCase(BaseTestCase):
    def test_get_authorizers_users(self):
        arr1 = RolePermission.get_authorizers_users("project.flow_member", 1)
        self.assertIn("admin", arr1)
        self.assertIn("processor666", arr1)

        arr2 = RolePermission.get_authorizers_users("dashboard.viewer", 1)
        self.assertIn("admin", arr2)
        self.assertIn("processor666", arr2)

        arr3 = RolePermission.get_authorizers_users("result_table.viewer", "591_test_rt")
        self.assertIn("processor666", arr3)

    def test_has_operate_permission(self):
        self.assertFalse(RolePermission.has_operate_permission("user01", 1, "project.flow_member"))
        self.assertTrue(RolePermission.has_operate_permission("admin", 1, "project.flow_member"))
