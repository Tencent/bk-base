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


from auth.core.permission import UserPermission
from auth.models import UserRole
from auth.models.base_models import RoleConfig
from auth.tests.utils import BaseTestCase


class ActionTestCase(BaseTestCase):
    multi_db = True

    @classmethod
    def setUpTestData(cls):
        cls.USER1 = "user01"
        cls.USER2 = "user02"
        cls.ROLE_PROJECT_MANAGER = RoleConfig.objects.get(pk="project.manager")

        UserRole.objects.create(user_id=cls.USER1, role_id=cls.ROLE_PROJECT_MANAGER.role_id, scope_id=1)

    def test_actions(self):
        self.assertTrue(UserPermission(user_id=self.USER1).check("project.update", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("flow.execute", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt"))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.update", "591_test_rt"))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.manage_task", "591_test_rt"))

        self.assertFalse(UserPermission(user_id=self.USER2).check("project.update", 1))
        self.assertFalse(UserPermission(user_id=self.USER2).check("flow.execute", 1))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.update", "591_test_rt"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.manage_task", "591_test_rt"))
