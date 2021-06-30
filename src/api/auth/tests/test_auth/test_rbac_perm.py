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


from auth.core.default.backend import RBACBackend
from auth.models.base_models import RoleConfig, RolePolicyInfo
from auth.tests.utils import BaseTestCase


class RBACPermTestCase(BaseTestCase):
    def test_clean_scopes(self):
        scopes = [{"project_id": 1}, {"project_id": 1}]
        self.assertEqual(len(RBACBackend().clean_scopes("result_table.query_data", scopes)), 1)

    def test_compress_roles(self):
        role_policies = [
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="bkdata.user"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="biz.manager"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="project.flow_member"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="result_table.viewer"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="bkdata.superuser"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="result_table.manager"))[0],
            RolePolicyInfo.objects.filter(role_id=RoleConfig.objects.get(pk="raw_data.manager"))[0],
        ]
        compress_roles_groups = RBACBackend._compress_roles(role_policies)
        self.assertEqual(compress_roles_groups[0], ("bkdata", ["bkdata.user", "bkdata.superuser"]))
        self.assertEqual(compress_roles_groups[1], ("biz", ["biz.manager"]))
        self.assertEqual(compress_roles_groups[2], ("project", ["project.flow_member"]))
        self.assertEqual(compress_roles_groups[3], ("raw_data", ["raw_data.manager"]))
        self.assertEqual(compress_roles_groups[4], ("result_table", ["result_table.viewer", "result_table.manager"]))
