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
from auth.models.base_models import ActionConfig, ObjectConfig, RoleConfig
from auth.tests.utils import BaseTestCase


class BaseTest(BaseTestCase):
    def test_proxy(self):

        self.assertEqual(RoleConfig.objects.get(role_id="biz.manager").role_id, "biz.manager")
        self.assertEqual(ObjectConfig.objects.get(pk="biz").object_class, "biz")
        self.assertEqual(ActionConfig.objects.get(pk="result_table.query_data").action_id, "result_table.query_data")

        # parent_action_ids = []
        # ActionConfig.get_parent_action_ids('result_table.retrieve', parent_action_ids)
        # self.assertEqual(parent_action_ids, ['result_table.query_data'])

    def test_model(self):
        action = ActionConfig.objects.get(pk="project.update")
        self.assertEqual(action.action_id, "project.update")
        self.assertEqual(action.has_instance, True)
        self.assertEqual(action.object_class_id, "project")
        self.assertEqual(action.object_class.object_class, "project")

        actions = ActionConfig.objects.filter(object_class_id="project")
        self.assertGreater(len(actions), 10)

        for a in actions:
            self.assertEqual(a.object_class_id, "project")


class ObjectConfigTestCase(BaseTestCase):
    def test_get_object_config_map(self):
        self.assertIsInstance(ObjectConfig.get_object_config_map(), dict)

    def test_get_object_scope_key_map(self):
        self.assertIsInstance(ObjectConfig.get_object_scope_key_map(), dict)

    def test_get_object_scope_action(self):
        self.assertIsInstance(ObjectConfig.get_object_scope_action(), list)


class ActionConfigTestCase(BaseTestCase):
    def test_get_action_name_map(self):
        self.assertIsInstance(ActionConfig.get_action_name_map(), dict)

    def test_get_parent_action_ids(self):
        parent_action_ids = []
        ActionConfig.get_parent_action_ids("raw_data.etl_retrieve", parent_action_ids)
        self.assertListEqual(parent_action_ids, ["raw_data.etl"])


class RoleConfigTestCase(BaseTestCase):
    def test_get_role_config_map(self):
        self.assertIsInstance(RoleConfig.get_role_config_map(), dict)

    def test_get_role_object_map(self):
        self.assertIsInstance(RoleConfig.get_role_object_map(), dict)
