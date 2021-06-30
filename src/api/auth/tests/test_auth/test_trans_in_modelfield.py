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
from common.local import set_local_param


class TestTrans(BaseTestCase):
    def setUp(self):
        set_local_param("language", "en")

    def test_role_info(self):
        roles = RoleConfig.objects.all()
        roles_mapping = {role.role_id: role for role in roles}

        self.assertEqual("SystemAdministrator", roles_mapping["bkdata.superuser"].role_name)
        self.assertEqual("BusinessMaintainer", roles_mapping["biz.manager"].role_name)

        self.assertEqual(
            "With query permissions for all private business data", roles_mapping["biz.productor"].description
        )
        self.assertEqual("Have data query permission", roles_mapping["raw_data.viewer"].description)

    def test_object_info(self):
        object_cls_arr = ObjectConfig.objects.all()
        object_cls_mapping = {obj_cls.object_class: obj_cls for obj_cls in object_cls_arr}

        self.assertEqual("DataSystem", object_cls_mapping["bkdata"].object_name)
        self.assertEqual("Project", object_cls_mapping["project"].object_name)

    def test_action_info(self):

        actions = ActionConfig.objects.all()
        actions_mapping = {action.action_id: action for action in actions}

        self.assertEqual("ResultTable DataQuery", actions_mapping["result_table.query_data"].action_name)
        self.assertEqual("RawData ETL", actions_mapping["raw_data.etl"].action_name)
