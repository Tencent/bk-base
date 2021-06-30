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

from auth.core.process import DefaultProcessMixin, RoleProcessMixin
from auth.models import UserRole
from auth.models.outer_models import ResultTable
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse
from common.local import set_local_param


class ProcessTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_common_process(self):
        UserRole.objects.create(user_id="user01", role_id="project.manager", scope_id="111")
        perm = {"action": "project.manage", "scope": {"project_id": 111}}
        process = DefaultProcessMixin.get_process(perm)
        self.assertEqual(process["process_length"], 1)
        self.assertEqual(process["states"][0]["processors"], ["user01"])

    def test_public_rt_query_process(self):
        result_table_id = "591_test_process_01"
        ResultTable.objects.create(
            result_table_id=result_table_id,
            bk_biz_id="591",
            project_id="111111",
            sensitivity="public",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )
        UserRole.objects.create(user_id="user01", role_id="result_table.manager", scope_id=result_table_id)

        set_local_param("bk_username", "user01")
        perm = {"action": "result_table.query_data", "scope": {"result_table_id": result_table_id}}
        process = DefaultProcessMixin.get_process(perm)

        self.assertEqual(process["process_length"], 1)
        self.assertEqual(process["states"][0]["processors"], ["user01"])

    def test_private_rt_query_process(self):
        result_table_id = "591_test_process_01"
        ResultTable.objects.create(
            result_table_id=result_table_id,
            bk_biz_id="591",
            project_id="111111",
            sensitivity="private",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )
        UserRole.objects.create(user_id="user01", role_id="result_table.manager", scope_id=result_table_id)

        perm = {"action": "result_table.query_data", "scope": {"result_table_id": result_table_id}}
        process = DefaultProcessMixin.get_process(perm)

        self.assertEqual(process["process_length"], 1)
        self.assertEqual(process["states"][0]["processors"], ["processor666", "user01"])

    @mock.patch("auth.api.TofApi.get_staff_info")
    @mock.patch("auth.api.TofApi.get_staff_direct_leader")
    def test_confidential_rt_query_process(self, get_staff_direct_leader, patch_get_staff_info):
        # 数据准备
        result_table_id = "591_test_process_01"
        ResultTable.objects.create(
            result_table_id=result_table_id,
            bk_biz_id="591",
            project_id="111111",
            sensitivity="confidential",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )
        UserRole.objects.create(user_id="user01", role_id="result_table.manager", scope_id=result_table_id)
        UserRole.objects.create(user_id="user03", role_id="biz.leader", scope_id="591")
        perm = {"action": "result_table.query_data", "scope": {"result_table_id": result_table_id}}

        set_local_param("bk_username", "user00")
        set_local_param("auth_info", {"bk_ticket": "--"})

        # 正常情况， 非 leader 提交单据
        patch_get_staff_info.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "LoginName": "user00",
                    "StatusName": "在职",
                    "EnglishName": "user00",
                    "StatusId": "1",
                    "OfficialId": "8",
                    "TypeName": "正式",
                    "OfficialName": "普通员工",
                    "RTX": "user00",
                },
            }
        )
        get_staff_direct_leader.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "LoginName": "user02",
                    "StatusName": "在职",
                    "EnglishName": "user02",
                    "StatusId": "1",
                    "OfficialId": "6",
                    "TypeName": "正式",
                    "OfficialName": "TeamLeader",
                    "RTX": "user02",
                },
            }
        )

        process = DefaultProcessMixin.get_process(perm)
        self.assertEqual(process["process_length"], 2)
        self.assertEqual(process["states"][0]["processors"], ["user02"])
        self.assertEqual(process["states"][1]["processors"], ["user01", "user03"])

        # 正常情况，leader 提交单据
        patch_get_staff_info.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "LoginName": "user00",
                    "StatusName": "在职",
                    "EnglishName": "user00",
                    "StatusId": "1",
                    "OfficialId": "6",
                    "TypeName": "正式",
                    "OfficialName": "普通员工",
                    "RTX": "user00",
                },
            }
        )
        process = DefaultProcessMixin.get_process(perm)
        self.assertEqual(process["process_length"], 2)
        self.assertEqual(process["states"][0]["processors"], ["user00"])
        self.assertEqual(process["states"][1]["processors"], ["user01", "user03"])

        # 正常情况，申请人即数据管理员
        set_local_param("bk_username", "user01")
        process = DefaultProcessMixin.get_process(perm)
        self.assertEqual(process["process_length"], 1)
        self.assertEqual(process["states"][0]["processors"], ["user01", "user03"])

    def test_role_process(self):
        """
        测试角色申请流程
        """
        UserRole.objects.create(user_id="user01", role_id="biz.manager", scope_id="591")

        perm = {"action": "role.add", "scope_id": 1, "role_id": "raw_data.manager"}

        process = RoleProcessMixin.get_process(perm)
        self.assertEqual(process["process_length"], 1)
        self.assertEqual(sorted(process["states"][0]["processors"]), ["processor666", "user01"])
