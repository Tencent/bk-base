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
import json
import time
from unittest import mock

from django.conf import settings

from auth.bkiam.backend import IAMBackend
from auth.core.default.backend import RBACBackend
from auth.core.permission import RolePermission, UserPermission
from auth.exceptions import PermissionDeniedError
from auth.models.audit_models import AuthAuditRecord
from auth.models.auth_models import ProjectData, UserRole
from auth.models.base_models import RoleConfig, RolePolicyInfo
from auth.models.others import OperationConfig
from auth.models.outer_models import AlgoModel, Algorithm, AlgoSampleSet, ResultTable
from auth.tests.utils import BaseTestCase


class UserPermTestCase(BaseTestCase):
    multi_db = True

    @classmethod
    def setUpTestData(cls):
        cls.USER1 = "user01"
        cls.USER2 = "user02"

        cls.ROLE_PROJECT_MANAGER = RoleConfig.objects.get(pk="project.manager")
        cls.ROLE_FLOW_MANAGER = RoleConfig.objects.get(pk="project.flow_member")
        cls.ROLE_BIZ_MANAGER = RoleConfig.objects.get(pk="biz.manager")
        cls.ROLE_SUPERUSER = RoleConfig.objects.get(pk="bkdata.superuser")
        # 2019-5-22 add
        cls.ROLE_FLOW_DEVELOPER = RoleConfig.objects.get(pk="project.flow_developer")
        # 2019-5-31
        cls.ROLE_PROJECT_DASHBOARD_VIEWER = RoleConfig.objects.get(pk="project.dashboard_viewer")
        cls.ROLE_DASHBOARD_VIEWER = RoleConfig.objects.get(pk="dashboard.viewer")
        # 2019-7-12
        cls.ROLE_FUNCTION_MANAGER = RoleConfig.objects.get(pk="function.manager")
        cls.ROLE_FUNCTION_DEVELOPER = RoleConfig.objects.get(pk="function.developer")

        cls.DATA_TOKEN_MANAGER = RoleConfig.objects.get(pk="data_token.manager")
        cls.RESOURCE_GROUP_MANAGER = RoleConfig.objects.get(pk="resource_group.manager")

    def setUp(self):
        super().setUp()
        IAMBackend.SUPPORT_IAM = False

    def test_2_project_role(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=2)

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("flow.execute", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt"))

        self.assertFalse(UserPermission(user_id=self.USER2).check("project.update", 1))
        self.assertFalse(UserPermission(user_id=self.USER2).check("flow.execute", 1))

        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt"))

    def test_1_project_2_role(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_FLOW_MANAGER.role_id, scope_id=1)

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("flow.execute", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt"))

        self.assertFalse(UserPermission(user_id=self.USER2).check("project.delete", 1))
        self.assertTrue(UserPermission(user_id=self.USER2).check("flow.execute", 1))
        self.assertTrue(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt"))

    def test_2_biz_role(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=591)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=592)

        self.assertTrue(UserPermission(user_id=self.USER1).check("biz.manage", 591))
        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.etl", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt"))

        self.assertFalse(UserPermission(user_id=self.USER2).check("biz.manage", 591))
        self.assertFalse(UserPermission(user_id=self.USER2).check("raw_data.etl", 1))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt"))

    def test_list_scope(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FLOW_MANAGER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=591)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_SUPERUSER.role_id, scope_id="*")

        # 此处入库的scope数据，数字ID重新读取为字符串，思考是否有更合适的方式
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("project.retrieve")[0]["project_id"], 1)
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("biz.manage")[0]["bk_biz_id"], 591)

        scopes = UserPermission(user_id=self.USER1).get_scopes("result_table.query_data")

        project_count = biz_count = 0
        for scope in scopes:
            if "project_id" in scope:
                self.assertEqual(scope, {"project_id": 1})
                project_count += 1
            elif "bk_biz_id" in scope:
                self.assertIn(scope["sensitivity"], ["private", "public"])
                biz_count += 1

        self.assertEqual(project_count, 1)
        self.assertEqual(biz_count, 1)

    def test_superuser(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_SUPERUSER.role_id, scope_id="*")
        self.assertTrue(UserPermission(user_id=self.USER1).check("biz.manage", 591))
        self.assertTrue(UserPermission(user_id=self.USER1).check("project.manage", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.etl", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("flow.execute", 1))

    def test_bkdata_user(self):
        self.assertTrue(UserPermission(user_id=self.USER1).check("project.create", None, display_detail=True))
        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.create", None, display_detail=True))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.retrieve", "591_test_rt"))
        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.retrieve", 1))

        self.assertFalse(UserPermission(user_id=self.USER1).check("biz.manage", 591))
        self.assertFalse(UserPermission(user_id=self.USER1).check("project.manage", 1))
        self.assertFalse(UserPermission(user_id=self.USER1).check("raw_data.etl", 1))
        self.assertFalse(UserPermission(user_id=self.USER1).check("flow.execute", 1))
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("result_table.retrieve")[0]["*"], "*")
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("raw_data.retrieve")[0]["*"], "*")

    def test_request_list_biz(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=591)
        data = self.is_api_success(self.get(f"/v3/auth/users/{self.USER1}/bizs/"))
        self.assertTrue(len(data) == 1)

        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_SUPERUSER.role_id, scope_id="*")
        data = self.is_api_success(self.get(f"/v3/auth/users/{self.USER2}/bizs/"))
        self.assertTrue(len(data) > 1)

        biz_tester_role = RoleConfig.objects.get(pk="biz.tester")
        UserRole.objects.create(user_id="user03", role_id=biz_tester_role.role_id, scope_id="591")
        data = self.is_api_success(self.get("/v3/auth/users/user03/bizs/", {"action_id": "biz.common_access"}))
        self.assertTrue(len(data) == 1)

        data = self.is_api_success(self.get("/v3/auth/users/user03/bizs/", {"action_id": "biz.job_access"}))
        self.assertTrue(len(data) == 0)

    def test_flow_developer_perm(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FLOW_DEVELOPER.role_id, scope_id=1)

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("project.manage_flow", 1))

        self.assertFalse(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt2"))

        self.assertEqual(RolePermission.get_authorizers(self.ROLE_FLOW_DEVELOPER.role_id), ["project.manager"])

    def test_flow_member_for_dashboard(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FLOW_MANAGER.role_id, scope_id=1)
        self.assertTrue(UserPermission(user_id=self.USER1).check("project.manage_dashboard", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.create", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.delete", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.update", 1))

        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("dashboard.update")[0]["project_id"], 1)
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("dashboard.create"), [])
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("dashboard.retrieve")[0]["project_id"], 1)
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("dashboard.delete")[0]["project_id"], 1)

    def test_dashboard(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_DASHBOARD_VIEWER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_DASHBOARD_VIEWER.role_id, scope_id=1)

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.retrieve", 1))

        scopes = UserPermission(user_id=self.USER1).get_scopes("project.retrieve")
        scope_project_ids = [s["project_id"] for s in scopes]
        self.assertIn(1, scope_project_ids)
        scopes = UserPermission(user_id=self.USER1).get_scopes("dashboard.retrieve")
        scope_project_ids = [s["project_id"] for s in scopes]
        self.assertIn(1, scope_project_ids)

        self.assertTrue(UserPermission(user_id=self.USER2).check("dashboard.retrieve", 1))
        self.assertEqual(UserPermission(user_id=self.USER2).get_scopes("dashboard.retrieve")[0]["dashboard_id"], 1)

        self.assertEqual(RolePermission.get_authorizers(self.ROLE_DASHBOARD_VIEWER.role_id), ["project.manager"])
        self.assertEqual(
            RolePermission.get_authorizers(self.ROLE_PROJECT_DASHBOARD_VIEWER.role_id), ["project.manager"]
        )

        scopes = UserPermission(user_id=self.USER1).get_scopes_all("project.retrieve")
        scope_project_ids = [s["project_id"] for s in scopes]
        self.assertIn(1, scope_project_ids)

        scopes = UserPermission(user_id=self.USER1).get_scopes_all("dashboard.retrieve")
        scope_dashboard_ids = [s["dashboard_id"] for s in scopes]
        self.assertIn(1, scope_dashboard_ids)

    def test_dashboard_project_manager(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.retrieve", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.update", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.create", 1))
        self.assertTrue(UserPermission(user_id=self.USER1).check("dashboard.delete", 1))

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.manage_dashboard", 1))
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("project.manage_dashboard")[0]["project_id"], 1)
        scopes = UserPermission(user_id=self.USER1).get_scopes_all("dashboard.retrieve")
        # self.assertListEqual(scopes, [{'dashboard_id': 1}, {u'project_id': 1}])
        self.assertListEqual(scopes, [{"dashboard_id": 1}])

    def test_function(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FUNCTION_MANAGER.role_id, scope_id="abs")
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_FUNCTION_DEVELOPER.role_id, scope_id="abs")

        self.assertTrue(UserPermission(user_id=self.USER1).check("function.develop", "abs"))
        self.assertTrue(UserPermission(user_id=self.USER2).check("function.develop", "abs"))

        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("function.develop")[0]["function_id"], "abs")
        self.assertEqual(UserPermission(user_id=self.USER2).get_scopes("function.develop")[0]["function_id"], "abs")

        self.assertEqual(RolePermission.get_authorizers(self.ROLE_FUNCTION_MANAGER.role_id), ["function.manager"])
        self.assertEqual(RolePermission.get_authorizers(self.ROLE_FUNCTION_DEVELOPER.role_id), ["function.manager"])

    def test_datatoken(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.DATA_TOKEN_MANAGER.role_id, scope_id="1")

        self.assertTrue(UserPermission(user_id=self.USER1).check("data_token.manage", 1))
        self.assertFalse(UserPermission(user_id=self.USER2).check("data_token.manage", 1))
        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("data_token.manage")[0]["data_token_id"], 1)
        self.assertEqual(RolePermission.get_authorizers(self.DATA_TOKEN_MANAGER.role_id), ["data_token.manager"])

    def test_resource_group(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.RESOURCE_GROUP_MANAGER.role_id, scope_id="default")

        self.assertTrue(UserPermission(user_id=self.USER1).check("resource_group.manage_capacity", "default"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("resource_group.manage_capacity", "default"))
        self.assertEqual(
            UserPermission(user_id=self.USER1).get_scopes("resource_group.manage_capacity")[0]["resource_group_id"],
            "default",
        )
        self.assertEqual(
            RolePermission.get_authorizers(self.RESOURCE_GROUP_MANAGER.role_id), ["resource_group.manager"]
        )

    def test_raw_data(self):
        self.USER3 = "user03"
        UserRole.objects.create(
            user_id=self.USER1, role_id=RoleConfig.objects.get(pk="raw_data.manager").role_id, scope_id="4"
        )
        UserRole.objects.create(
            user_id=self.USER2, role_id=RoleConfig.objects.get(pk="raw_data.cleaner").role_id, scope_id="4"
        )
        UserRole.objects.create(
            user_id=self.USER3, role_id=RoleConfig.objects.get(pk="raw_data.viewer").role_id, scope_id="4"
        )
        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.collect_hub", "4"))

        # 这个是因为 raw_data.cleaner 也有 raw_data.collect_hub 这个权限
        self.assertTrue(UserPermission(user_id=self.USER2).check("raw_data.collect_hub", "4"))
        self.assertFalse(UserPermission(user_id=self.USER3).check("raw_data.collect_hub", "4"))

        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.etl", "4"))
        self.assertTrue(UserPermission(user_id=self.USER2).check("raw_data.etl", "4"))
        self.assertFalse(UserPermission(user_id=self.USER3).check("raw_data.etl", "4"))

        self.assertTrue(UserPermission(user_id=self.USER1).check("raw_data.query_data", "4"))
        self.assertTrue(UserPermission(user_id=self.USER2).check("raw_data.query_data", "4"))
        self.assertTrue(UserPermission(user_id=self.USER3).check("raw_data.query_data", "4"))

        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt3"))
        self.assertTrue(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt3"))
        self.assertFalse(UserPermission(user_id=self.USER3).check("result_table.query_data", "591_test_rt3"))

        self.assertEqual(UserPermission(user_id=self.USER1).get_scopes("raw_data.etl")[0]["raw_data_id"], 4)
        self.assertEqual(
            UserPermission(user_id=self.USER1).get_scopes_all("result_table.query_data")[0]["result_table_id"],
            "591_test_rt3",
        )

    def test_project(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        ProjectData.objects.create(project_id=1, bk_biz_id=593, result_table_id="593_test_rt", created_by="admin")

        self.assertFalse(UserPermission(user_id=self.USER1).check("result_table.query_data", "592_test_rt"))
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "593_test_rt"))

        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "592_test_rt"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "593_test_rt"))

        scopes = UserPermission(user_id=self.USER1).get_scopes_all("result_table.query_data")
        scope_result_table_ids = [scope["result_table_id"] for scope in scopes]

        self.assertIn("593_test_rt", scope_result_table_ids)

    def test_dataadmin_sys_auth(self):
        dataadmin_data_operator = RoleConfig.objects.get(pk="dataadmin.data_operator")
        UserRole.objects.create(user_id=self.USER1, role_id=dataadmin_data_operator.role_id)

        self.assertTrue(UserPermission(user_id=self.USER1).check("dataadmin.standardization"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("dataadmin.standardization"))

    def test_check_with_diff_response(self):
        """
        不同检查结果的返回
        """
        dataadmin_data_operator = RoleConfig.objects.get(pk="dataadmin.data_operator")
        UserRole.objects.create(user_id=self.USER1, role_id=dataadmin_data_operator.role_id)

        ret = UserPermission(user_id=self.USER1).check("dataadmin.standardization", display_detail=True)
        self.assertTrue(ret["result"])
        self.assertTrue(ret["code"], "1500200")

        self.assertRaises(
            PermissionDeniedError,
            UserPermission(user_id=self.USER2).check,
            "dataadmin.standardization",
            raise_exception=True,
        )

    def test_check_policy_with_attr(self):

        # 测试使用
        RoleConfig.objects.create(role_id="bkdata.test_role", role_name="", object_class="bkdata")
        RolePolicyInfo.objects.create(
            role_id="bkdata.test_role",
            action_id="project.manage",
            object_class="project",
            scope_attr_key="project_id",
            scope_attr_value="111",
        )

        UserRole.objects.create(user_id=self.USER1, role_id="bkdata.test_role", scope_id="*")

        self.assertTrue(UserPermission(user_id=self.USER1).check("project.manage", "111"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("project.manage", "111"))

    def test_check_policy_with_sen_attr(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id="591")
        self.assertTrue(UserPermission(user_id=self.USER1).check("result_table.query_data", "591_test_rt"))
        self.assertFalse(UserPermission(user_id=self.USER2).check("result_table.query_data", "591_test_rt"))

    def test_request_role_check(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        ret = self.is_api_success(
            self.post(f"/v3/auth/users/{self.USER1}/check/", {"object_id": 1, "action_id": "project.retrieve"})
        )
        self.assertTrue(ret)

        ret = self.is_api_success(
            self.post(f"/v3/auth/users/{self.USER1}/check/", {"object_id": 2, "action_id": "project.retrieve"})
        )
        self.assertFalse(ret)

        # 测试 5 秒失败缓存
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=2)
        ret = self.is_api_success(
            self.post(f"/v3/auth/users/{self.USER1}/check/", {"object_id": 2, "action_id": "project.retrieve"})
        )
        self.assertFalse(ret)

        time.sleep(10)
        ret = self.is_api_success(
            self.post(f"/v3/auth/users/{self.USER1}/check/", {"object_id": 2, "action_id": "project.retrieve"})
        )
        self.assertTrue(ret)

        # 测试 1 分钟成功缓存
        UserRole.objects.filter(scope_id=2).delete()
        ret = self.is_api_success(
            self.post(f"/v3/auth/users/{self.USER1}/check/", {"object_id": 2, "action_id": "project.retrieve"})
        )
        self.assertTrue(ret)

    def test_request_user_check_with_detail(self):
        ret = self.is_api_success(
            self.post(
                f"/v3/auth/users/{self.USER1}/check/",
                {"object_id": 1, "action_id": "project.retrieve", "display_detail": True},
            )
        )
        self.assertFalse(ret["result"])

    def test_request_user_batch_check(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)

        user = "processor666"
        params = {
            "permissions": [
                {"user_id": user, "action_id": "project.retrieve", "object_id": "1"},
                {"user_id": user, "action_id": "project.retrieve", "object_id": "2"},
            ]
        }
        results = self.is_api_success(self.post("/v3/auth/users/batch_check/", params))

        m_result = {"{}::{}::{}".format(r["user_id"], r["action_id"], r["object_id"]): r["result"] for r in results}

        self.assertTrue(m_result[f"{user}::project.retrieve::1"])
        self.assertFalse(m_result[f"{user}::project.retrieve::2"])

    def test_request_list_scope_dimensions(self):
        ResultTable.objects.create(
            bk_biz_id=105,
            project_id=2,
            result_table_id="105_aaaa",
            result_table_name="105_aaaa",
            result_table_name_alias="105_aaaa",
            description="105_aaaa",
            processing_type="clean",
            sensitivity="private",
        )
        ResultTable.objects.create(
            bk_biz_id=780,
            project_id=2,
            result_table_id="780_aaaa",
            result_table_name="780_aaaa",
            result_table_name_alias="780_aaaa",
            description="780_aaaa",
            processing_type="clean",
            sensitivity="private",
        )
        ResultTable.objects.create(
            bk_biz_id=105,
            project_id=2,
            result_table_id="900_aaaa",
            result_table_name="900_aaaa",
            result_table_name_alias="900_aaaa",
            description="900_aaaa",
            processing_type="clean",
            sensitivity="private",
        )
        ProjectData.objects.create(project_id=1, result_table_id="900_aaaa", bk_biz_id=900)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id=1)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=591)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=105)
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id=780)
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_SUPERUSER.role_id, scope_id="*")

        data1 = self.is_api_success(
            self.get(
                "/v3/auth/users/scope_dimensions/",
                {"action_id": "result_table.query_data", "dimension": "bk_biz_id", "bk_username": self.USER1},
            )
        )
        self.assertTrue(len(data1) == 4)

        data2 = self.is_api_success(
            self.get(
                "/v3/auth/users/scope_dimensions/",
                {"action_id": "result_table.query_data", "dimension": "bk_biz_id", "bk_username": self.USER2},
            )
        )
        self.assertTrue(len(data2) > 10)

        data3 = self.is_api_success(
            self.get(
                "/v3/auth/users/scope_dimensions/",
                {"action_id": "raw_data.update", "dimension": "bk_biz_id", "bk_username": self.USER1},
            )
        )
        self.assertTrue(len(data3) == 1)

    def test_request_operations_configs(self):
        OperationConfig.objects.filter(operation_id="datahub").update(users=f"user11111,user2222,{self.USER1}")
        response = self.get("/v3/auth/users/operation_configs/", {"bk_username": self.USER1})
        data = self.is_api_success(response)
        datahub_operation = [_d for _d in data if _d["operation_id"] == "datahub"][0]

        self.assertEqual(datahub_operation["status"], "active")

    def test_request_dgraph_scopes(self):
        ROLE_RESULT_TABLE_MANAGER = RoleConfig.objects.get(pk="result_table.manager")

        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id="591")
        UserRole.objects.create(user_id=self.USER1, role_id=ROLE_RESULT_TABLE_MANAGER.role_id, scope_id="591_xxxx")

        response = self.post(
            "/v3/auth/users/dgraph_scopes/",
            {
                "permissions": [
                    {
                        "action_id": "raw_data.update",
                        "user_id": self.USER1,
                        "variable_name": "AAA",
                        "metadata_type": "access_raw_data",
                    },
                    {
                        "action_id": "result_table.query_data",
                        "user_id": self.USER1,
                        "variable_name": "AAA",
                        "metadata_type": "result_table",
                    },
                    {
                        "action_id": "result_table.query_data",
                        "user_id": self.USER1,
                        "variable_name": "AAA",
                        "metadata_type": "bk_biz",
                    },
                ]
            },
        )
        data1 = self.is_api_success(response)

        self.assertTrue(data1[0]["result"]["result"])
        statement0 = data1[0]["result"]["statement"]
        self.assertIn('eq(AccessRawData.sensitivity, "private") AND eq(AccessRawData.bk_biz_id, [591])', statement0)
        self.assertIn('eq(AccessRawData.sensitivity, "public") AND eq(AccessRawData.bk_biz_id, [591])', statement0)

        self.assertTrue(data1[1]["result"]["result"])
        statement1 = data1[1]["result"]["statement"]

        self.assertIn('eq(ResultTable.sensitivity, ["public"])', statement1)
        self.assertIn("eq(ResultTable.project_id, [1])", statement1)
        self.assertIn('eq(ResultTable.result_table_id, ["591_xxxx"])', statement1)
        self.assertIn('eq(ResultTable.sensitivity, "private") AND eq(ResultTable.bk_biz_id, [591])', statement1)

        self.assertFalse(data1[2]["result"]["result"])

    def test_request_scopes(self):
        ROLE_RESULT_TABLE_MANAGER = RoleConfig.objects.get(pk="result_table.manager")

        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=ROLE_RESULT_TABLE_MANAGER.role_id, scope_id="591_xxxx")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id="592")

        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER2, role_id=self.ROLE_SUPERUSER.role_id, scope_id="*")

        ResultTable.objects.create(
            result_table_id="592_test_rt_111111",
            bk_biz_id="592",
            project_id="111111",
            sensitivity="confidential",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )

        response = self.get(
            f"/v3/auth/users/{self.USER1}/scopes/", {"action_id": "result_table.query_data", "show_display": 1}
        )
        data = self.is_api_success(response)
        result_table_ids = [d["result_table_id"] for d in data if "result_table_id" in d]
        # project_ids = [d['project_id'] for d in data if 'project_id' in d]

        # self.assertIn(1, project_ids)
        self.assertIn("591_test_rt", result_table_ids)
        self.assertIn("591_xxxx", result_table_ids)
        self.assertNotIn("592_test_rt_111111", result_table_ids)

        response = self.get(
            f"/v3/auth/users/{self.USER2}/scopes/",
            {"action_id": "result_table.query_data", "show_display": 1, "show_admin_scopes": 1},
        )
        data = self.is_api_success(response)
        result_table_ids = [d["result_table_id"] for d in data if "result_table_id" in d]
        # project_ids = [d['project_id'] for d in data if 'project_id' in d]

        super_scopes = [d for d in data if "*" in d]
        self.assertEqual(len(super_scopes), 0)

        # self.assertIn(1, project_ids)
        self.assertIn("591_test_rt", result_table_ids)
        self.assertIn("591_test_rt2", result_table_ids)
        self.assertIn("591_test_rt3", result_table_ids)
        self.assertIn("592_test_rt_111111", result_table_ids)

        # 当未开启自动解析管理员范围时，会有 * 范围返回
        response = self.get(
            f"/v3/auth/users/{self.USER2}/scopes/", {"action_id": "result_table.query_data", "show_display": 1}
        )
        data = self.is_api_success(response)
        super_scopes = [d for d in data if "*" in d]
        self.assertEqual(len(super_scopes), 1)
        self.assertEqual(super_scopes[0], {"*": "*"})

        result_table_ids = [d["result_table_id"] for d in data if "result_table_id" in d]
        self.assertNotIn("592_test_rt_111111", result_table_ids)

        # 测试分页
        response = self.get(
            f"/v3/auth/users/{self.USER2}/scopes/",
            {"action_id": "result_table.query_data", "show_admin_scopes": 1, "page": 1, "page_size": 2},
        )
        data = self.is_api_success(response)
        self.assertEqual(len(data), 2)

    def test_handover(self):
        ROLE_RESULT_TABLE_MANAGER = RoleConfig.objects.get(pk="result_table.manager")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FLOW_DEVELOPER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=ROLE_RESULT_TABLE_MANAGER.role_id, scope_id="591_xxxx")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id="592")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FUNCTION_MANAGER.role_id, scope_id="abs")

        UserPermission(self.USER1).handover(self.USER2)

        not_handovered_role_ids = [r.role_id for r in UserRole.objects.filter(user_id=self.USER1)]
        has_handovered_role_ids = [r.role_id for r in UserRole.objects.filter(user_id=self.USER2)]

        self.assertIn("project.manager", has_handovered_role_ids)
        self.assertIn("function.manager", has_handovered_role_ids)
        self.assertIn("project.flow_developer", not_handovered_role_ids)
        self.assertIn("result_table.manager", not_handovered_role_ids)
        self.assertIn("biz.manager", not_handovered_role_ids)

    def test_handover_request(self):
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_PROJECT_MANAGER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FLOW_DEVELOPER.role_id, scope_id="1")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_BIZ_MANAGER.role_id, scope_id="592")
        UserRole.objects.create(user_id=self.USER1, role_id=self.ROLE_FUNCTION_MANAGER.role_id, scope_id="abs")

        url = "/v3/auth/users/handover/"
        params = {
            "bkdata_authentication_method": "inner",
            "bk_app_code": settings.APP_ID,
            "bk_app_secret": settings.APP_TOKEN,
            "bk_username": self.USER1,
            "receiver": self.USER2,
        }
        response = self.post(url, params)

        self.is_api_success(response)

        has_handovered_role_ids = [r.role_id for r in UserRole.objects.filter(user_id=self.USER2)]

        self.assertIn("project.manager", has_handovered_role_ids)
        self.assertIn("function.manager", has_handovered_role_ids)

        record = AuthAuditRecord.objects.all()[0]

        self.assertEqual(record.audit_num, 2)
        self.assertEqual(len(json.loads(record.audit_log)), 2)

    def test_public_rts(self):
        ResultTable.objects.create(
            result_table_id="592_test_rt_111111",
            bk_biz_id="592",
            project_id="111111",
            sensitivity="public",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )
        UserPermission(self.USER1).check("result_table.query_data", "592_test_rt_111111", raise_exception=True)
        self.assertTrue(UserPermission(self.USER1).check("result_table.query_data", "592_test_rt_111111"))
        self.assertEqual(UserPermission(self.USER1).get_scopes("result_table.query_data"), [{"sensitivity": "public"}])
        self.assertEqual(
            UserPermission(self.USER1).get_scopes_all("result_table.query_data"),
            [{"result_table_id": "592_test_rt_111111"}],
        )


class ModelUserPermTestCase(BaseTestCase):
    @classmethod
    def setUpTestData(cls):
        AlgoModel.objects.create(
            model_id="test_model",
            model_name="test_model_name",
            model_alias="test_model_alias",
            description="xxxx",
            project_id=1,
            sensitivity="private",
            active=1,
        )

        AlgoSampleSet.objects.create(
            sample_set_id=1111,
            sample_set_name="test_sample_name",
            sample_set_alias="test_sample_alias",
            description="xxxx",
            project_id=1,
            sensitivity="private",
            active=1,
        )

    def test_sample_model(self):
        model_member_role = "project.model_member"
        flow_member_role = "project.flow_member"

        UserRole.objects.create(user_id="user1", role_id=model_member_role, scope_id="1")
        self.assertTrue(UserPermission(user_id="user1").check("project.manage_flow", 1))
        self.assertTrue(UserPermission(user_id="user1").check("model.create", 1))
        self.assertTrue(UserPermission(user_id="user1").check("model.copy_experiment", "test_model"))
        self.assertTrue(UserPermission(user_id="user1").check("model.create_release", "test_model"))
        self.assertTrue(UserPermission(user_id="user1").check("model.copy_service", "test_model"))
        self.assertTrue(UserPermission(user_id="user1").check("model.update_serving", "test_model"))

        self.assertTrue(UserPermission(user_id="user1").check("sample_set.create", 1))
        self.assertTrue(UserPermission(user_id="user1").check("sample_set.copy", 1111))

        self.assertListEqual(UserPermission(user_id="user1").get_scopes("model.create"), [{"project_id": 1}])
        self.assertListEqual(UserPermission(user_id="user1").get_scopes("model.update_serving"), [{"project_id": 1}])

        UserRole.objects.create(user_id="user2", role_id=flow_member_role, scope_id="1")
        self.assertFalse(UserPermission(user_id="user2").check("model.create", 1))
        self.assertFalse(UserPermission(user_id="user2").check("model.copy_experiment", "test_model"))
        self.assertTrue(UserPermission(user_id="user2").check("model.retrieve_service", "test_model"))

        self.assertFalse(UserPermission(user_id="user2").check("sample_set.create", 1))
        self.assertFalse(UserPermission(user_id="user2").check("sample_set.update", 1111))
        self.assertTrue(UserPermission(user_id="user2").check("sample_set.copy", 1111))

        # [{'project_id': 1}, {'sensitivity': 'public'}]
        scopes = UserPermission(user_id="user2").get_scopes("sample_set.copy")
        for s in scopes:
            if "project_id" in s:
                self.assertEqual(s["project_id"], 1)
            if "sensitivity" in s:
                self.assertEqual(s["sensitivity"], "public")

    def test_request_dgraph_scopes(self):
        model_member_role = "project.model_member"

        UserRole.objects.create(user_id="user1", role_id=model_member_role, scope_id="1")

        response = self.post(
            "/v3/auth/users/dgraph_scopes/",
            {
                "permissions": [
                    {
                        "action_id": "model.update_serving",
                        "user_id": "user1",
                        "variable_name": "AAA",
                        "metadata_type": "model_info",
                    },
                    {
                        "action_id": "sample_set.copy",
                        "user_id": "user1",
                        "variable_name": "AAA",
                        "metadata_type": "sample_set",
                    },
                ]
            },
        )
        data = self.is_api_success(response)

        self.assertTrue(data[0]["result"]["result"])
        statement0 = "AAA as var(func: has(ModelInfo.typed)) @filter( ( eq(ModelInfo.project_id, [1]) ) )"
        self.assertEqual(" ".join(data[0]["result"]["statement"].split()), statement0)

        self.assertTrue(data[1]["result"]["result"])
        content = data[1]["result"]["statement"]
        self.assertIn("OR", content)
        self.assertIn("eq(SampleSet.project_id, [1])", content)
        self.assertIn('eq(SampleSet.sensitivity, ["public"])', content)


class AlgorithmUserPermTestCase(BaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Algorithm.objects.create(
            algorithm_name="test_algorithm_1",
            algorithm_alias="Test Algorithm",
            description="xxxx",
            project_id=1,
            sensitivity="private",
        )

        Algorithm.objects.create(
            algorithm_name="test_algorithm_2",
            algorithm_alias="Test Algorithm",
            description="xxxx",
            project_id=2,
            sensitivity="private",
        )

    def test_algorithm(self):
        model_member_role = "project.model_member"

        UserRole.objects.create(user_id="user1", role_id=model_member_role, scope_id="1")
        self.assertTrue(UserPermission(user_id="user1").check("project.manage_flow", 1))
        self.assertTrue(UserPermission(user_id="user1").check("algorithm.create", 1))
        self.assertTrue(UserPermission(user_id="user1").check("algorithm.retrieve", "test_algorithm_1"))
        self.assertTrue(UserPermission(user_id="user1").check("algorithm.update", "test_algorithm_1"))
        self.assertTrue(UserPermission(user_id="user1").check("algorithm.delete", "test_algorithm_1"))
        self.assertListEqual(UserPermission(user_id="user1").get_scopes("algorithm.create"), [{"project_id": 1}])

        flow_member_role = "project.flow_member"
        UserRole.objects.create(user_id="user2", role_id=flow_member_role, scope_id="1")
        self.assertTrue(UserPermission(user_id="user2").check("algorithm.retrieve", "test_algorithm_1"))

        view_role = "project.viewer"
        UserRole.objects.create(user_id="user3", role_id=view_role, scope_id="1")
        self.assertTrue(UserPermission(user_id="user3").check("algorithm.retrieve", "test_algorithm_1"))

        manager_role = "project.manager"
        UserRole.objects.create(user_id="user3", role_id=manager_role, scope_id="2")
        self.assertTrue(UserPermission(user_id="user3").check("algorithm.retrieve", "test_algorithm_2"))
        self.assertTrue(UserPermission(user_id="user3").check("algorithm.update", "test_algorithm_2"))
        self.assertTrue(UserPermission(user_id="user3").check("algorithm.delete", "test_algorithm_2"))

    def test_request_dgraph_scopes(self):
        model_member_role = "project.model_member"

        UserRole.objects.create(user_id="user1", role_id=model_member_role, scope_id="1")

        response = self.post(
            "/v3/auth/users/dgraph_scopes/",
            {
                "permissions": [
                    {
                        "action_id": "algorithm.delete",
                        "user_id": "user1",
                        "variable_name": "AAA",
                        "metadata_type": "algorithm",
                    }
                ]
            },
        )
        data = self.is_api_success(response)
        self.assertTrue(data[0]["result"]["result"])
        statement = "AAA as var(func: has(Algorithm.typed)) @filter( ( eq(Algorithm.project_id, [1]) ) )"
        self.assertEqual(" ".join(data[0]["result"]["statement"].split()), statement)


class ResultTableUpdateDataTestCase(BaseTestCase):
    """
    引入结果表更新数据权限，
    """

    multi_db = True

    def setUp(self):
        super().setUp()
        IAMBackend.SUPPORT_IAM = False

    def test_queryset_update_data(self):
        ResultTable.objects.create(
            result_table_id="592_test_rt_111111",
            bk_biz_id="592",
            project_id="111111",
            sensitivity="private",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )
        ResultTable.objects.create(
            result_table_id="592_test_rt_22222",
            bk_biz_id="592",
            project_id="111111",
            sensitivity="private",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="queryset",
            description="test_desc",
        )
        UserRole.objects.create(user_id="userA", role_id="project.manager", scope_id="111111")

        UserRole.objects.create(user_id="userB", role_id="biz.manager", scope_id="592")

        self.assertTrue(
            UserPermission("userA").check("result_table.update_data", "592_test_rt_111111", raise_exception=False)
        )

        self.assertFalse(
            UserPermission("userB").check("result_table.update_data", "592_test_rt_22222", raise_exception=False)
        )

        self.assertListEqual(UserPermission("userA").get_scopes("result_table.update_data"), [{"project_id": 111111}])
        content = UserPermission("userA").get_scopes_all("result_table.update_data")
        result_tabled_ids = [c["result_table_id"] for c in content]
        self.assertIn("592_test_rt_22222", result_tabled_ids)
        self.assertIn("592_test_rt_111111", result_tabled_ids)


class IAMUserPermTestCase(BaseTestCase):
    multi_db = True

    def setUp(self):
        super().setUp()
        IAMBackend.SUPPORT_IAM = True

    def test_get_user_backend(self):
        backend = UserPermission("userA").get_user_backend("project.manage")
        self.assertTrue(isinstance(backend, IAMBackend))

        backend = UserPermission("userA").get_user_backend("bkdata.admin")
        self.assertTrue(isinstance(backend, RBACBackend))

    @mock.patch("auth.core.permission.users.IAMBackend.check")
    def test_core_check(self, patch_check):
        UserPermission("userA").core_check("project.manage", "11")
        patch_check.assert_called_with("userA", "project.manage", "11")

    @mock.patch("auth.core.permission.users.IAMBackend.get_scopes")
    def test_get_scope(self, patch_get_scopes):
        UserPermission("userA").get_scopes("project.manage")
        patch_get_scopes.assert_called_with("userA", "project.manage")
