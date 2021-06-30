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

from django.conf import settings

from auth.handlers.role import RoleHandler
from auth.models.auth_models import AUTH_STATUS, UserRole
from auth.tests.utils import BaseTestCase, is_api_success


class RoleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        RoleHandler.SUPPORT_IAM = False

    def test_list(self):
        resp = self.get("/v3/auth/roles/")
        data = is_api_success(self, resp)
        self.assertTrue("authorizer" in data[0])
        self.assertTrue("user_mode" in data[0])
        self.assertTrue("actions" in data[0])

    def test_list_users(self):
        resp = self.get("/v3/auth/roles/project.manager/users/", {"scope_id": 1})
        return is_api_success(self, resp)

    def test_list_users2(self):
        """
        结果数据管理员，目前需要考虑业务负责人
        """
        UserRole.objects.create(user_id="user001", role_id="result_table.manager", scope_id="591_test_rt")

        resp = self.get("/v3/auth/roles/result_table.manager/users/", {"scope_id": "591_test_rt"})
        users = is_api_success(self, resp)
        self.assertIn("processor666", users)
        self.assertIn("user001", users)

    def test_user_role(self):
        resp = self.get("/v3/auth/roles/user_role/", {"scope_id": "591", "object_class": "biz", "show_display": True})
        data = is_api_success(self, resp)
        self.assertEqual(5, len(data[0]["roles"]))
        self.assertEqual("biz.manager", data[0]["roles"][1]["role_id"])
        self.assertEqual(["processor666"], data[0]["roles"][1]["users"])

        resp = self.get(
            "/v3/auth/roles/user_role/",
            {"scope_id": "591", "object_class": "biz", "show_display": True, "is_user_mode": False},
        )
        data = is_api_success(self, resp)
        self.assertEqual(5, len(data[0]["roles"]))

    def test_update_users(self):
        resp = self.put(
            "/v3/auth/roles/project.manager/users/",
            {
                "bkdata_authentication_method": "inner",
                "bk_app_code": settings.APP_ID,
                "bk_app_secret": settings.APP_TOKEN,
                "scope_id": 1,
                "user_ids": ["user00", "user01", "user02"],
            },
        )
        is_api_success(self, resp)
        self.assertEqual(sorted(self.test_list_users()), ["user00", "user01", "user02"])

        resp = self.put(
            "/v3/auth/roles/project.manager/users/",
            {
                "bkdata_authentication_method": "inner",
                "bk_app_code": settings.APP_ID,
                "bk_app_secret": settings.APP_TOKEN,
                "scope_id": 1,
                "user_ids": ["user01", "user02", "user03"],
            },
        )
        is_api_success(self, resp)
        self.assertEqual(sorted(self.test_list_users()), ["user01", "user02", "user03"])

    def test_user_role_manager(self):
        # 理论上 INVALID 状态的内容，不包括在 objects 的正常返回内容里
        UserRole.objects.create(
            user_id="user04", scope_id="1", role_id="project.manager", auth_status=AUTH_STATUS.INVALID
        )

        UserRole.objects.create(user_id="user05", scope_id="1", role_id="project.manager")

        user_ids = list(
            UserRole.objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertNotIn("user04", user_ids)
        self.assertIn("user05", user_ids)

        UserRole.objects.create(user_id="user04", scope_id="1", role_id="project.manager")
        user_ids = list(
            UserRole.objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertIn("user04", user_ids)

        UserRole.objects.create(
            user_id="user06", scope_id="1", role_id="project.manager", auth_status=AUTH_STATUS.INVALID
        )
        UserRole.objects.create(
            user_id="user07", scope_id="1", role_id="project.manager", auth_status=AUTH_STATUS.INVALID
        )
        UserRole.objects.bulk_create(
            [
                UserRole(role_id="project.manager", scope_id="1", user_id="user06"),
                UserRole(role_id="project.manager", scope_id="1", user_id="user08"),
                UserRole(role_id="project.manager", scope_id="1", user_id="user09"),
            ]
        )
        user_ids = list(
            UserRole.objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertIn("user06", user_ids)
        self.assertNotIn("user07", user_ids)
        self.assertIn("user08", user_ids)
        self.assertIn("user09", user_ids)

        user_ids = list(
            UserRole.origin_objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertIn("user07", user_ids)

        resp = self.put(
            "/v3/auth/roles/project.manager/users/",
            {
                "bkdata_authentication_method": "inner",
                "bk_app_code": settings.APP_ID,
                "bk_app_secret": settings.APP_TOKEN,
                "scope_id": 1,
                "user_ids": ["user00", "user01", "user02"],
            },
        )
        is_api_success(self, resp)
        user_ids = list(
            UserRole.objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertIn("user00", user_ids)
        self.assertNotIn("user06", user_ids)

        # 正常更新，不会移除 AUTH_STATUS.INVALID 的条目
        user_ids = list(
            UserRole.origin_objects.filter(role_id="project.manager", scope_id="1").values_list("user_id", flat=True)
        )
        self.assertIn("user07", user_ids)


class ProjectTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        RoleHandler.SUPPORT_IAM = False

    def test_list_users(self):
        resp = self.get("/v3/auth/projects/1/role_users/")
        is_api_success(self, resp)

    def test_update_users(self):
        resp = self.put(
            "/v3/auth/projects/1/role_users/",
            {
                "role_users": [
                    {"role_id": "project.manager", "user_ids": ["user00", "user01", "user02"]},
                    {"role_id": "project.flow_member", "user_ids": ["user00", "user01"]},
                ]
            },
        )
        is_api_success(self, resp)

        # 非法角色参数，如raw_data.manager
        resp = self.put(
            "/v3/auth/projects/1/role_users/",
            {
                "role_users": [
                    {"role_id": "raw_data.manager", "user_ids": ["user00", "user02"]},
                ]
            },
        )
        self.is_api_failure(resp)


class RawDataTestCase(BaseTestCase):
    RAW_DATA_ID = 1

    def setUp(self):
        super().setUp()
        RoleHandler.SUPPORT_IAM = False

    def test_list_users(self):
        resp = self.get("/v3/auth/raw_data/1/role_users/")
        is_api_success(self, resp)

    @mock.patch("auth.handlers.event.EventController.push_event")
    def test_update_users(self, patch_push_event):
        update_url = f"/v3/auth/raw_data/{self.RAW_DATA_ID}/role_users/"
        self.assertFalse(
            UserRole.objects.filter(user_id="user00", scope_id=self.RAW_DATA_ID, role_id="raw_data.manager").exists()
        )
        resp = self.put(
            update_url,
            {
                "role_users": [
                    {"role_id": "raw_data.manager", "user_ids": ["user00", "user01", "user02"]},
                ]
            },
        )
        is_api_success(self, resp)
        self.assertTrue(
            UserRole.objects.filter(user_id="user00", scope_id=self.RAW_DATA_ID, role_id="raw_data.manager").exists()
        )
        self.assertTrue(
            UserRole.objects.filter(user_id="user01", scope_id=self.RAW_DATA_ID, role_id="raw_data.manager").exists()
        )

        # 剔除成员
        self.put(
            update_url,
            {
                "role_users": [
                    {"role_id": "raw_data.manager", "user_ids": ["user00", "user02"]},
                ]
            },
        )
        self.assertFalse(
            UserRole.objects.filter(user_id="user01", scope_id=self.RAW_DATA_ID, role_id="raw_data.manager").exists()
        )

        # 非法角色参数，如project.manager
        resp = self.put(
            update_url,
            {
                "role_users": [
                    {"role_id": "project.manager", "user_ids": ["user00", "user02"]},
                ]
            },
        )
        self.is_api_failure(resp)
        self.assertEqual(patch_push_event.call_count, 5)


class OuterRoleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        RoleHandler.SUPPORT_IAM = False

    def test_update_roles_api(self):
        resp = self.put(
            "/v3/auth/roles/update_roles/",
            {
                "bk_username": "processor666",
                "user_ids": ["user01", "user02"],
                "scope_id": "1",
                "role_id": "project.manager",
            },
        )
        is_api_success(self, resp)
        self.assertEqual(
            UserRole.objects.filter(user_id="processor666", scope_id="1", role_id="project.manager").count(), 0
        )
        self.assertEqual(
            UserRole.objects.filter(user_id__in=["user01", "user02"], scope_id="1", role_id="project.manager").count(),
            2,
        )

    def test_update_roles_api_with_no_auth(self):
        resp = self.put(
            "/v3/auth/roles/update_roles/",
            {"bk_username": "user03", "user_ids": ["user01", "user02"], "scope_id": "1", "role_id": "project.manager"},
        )
        content = self.is_api_failure(resp)
        self.assertEqual(content["code"], "1511705")

    def test_multi_update_roles_api(self):
        resp = self.put(
            "/v3/auth/roles/multi_update_roles/",
            {
                "bk_username": "processor666",
                "role_users": [{"user_ids": ["user01", "user02"], "scope_id": "1", "role_id": "project.manager"}],
            },
        )
        is_api_success(self, resp)
        self.assertEqual(
            UserRole.objects.filter(user_id__in=["user01", "user02"], scope_id="1", role_id="project.manager").count(),
            2,
        )

    def test_mul_update_roles_api_no_auth(self):
        resp = self.put(
            "/v3/auth/roles/multi_update_roles/",
            {
                "bk_username": "user03",
                "role_users": [
                    {"user_ids": ["user01", "user02"], "scope_id": "1", "role_id": "project.manager"},
                    {"user_ids": ["user01", "user02"], "scope_id": "2", "role_id": "project.manager"},
                ],
            },
        )
        content = self.is_api_failure(resp)
        self.assertEqual(content["code"], "1511705")


class InnerRoleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        RoleHandler.SUPPORT_IAM = False

    @mock.patch("auth.handlers.role.role_handler.RoleSync.grant")
    @mock.patch("auth.handlers.event.EventController.push_event")
    def test_add_role(self, patch_push_event, patch_grant):
        RoleHandler.add_role("admin", "raw_data.manager", "user01", "11")
        relation = UserRole.objects.filter(user_id="user01")
        self.assertEqual(relation[0].role_id, "raw_data.manager")
        self.assertEqual(patch_push_event.call_count, 1)

        RoleHandler.SUPPORT_IAM = True
        RoleHandler.add_role("admin", "raw_data.manager", "user01", "22")
        patch_grant.assert_called_with("user01", "raw_data.manager", "22")

    @mock.patch("auth.handlers.role.role_handler.RoleSync.revoke")
    @mock.patch("auth.handlers.event.EventController.push_event")
    def test_delete_role(self, patch_push_event, patch_revoke):
        UserRole.objects.create(user_id="user01", role_id="raw_data.manager", scope_id="222")
        RoleHandler.delete_role("raw_data.manager", "user01", "222")
        self.assertEqual(patch_push_event.call_count, 1)

        RoleHandler.SUPPORT_IAM = True
        UserRole.objects.create(user_id="user01", role_id="raw_data.manager", scope_id="333")
        RoleHandler.delete_role("raw_data.manager", "user01", "333")
        patch_revoke.assert_called_with("user01", "raw_data.manager", "333")

    @mock.patch("auth.handlers.event.EventController.push_event")
    def test_handover_role(self, patch_push_event):
        UserRole.objects.create(user_id="user01", role_id="raw_data.viewer", scope_id="1234")

        UserRole.objects.create(user_id="user01", role_id="raw_data.manager", scope_id="222")

        UserRole.objects.create(user_id="user01", role_id="raw_data.manager", scope_id="333")

        RoleHandler.handover_roles("user01", "user02")
        self.assertEqual(patch_push_event.call_count, 4)

    def test_update_role_batch(self):
        permissions = [
            {"user_id": "user11", "role_id": "project.manager", "scope_id": "111", "operate": "grant"},
            {"user_id": "user22", "role_id": "project.manager", "scope_id": "111", "operate": "grant"},
        ]
        result = RoleHandler.update_role_batch("admin", permissions)
        self.assertEqual(len(result), 2)
