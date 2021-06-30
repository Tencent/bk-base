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
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

from auth.constants import TokenPermissionStatus
from auth.core.permission import TokenPermission
from auth.exceptions import TokenDisabledErr, TokenExpiredErr
from auth.models import TokenTicketPermission
from auth.models.audit_models import AuthAuditRecord
from auth.models.auth_models import AuthDataToken, AuthDataTokenPermission
from auth.models.ticket_models import Ticket
from auth.services.token import TokenGenerator
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse

MOCK_DATA_SCOPE = {
    "is_all": False,
    "permissions": [
        {
            "action_id": "result_table.query_data",
            "object_class": "result_table",
            "scope_id_key": "result_table_id",
            "scope_name_key": "result_table_name",
            "scope_object_class": "result_table",
            "scope": {"result_table_id": "591_test_rt"},
        },
        {
            "action_id": "project.manage",
            "object_class": "project",
            "scope_id_key": "project_id",
            "scope_name_key": "project_name",
            "scope_object_class": "project",
            "scope": {"project_id": "1"},
        },
        {
            "action_id": "project.manage_flow",
            "object_class": "project",
            "scope_id_key": "project_id",
            "scope_name_key": "project_name",
            "scope_object_class": "project",
            "scope": {"project_id": "1"},
        },
        {
            "action_id": "raw_data.etl",
            "object_class": "raw_data",
            "scope_id_key": "raw_data_id",
            "scope_name_key": "raw_data_name",
            "scope_object_class": "raw_data",
            "scope": {"raw_data_id": "1"},
        },
        {
            "action_id": "raw_data.query_data",
            "object_class": "raw_data",
            "scope_id_key": "raw_data_id",
            "scope_name_key": "raw_data_name",
            "scope_object_class": "raw_data",
            "scope": {"raw_data_id": "1"},
        },
    ],
}

BK_USERNAME = "processor666"
BK_APP_CODE = "bk_log_search"
EXPIRES = 7


class TokenTestCase(BaseTestCase):
    data_token = None

    def setUp(self):
        super().setUp()

        token_generator = TokenGenerator(BK_USERNAME, BK_APP_CODE, MOCK_DATA_SCOPE, EXPIRES)
        self.data_token_object = token_generator.create_token(reason="init token")
        self.data_token = self.data_token_object.data_token

    def test_check(self):
        """
        校验权限是否写入
        @return:
        """
        for _perm in MOCK_DATA_SCOPE.get("permissions", []):
            ret = TokenPermission(self.data_token).check(
                _perm["action_id"], list(_perm["scope"].values())[0], bk_app_code=BK_APP_CODE
            )
            self.assertTrue(ret)

        # 非法对象
        ret = TokenPermission(self.data_token).check("result_table.query_data", "592_test_rt", bk_app_code=BK_APP_CODE)
        self.assertFalse(ret)

        # 非法 APP_CODE
        ret = TokenPermission(self.data_token).check("result_table.query_data", "591_test_rt", bk_app_code="data")
        self.assertFalse(ret)

        # 校验子对象
        ret = TokenPermission(self.data_token).check("flow.update", "1", bk_app_code=BK_APP_CODE)

        ret = TokenPermission(self.data_token).check("flow.update", "2", bk_app_code=BK_APP_CODE)
        self.assertFalse(ret)

        # 状态修改
        o_token = AuthDataToken.objects.get(data_token=self.data_token)
        o_token._status = "disabled"
        o_token.save()
        self.assertRaises(
            TokenDisabledErr,
            TokenPermission(self.data_token).check,
            "result_table.query_data",
            "591_test_rt",
            bk_app_code=BK_APP_CODE,
            raise_exception=True,
        )
        # 修改过期时间
        o_token = AuthDataToken.objects.get(data_token=self.data_token)
        o_token._status = "enabled"
        o_token.expired_at = datetime.now() - timedelta(days=1)
        o_token.save()

        self.assertRaises(
            TokenExpiredErr,
            TokenPermission(self.data_token).check,
            "result_table.query_data",
            "591_test_rt",
            bk_app_code=BK_APP_CODE,
            raise_exception=True,
        )

    def test_token_approve(self):
        """
        测试没有权限的用户，需要产生审批单据
        """
        scope = {
            "is_all": False,
            "permissions": [
                {
                    "action_id": "project.manage",
                    "object_class": "project",
                    "scope_id_key": "project_id",
                    "scope_name_key": "project_name",
                    "scope_object_class": "project",
                    "scope": {"project_id": "1"},
                }
            ],
        }
        token_generator = TokenGenerator("user01", BK_APP_CODE, scope, EXPIRES)
        o_token = token_generator.create_token(reason="init token")
        data_token_id = o_token.id
        data_token = o_token.data_token

        ret = TokenPermission(data_token).check("project.manage", "1", bk_app_code=BK_APP_CODE)
        self.assertFalse(ret)
        ticket_permissions = TokenTicketPermission.objects.filter(subject_id=data_token_id)
        self.assertEqual(len(ticket_permissions), 1)
        ticket_id = ticket_permissions[0].ticket_id
        self.assertEqual(Ticket.objects.get(id=ticket_id).status, "processing")

    def test_create_token(self, data_scope=MOCK_DATA_SCOPE):
        """
        正常流程
        @param data_scope:
        @return:
        """
        token_generator = TokenGenerator(BK_USERNAME, BK_APP_CODE, data_scope, EXPIRES)
        o_token = AuthDataToken.objects.get(data_token=self.data_token)
        self.assertTrue(o_token.data_token == self.data_token)
        self.assertTrue(o_token.created_by == BK_USERNAME)
        self.assertTrue(o_token.data_token_bk_app_code == BK_APP_CODE)
        self.assertTrue(o_token.description == "init token")
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(data_token=o_token).count()
            == len(token_generator.data_scope_permission)
        )

    def test_create_token_for_applying(self):
        """
        创建需审批的授权码
        @return:
        """
        data_scope = {
            "is_all": False,
            "permissions": [
                {
                    "action_id": "result_table.query_data",
                    "object_class": "result_table",
                    "scope_id_key": "result_table_id",
                    "scope_name_key": "result_table_name",
                    "scope_object_class": "result_table",
                    "scope": {"result_table_id": "666_test_rt"},
                },
            ],
        }
        token_generator = TokenGenerator(BK_USERNAME, BK_APP_CODE, data_scope, EXPIRES)
        data_token = token_generator.create_token().data_token
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(
                data_token__data_token=data_token, status=TokenPermissionStatus.APPLYING
            ).count()
            == len(token_generator.data_scope_permission)
        )

        ret = TokenPermission(data_token).check("result_table.query_data", "592_test_rt", bk_app_code=BK_APP_CODE)

        self.assertFalse(ret)

    # def test_create_token_without_permission(self):
    #     """
    #     绕过页面，通过API申请无权限的数据范围
    #     @return:
    #     """
    #     data_scope = {
    #         "is_all": False,
    #         "permissions": [
    #             {
    #                 "action_id": "result_table.query_data",
    #                 "object_class": "result_table",
    #                 "scope_id_key": "result_table_id",
    #                 "scope_name_key": "result_table_name",
    #                 "scope_object_class": "result_table",
    #                 "scope": {
    #                     "result_table_id": "593_test_rt"
    #                 }
    #             },
    #         ]
    #     }
    #     token_generator = TokenGenerator(BK_USERNAME, BK_APP_CODE, data_scope, EXPIRES)
    #     with self.assertRaises(PermissionDeniedError):
    #         token_generator.create_token()

    def test_mock_direct_pass_perm(self):
        """
        直接通过，mock
        @return:
        """
        data_scope = {
            "is_all": False,
            "permissions": [
                {
                    "action_id": "result_table.query_data",
                    "object_class": "result_table",
                    "scope_id_key": "result_table_id",
                    "scope_name_key": "result_table_name",
                    "scope_object_class": "result_table",
                    "scope": {"result_table_id": "591_test_rt"},
                },
            ],
        }
        token_generator = TokenGenerator(BK_USERNAME, BK_APP_CODE, data_scope, EXPIRES)

        # 直接授权，不产生单据
        token_generator.is_direct_pass = MagicMock(return_value=True)
        o_data_token = token_generator.create_token()
        self.assertFalse(TokenTicketPermission.objects.filter(subject_id=o_data_token.id).exists())
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(data_token=o_data_token, status=TokenPermissionStatus.ACTIVE).count()
            == len(token_generator.data_scope_permission)
        )

        # 申请审批单据
        token_generator.is_direct_pass = MagicMock(return_value=False)
        o_data_token = token_generator.create_token()
        self.assertTrue(TokenTicketPermission.objects.filter(subject_id=o_data_token.id).exists())
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(data_token=o_data_token, status=TokenPermissionStatus.ACTIVE).count()
            == len(token_generator.data_scope_permission)
        )

    def test_check_api(self):
        url = "/v3/auth/tokens/check/"
        params = {
            "check_app_code": BK_APP_CODE,
            "check_data_token": self.data_token,
            "action_id": "result_table.query_data",
            "object_id": "591_test_rt",
        }

        response = self.client.post(url, json.dumps(params), content_type="application/json")
        data = self.is_api_success(response)
        self.assertTrue(data)

    def test_retrive_by_data_token_api(self):
        url = "/v3/auth/tokens/retrive_by_data_token/"
        params = {"search_data_token": self.data_token}

        response = self.client.get(url, params)
        data = self.is_api_success(response)
        self.assertTrue(data["data_token"], self.data_token)

    def test_exchange_default_data_token(self):
        url = "/v3/auth/tokens/exchange_default_data_token/"
        params = {"data_token_bk_app_code": "bk_log_search"}
        response = self.client.post(url, params)
        data = self.is_api_success(response)
        self.assertEqual(data["data_token_bk_app_code"], "bk_log_search")

        params = {"data_token_bk_app_code": "gem"}
        response = self.client.post(url, params)
        data = self.is_api_success(response)
        self.assertIsNone(data)

    def test_list(self):
        # 除了 setup 初次创建的 Token 以外，再建一个新的，便于测试列表接口
        token_generator = TokenGenerator("others", BK_APP_CODE, {"is_all": False, "permissions": []}, EXPIRES)
        token_generator.create_token(reason="init token2")

        url = "/v3/auth/tokens/"
        params = {"bkdata_authentication_method": "user", "bk_username": BK_USERNAME}
        response = self.client.get(url, params)
        data = self.is_api_success(response)
        self.assertEqual(len(data), 1)

    def test_renewal(self):
        """
        测试对 DataToken 进行续期
        """
        data_token_id = self.data_token_object.id
        # 第一次续期由于只有 7 天，可以续期
        url = f"/v3/auth/tokens/{data_token_id}/renewal/"
        params = {"expire": 30, "bkdata_authentication_method": "user", "bk_username": BK_USERNAME}
        response = self.post(url, params)
        self.is_api_success(response)
        record = AuthAuditRecord.objects.all()[0]
        self.assertEqual(record.created_by, BK_USERNAME)
        self.assertEqual(record.audit_object_id, str(data_token_id))

        # 第二次续期由于已经 37 天，不需要续期
        url = f"/v3/auth/tokens/{data_token_id}/renewal/"
        params = {"expire": 30, "bkdata_authentication_method": "user", "bk_username": BK_USERNAME}
        response = self.post(url, params)
        self.is_api_success(response)
        self.assertEqual(AuthAuditRecord.objects.all().count(), 1)

    def test_retrieve(self):
        data_token_id = self.data_token_object.id
        url = f"/v3/auth/tokens/{data_token_id}/"
        params = {"bkdata_authentication_method": "user", "bk_username": BK_USERNAME}
        response = self.client.get(url, params)
        data = self.is_api_success(response)
        self.assertIn("permissions", data)
        self.assertTrue(len(data["permissions"]) > 0)
        self.assertIn("scopes", data)

        url = f"/v3/auth/tokens/{data_token_id}/"
        params = {
            "bkdata_authentication_method": "user",
            "bk_username": BK_USERNAME,
            "permission_status": "applying",
            "show_display": "False",
            "show_scope_structure": "False",
        }
        response = self.client.get(url, params)
        data = self.is_api_success(response)
        self.assertIn("permissions", data)
        self.assertEqual(len(data["permissions"]), 0)
        self.assertNotIn("scopes", data)

    @mock.patch("auth.handlers.result_table.MetaApi.list_result_table")
    @mock.patch("auth.services.token.TokenGenerator.create_token")
    def test_upsert_datascope_for_queue(self, patch_create_token, patch_list_result_table):

        patch_create_token.return_value = AuthDataToken(data_token=1, data_token_bk_app_code="xxxx")
        patch_list_result_table.return_value = DataResponse(
            {"data": [{"result_table_id": "591_presto_cluster", "storages": {"tspider": {}}}]}
        )
        url = "/v3/auth/tokens/"

        params = {
            "data_token_bk_app_code": "xxxx",
            "data_scope": {
                "permissions": [
                    {
                        "action_id": "result_table.query_queue",
                        "object_class": "result_table",
                        "scope_id_key": "result_table_id",
                        "scope_object_class": "result_table",
                        "scope": {
                            "result_table_id": "591_presto_cluster",
                        },
                    }
                ]
            },
            "reason": "dsff",
            "expire": 7,
        }
        response = self.post(url, params)
        self.is_api_failure(response)

        patch_list_result_table.return_value = DataResponse(
            {"data": [{"result_table_id": "591_dimension_rt", "storages": {"queue": {}, "tspider": {}}}]}
        )
        params = {
            "data_token_bk_app_code": "xxxx",
            "data_scope": {
                "permissions": [
                    {
                        "action_id": "result_table.query_queue",
                        "object_class": "result_table",
                        "scope_id_key": "result_table_id",
                        "scope_object_class": "result_table",
                        "scope": {
                            "result_table_id": "591_dimension_rt",
                        },
                    }
                ]
            },
            "reason": "dsff",
            "expire": 7,
        }
        response = self.post(url, params)
        self.is_api_success(response)

    @mock.patch("auth.handlers.resource_group.StoreKitApi.list_cluster_by_type")
    @mock.patch("auth.services.token.TokenGenerator.create_token")
    def test_upsert_datascope_for_res_group(self, patch_create_token, patch_list_cluster_by_type):
        patch_create_token.return_value = AuthDataToken(data_token=1, data_token_bk_app_code="xxxx")
        patch_list_cluster_by_type.return_value = DataResponse(
            {"data": [{"cluster_type": "presto", "cluster_group": "aiops"}]}
        )
        url = "/v3/auth/tokens/"
        params = {
            "data_token_bk_app_code": "xxxx",
            "data_scope": {
                "permissions": [
                    {
                        "action_id": "resource_group.use",
                        "object_class": "resource_group",
                        "scope_id_key": "resource_group_id",
                        "scope_object_class": "resource_group",
                        "scope": {
                            "resource_group_id": "aiops",
                        },
                    }
                ]
            },
            "reason": "dsff",
            "expire": 7,
        }
        response = self.post(url, params)
        self.is_api_success(response)
