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

import json
from unittest import mock, skip

from django.conf import settings
from django.http import JsonResponse
from rest_framework.test import APITestCase


from common.auth.identities import InnerIdentity, TokenIdentity, UserIdentity
from common.auth.middlewares import AuthenticationMiddleware
from common.auth.perms import ProjectPerm, UserPerm
from common.local import del_local_param, get_local_param


class TestUserPerm(APITestCase):
    def test_check(self):
        self.assertFalse(UserPerm("test_user").check("result_table.retrieve", "591_xxx"))

    def test_list_scope(self):
        UserPerm("test_user").list_scopes("result_table.retrieve")


class TestProjectPerm(APITestCase):
    def test_update_role_users(self):
        self.assertTrue(ProjectPerm(1).update_role_users([{"role_id": "project.manager", "user_ids": ["test_user"]}]))

    def test_check_data(self):
        self.assertFalse(ProjectPerm(1).check_data("591_xxx"))

    def test_check_cluster_group(self):
        self.assertTrue(ProjectPerm(1).check_cluster_group("default"))
        self.assertFalse(ProjectPerm(1).check_cluster_group("default1"))


class MockJWTClient(object):
    def __init__(self, request):
        self.jwt = request.META["HTTP_X_BKAPI_JWT"]

    def is_valid(self):
        return True

    def get_app_model(self):
        app_code = self.jwt.split(":")[0]
        return {"verified": True, "app_code": app_code}

    def get_user_model(self):
        username = self.jwt.split(":")[1]

        return {"verified": username == "admin", "username": username}  # 非 admin 均认为是未验证


class TestIdentity(APITestCase):
    @skip("Here, testcases use the real jwt")
    def test_verify_bk_jwt(self):
        request = mock.Mock()
        request.META = {
            "HTTP_X_BKAPI_JWT": "aabbxxxaaqaa",
            "HTTP_GATEWAY_NAME": "bk-data-inner",
        }
        print(UserIdentity.verify_bk_jwt_request(request, verify_app=True, verify_user=True))

    def setUp(self):
        self.clear_local_identity()

    def test_inner_regular_format(self):
        """
        正常格式
        """
        request = mock.Mock()
        request.method = "GET"
        request.GET = {}
        request.META = {
            "HTTP_X_BKDATA_AUTHORIZATION": json.dumps(
                {
                    "bk_app_code": settings.APP_ID,
                    "bk_app_secret": settings.APP_TOKEN,
                    "bkdata_authentication_method": "inner",
                    "bk_username": "admin",
                }
            )
        }
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), InnerIdentity))
        self.assertEqual(get_local_param("bk_username"), "admin")
        self.assertEqual(get_local_param("bk_app_code"), "data")

    def test_inner_compatible_format(self):
        """
        兼容格式
        """
        request = mock.Mock()
        request.method = "GET"
        request.GET = {
            "bk_app_code": settings.APP_ID,
            "bk_app_secret": settings.APP_TOKEN,
            "bkdata_authentication_method": "inner",
        }
        request.META = {}
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), InnerIdentity))
        self.assertEqual(get_local_param("bk_username"), "")
        self.assertEqual(get_local_param("bk_app_code"), "data")

    def test_inner_user_compatible_format(self):
        request = mock.Mock()
        request.method = "GET"
        request.GET = {
            "bk_app_code": settings.APP_ID,
            "bk_app_secret": settings.APP_TOKEN,
            "bkdata_authentication_method": "user",
            "bk_username": "user01",
        }
        request.META = {}
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), UserIdentity))
        self.assertEqual(get_local_param("bk_username"), "user01")
        self.assertEqual(get_local_param("bk_app_code"), "data")

    def test_inner_wrong_params(self):
        """
        错误参数
        """
        request = mock.Mock()
        request.method = "GET"
        request.GET = {
            "bk_app_code": settings.APP_ID,
            "bk_app_secret": "xxx",
            "bkdata_authentication_method": "inner",
            "bkdata_force_authentication": True,
        }
        request.META = {}
        response = AuthenticationMiddleware().process_request(request)
        self.assertTrue(isinstance(response, JsonResponse))
        self.assertFalse(isinstance(get_local_param("identity"), InnerIdentity))

    @mock.patch("common.auth.identities.JWTClient", MockJWTClient)
    def test_user_regular_format(self):
        """
        正常格式
        """
        request = mock.Mock()
        request.method = "GET"
        request.GET = {}
        request.META = {
            "HTTP_X_BKDATA_AUTHORIZATION": json.dumps({"bkdata_authentication_method": "user"}),
            "HTTP_X_BKAPI_JWT": "data:admin",
        }
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), UserIdentity))
        self.assertEqual(get_local_param("bk_username"), "admin")
        self.assertEqual(get_local_param("bk_app_code"), "data")

        self.clear_local_identity()

        request.META = {"HTTP_X_BKAPI_JWT": "data:admin"}
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), UserIdentity))
        self.assertEqual(get_local_param("bk_username"), "admin")
        self.assertEqual(get_local_param("bk_app_code"), "data")

    @mock.patch("common.auth.identities.JWTClient", MockJWTClient)
    def test_user_wrong_params(self):
        """
        错误格式，没有经过验证的用户名
        """
        request = mock.Mock()
        request.method = "GET"
        request.GET = {}
        request.META = {"HTTP_X_BKAPI_JWT": "data:"}
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertFalse(isinstance(get_local_param("identity"), UserIdentity))

    @mock.patch("common.auth.identities.JWTClient", MockJWTClient)
    @mock.patch("common.auth.identities.TokenIdentity.get_token_info")
    def test_token_regular_format(self, patch_get_token_info):
        """
        正常格式
        """
        patch_get_token_info.return_value = {"bk_username": "user01"}

        request = mock.Mock()
        request.method = "GET"
        request.GET = {
            "bkdata_data_token": "xxxxxxxxxxxxxxx",
        }
        request.META = {"HTTP_X_BKAPI_JWT": "data:xxx"}
        response = AuthenticationMiddleware().process_request(request)
        self.assertIsNone(response)
        self.assertTrue(isinstance(get_local_param("identity"), TokenIdentity))
        self.assertEqual(get_local_param("bk_app_code"), "data")
        self.assertEqual(get_local_param("bk_username"), "xxx")

    @mock.patch("common.auth.identities.JWTClient", MockJWTClient)
    @mock.patch("common.auth.identities.TokenIdentity.get_token_info")
    def test_token_wrong_params(self, patch_get_token_info):
        """
        正常格式
        """
        patch_get_token_info.return_value = None

        request = mock.Mock()
        request.method = "GET"
        request.GET = {"bkdata_data_token": "xxxxxxxxxxxxxxx", "bkdata_force_authentication": True}
        request.META = {"HTTP_X_BKAPI_JWT": "data:xxx"}
        response = AuthenticationMiddleware().process_request(request)
        self.assertTrue(isinstance(response, JsonResponse))
        self.assertFalse(isinstance(get_local_param("identity"), TokenIdentity))

    def clear_local_identity(self):
        del_local_param("identity")
        del_local_param("bk_username")
        del_local_param("bk_app_code")
