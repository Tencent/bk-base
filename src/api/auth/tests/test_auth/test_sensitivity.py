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

from auth.models.auth_models import AuthDatasetSensitivity
from auth.tests.utils import BaseTestCase


class SensitivityTestCase(BaseTestCase):
    def test_list(self):
        resp = self.get("/v3/auth/sensitivity/", {"has_biz_role": True, "bk_biz_id": 591})
        data = self.is_api_success(resp)

        private_config = [_d for _d in data if _d["id"] == "private"][0]
        self.assertEqual(private_config["biz_role_id"], "biz.manager")

    def test_retrieve_dataset(self):
        resp = self.get(
            "/v3/auth/sensitivity/retrieve_dataset/", {"data_set_type": "result_table", "data_set_id": "591_test_rt"}
        )
        data = self.is_api_success(resp)
        self.assertEqual(data["tag_method"], "default")
        self.assertEqual(data["biz_role_memebers"], ["processor666"])
        self.assertEqual(data["bk_biz_id"], 591)
        self.assertEqual(data["sensitivity_id"], "private")
        self.assertIn("sensitivity_description", data)

        resp = self.get("/v3/auth/sensitivity/retrieve_dataset/", {"data_set_type": "raw_data", "data_set_id": "1"})
        data = self.is_api_success(resp)
        self.assertEqual(data["tag_method"], "user")

    @mock.patch("auth.views.sensitivity_views.AccessApi.update_raw_data")
    @mock.patch("auth.views.sensitivity_views.MetaApi.update_result_table")
    @mock.patch("auth.views.sensitivity_views.check_perm")
    def test_update_dataset(self, patch_check_perm, patch_update_result_table, patch_raw_data):
        patch_check_perm.return_value = True
        patch_check_perm.patch_update_result_table = True
        patch_check_perm.patch_raw_data = True
        resp = self.post(
            "/v3/auth/sensitivity/update_dataset/",
            {
                "bkdata_authentication_method": "user",
                "bk_username": "admin",
                "data_set_type": "raw_data",
                "data_set_id": "1",
                "tag_method": "user",
                "sensitivity": "confidential",
            },
        )
        self.is_api_success(resp)
        patch_check_perm.assert_called_with("raw_data.manage_auth", "1")
        patch_raw_data.assert_called_with({"raw_data_id": "1", "sensitivity": "confidential"}, raise_exception=True)

        resp = self.post(
            "/v3/auth/sensitivity/update_dataset/",
            {
                "bkdata_authentication_method": "user",
                "bk_username": "admin",
                "data_set_type": "result_table",
                "data_set_id": "591_test_rt",
                "tag_method": "user",
                "sensitivity": "confidential",
            },
        )
        self.is_api_success(resp)
        patch_update_result_table.assert_called_with(
            {"result_table_id": "591_test_rt", "sensitivity": "confidential"}, raise_exception=True
        )
        patch_check_perm.assert_called_with("result_table.manage_auth", "591_test_rt")
        self.assertEqual(AuthDatasetSensitivity.get_dataset_tag_method("result_table", "591_test_rt"), "user")

        resp = self.post(
            "/v3/auth/sensitivity/update_dataset/",
            {
                "bkdata_authentication_method": "user",
                "bk_username": "admin",
                "data_set_type": "result_table",
                "data_set_id": "591_test_rt",
                "tag_method": "default",
            },
        )
        self.is_api_success(resp)
        patch_check_perm.assert_called_with("result_table.manage_auth", "591_test_rt")
        self.assertEqual(AuthDatasetSensitivity.get_dataset_tag_method("result_table", "591_test_rt"), "default")
