# -*- coding: utf-8 -*
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
from unittest import mock

from auth.bkiam.backend import iam
from auth.handlers.resource.manager import ResourceFilter
from auth.tests.utils import BaseTestCase
from rest_framework.reverse import reverse


class IAMResourceTestCase(BaseTestCase):
    """
    IAM Resource 测试用例
    """

    def setUp(self):
        self.url = reverse("iamApi")
        # 对 API 认证信息进行 MOCK
        iam.get_token = mock.Mock(return_value=(True, "ok", "254bly4zon1awbcc2oyryyvi03ra1o99"))

    @mock.patch("auth.bkiam.resources.AuthResourceManager.list_scope_attributes")
    def test_list_attr(self, patch_list_scope_attributes):
        content = [{"id": "bk_biz_id", "display_name": "业务id"}]

        patch_list_scope_attributes.return_value = content

        data = {"type": "raw_data", "method": "list_attr"}
        self.assertListEqual(self.check_response(data), content)

    @mock.patch("auth.bkiam.resources.AuthResourceManager.query_scope_attribute_value")
    def test_list_attr_value(self, patch_query_scope_attribute_value):
        content = {"results": [{"id": 100724, "display_name": "[100724]王者荣耀"}], "count": 100}

        patch_query_scope_attribute_value.return_value = content
        data = {
            "type": "raw_data",
            "method": "list_attr_value",
            "filter": {"attr": "bk_biz_id", "keyword": "关键字搜索", "ids": [1, 2, 3]},
            "page": {"offset": 0, "limit": 20},
        }

        self.assertDictEqual(self.check_response(data), content)
        patch_query_scope_attribute_value.assert_called_with(
            "bk_biz_id", search="关键字搜索", ids=[1, 2, 3], limit=20, offset=0
        )

    @mock.patch("auth.bkiam.resources.AuthResourceManager.query")
    def test_list_instance(self, patch_query):
        content = {"results": [{"display_name": "[100312] fg555", "id": 100312}], "count": 100}
        patch_query.return_value = content

        data = {
            "method": "list_instance",
            "type": "raw_data",
            "filter": {"parent": {"type": "biz", "id": "591"}},
            "page": {"offset": 0, "limit": 20},
        }
        self.assertDictEqual(self.check_response(data), content)
        patch_query.assert_called_with(
            resource_filter=ResourceFilter("biz", "591"), limit=20, offset=0, only_display=True
        )

    def check_response(self, data):
        response = self.client.post(
            self.url,
            data,
            format="json",
            **{"HTTP_AUTHORIZATION": 'Basic "YmtfaWFtOjI1NGJseTR6b24xYXdiY2Myb3lyeXl2aTAzcmExbzk5"'}
        )
        result = json.loads(response._container[0])
        self.assertEqual(result["code"], 0)
        self.assertEqual(result["result"], True)

        return result["data"]
