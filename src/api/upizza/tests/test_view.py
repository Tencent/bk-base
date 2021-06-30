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

from unittest import mock

from demo.models import DemoModel
from demo.views import InstanceViewset, SimpleFlowViewSet, SimpleViewSet
from django.test import TestCase
from django.urls import resolve
from tests.utils import UnittestClient


class DemoSimpleViewTest(TestCase):
    unittest_client = UnittestClient()

    def test_error_url(self):
        response = self.unittest_client.get("/v3/demo/error_url/")
        self.assertEqual(response.message, "您所请求的内容不存在")

    def test_run_with_success(self):
        found = resolve("/v3/demo/succeed/")
        self.assertEqual(found.func.cls, SimpleViewSet)

    def test_run_with_exception(self):
        response = self.unittest_client.get("/v3/demo/fail/")
        self.assertEqual(response.message, "带有参数的异常，aaa，bbb")
        self.assertEqual(response.code, "1500102")

    def test_return_with_json(self):
        response = self.unittest_client.get("/v3/demo/return_with_json/")
        self.assertEqual(response.code, "1500200")
        self.assertIsInstance(response.response, dict)

    def test_return_with_data_response(self):
        response = self.unittest_client.get("/v3/demo/return_with_data_response/")
        self.assertEqual(response.message, "self-message")

    def test_get_params(self):
        response = self.unittest_client.get("/v3/demo/get_params/", {"param1": "value"})
        self.assertEqual(response.data["param1"], "value")

    def test_post_params(self):
        response = self.unittest_client.post("/v3/demo/post_params/", {"param1": "value"})
        self.assertEqual(
            response.data, {"param1": "value", "bk_username": "bk_demo_unittest", "bk_app_code": "bk_demo_unittest"}
        )


class DemoViewTest(TestCase):
    databases = "__all__"
    unittest_client = UnittestClient()

    def test_url_to_simple_flow(self):
        found = resolve("/v3/demo/flows/1/")
        self.assertEqual(found.func.cls, SimpleFlowViewSet)

    def test_url_to_instance_view(self):
        found = resolve("/v3/demo/instances/")
        self.assertEqual(found.func.cls, InstanceViewset)

    def test_instances_list_view_response(self):
        found = resolve("/v3/demo/instances/")
        self.assertEqual(found.func.cls, InstanceViewset)
        response = self.unittest_client.get("/v3/demo/instances/")
        self.assertEqual(response.data, list(range(100000)))

    def test_instance_create_view_response(self):
        obj_dict = {"id": 1, "field1": "ins", "field2": 1, "field3": "test"}
        mock.patch("demo.models.DemoModel.objects.create", return_value=DemoModel(**obj_dict))
        response = self.unittest_client.post("/v3/demo/instances/", data=obj_dict)
        self.assertEqual(response.data["field3"], "test")

    def test_instance_destroy_view_response(self):
        DemoModel.objects.create(**{"id": 1, "field1": "ins", "field2": 1, "field3": "test"})
        mock.patch(
            "demo.models.DemoModel.objects.get",
            return_value=DemoModel(**{"id": 1, "field1": "ins", "field2": 1, "field3": "test"}),
        )
        mock.patch("demo.models.DemoModel.objects.delete", return_value=None)
        response = self.unittest_client.delete("/v3/demo/instances/1/")
        self.assertEqual(response.message, "ok")
