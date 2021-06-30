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
from auth.handlers.object_classes import AuthResultTable, oFactory
from auth.models.outer_models import FunctionInfo
from auth.tests.utils import BaseTestCase


class ObjectTestCase(BaseTestCase):
    def test_list_result_table(self):
        rt_class = oFactory.init_object_by_class("result_table")
        rts = rt_class.list()
        self.assertEqual(len(rts), 6)

    def test_list_function(self):
        object_class = "function"
        o_object_class = oFactory.init_object_by_class(object_class)
        data = o_object_class.list(output_val_key="id", display_key_maps={"display_name_key": "name"}, filters={})
        self.assertTrue(len(data) == 1)
        self.assertEqual(data[0]["id"], "abs")
        self.assertEqual(data[0]["name"], "abs")

    def test_get_function(self):
        o_func = FunctionInfo.objects.get(function_id="abs")
        self.assertEqual(o_func.function_id, "abs")

    def test_attr_compress(self):

        more_attr_scopes = [
            {"bk_biz_id": 592, "sensitivity": "private"},
            {"bk_biz_id": 591, "sensitivity": "private"},
            {"bk_biz_id": 593, "sensitivity": "private"},
            {"bk_biz_id": 602, "sensitivity": "confidential"},
            {"project_id": 1, "sensitivity": "confidential"},
            {"project_id": 2, "sensitivity": "private"},
            {"project_id": 3, "sensitivity": "private"},
        ]

        compressed_scopes = AuthResultTable.more_attr_compress(more_attr_scopes)
        private_bk_biz_ids = []
        private_project_ids = []
        for scope in compressed_scopes:
            if "bk_biz_id" in scope and "sensitivity" in scope and scope["sensitivity"] == "private":
                private_bk_biz_ids = sorted(scope["bk_biz_id"])
            if "project_id" in scope and "sensitivity" in scope and scope["sensitivity"] == "private":
                private_project_ids = sorted(scope["project_id"])

        self.assertEqual(private_bk_biz_ids, [591, 592, 593])
        self.assertEqual(private_project_ids, [2, 3])

        one_attr_scopes = [
            {"bk_biz_id": 592},
            {"bk_biz_id": 593},
            {"bk_biz_id": 594},
            {"project_id": 11},
            {"project_id": 22},
        ]
        compressed_scopes = AuthResultTable.one_attr_compress(one_attr_scopes)
        bk_biz_ids = []
        for scope in compressed_scopes:
            if "bk_biz_id" in scope:
                bk_biz_ids = scope["bk_biz_id"]

        self.assertEqual(bk_biz_ids, [592, 593, 594])

        scopes = [
            {"result_table_id": "591_xxxx"},
            {"result_table_id": "592_xxxx"},
            {"project_id": 1},
            {"bk_biz_id": 592, "sensitivity": "private"},
        ]

        compressed_scopes = AuthResultTable.get_compressed_scopes(scopes)

        private_bk_biz_ids = []
        project_ids = []
        result_table_ids = []
        for scope in compressed_scopes:
            if "bk_biz_id" in scope and "sensitivity" in scope and scope["sensitivity"] == "private":
                private_bk_biz_ids = sorted(scope["bk_biz_id"])
            if "project_id" in scope:
                project_ids = scope["project_id"]
            if "result_table_id" in scope:
                result_table_ids = scope["result_table_id"]

        self.assertEqual(private_bk_biz_ids, [592])
        self.assertEqual(project_ids, [1])
        self.assertEqual(result_table_ids, ["591_xxxx", "592_xxxx"])
