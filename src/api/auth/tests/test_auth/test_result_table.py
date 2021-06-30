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

from auth.handlers.result_table import ResultTableHandler
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse


class ResultTableTestCase(BaseTestCase):
    @mock.patch("auth.handlers.result_table.MetaApi.list_result_table")
    def test_list_by_rt_ids_with_storages(self, patch_list_result_table):

        patch_list_result_table.return_value = DataResponse(
            {
                "data": [
                    {"result_table_id": "591_dimension_rt", "storages": {"queue": {}, "tspider": {}}},
                    {"result_table_id": "591_presto_cluster", "storages": {"tspider": {}}},
                ]
            }
        )

        result_tables = ResultTableHandler.list_by_rt_ids_with_storages(["591_dimension_rt", "591_presto_cluster"])
        self.assertTrue(result_tables[0].has_queue())
        self.assertFalse(result_tables[1].has_queue())
