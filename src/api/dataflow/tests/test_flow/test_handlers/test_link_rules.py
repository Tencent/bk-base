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

from rest_framework.test import APITestCase

from dataflow.flow.handlers.link_rules import NodeInstanceLink


class TestNodeInstanceLink(APITestCase):
    def test_filter_detail_path(self):
        detail_path_list = [
            "process_model->kafka->hdfs->hdfs_storage",
            "process_model->kafka->hdfs->clickhouse_storage",
            "stream->kafka->clickhouse_storage",
        ]
        result = NodeInstanceLink.filter_detail_path(detail_path_list)
        self.assertTrue(result, ["stream->kafka->clickhouse_storage", "process_model->kafka->hdfs_storage"])

    def test_get_head_tail(self):
        total_path = ""
        result = NodeInstanceLink.get_head_tail(total_path)
        self.assertFalse(result, "")
        total_path = "process_model->kafka->hdfs_storage"
        result = NodeInstanceLink.get_head_tail(total_path)
        self.assertTrue(result, "process_model->hdfs_storage")

    def test_build_jump_path(self):
        pass
