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

from dataflow.uc.graph.unified_computing_edge import UnifiedComputingEdge
from dataflow.uc.graph.unified_computing_node import UnifiedComputingNode


class TestUnifiedComputingNode(APITestCase):
    def setUp(self):
        conf = {"processing_id": "123"}
        self.node = UnifiedComputingNode(conf)

    def test_add_in_edge(self):
        source_node = UnifiedComputingNode({"processing_id": "123_source"})
        in_edge = UnifiedComputingEdge(source_node, self.node)
        self.node.add_in_edge(in_edge)
        assert 1 == len(self.node.in_edges)
        assert "123_source" == self.node.in_edges[0].source_id
        assert "123" == self.node.in_edges[0].target_id

    def test_add_out_edge(self):
        target_node = UnifiedComputingNode({"processing_id": "123_target"})
        out_edge = UnifiedComputingEdge(self.node, target_node)
        self.node.add_out_edge(out_edge)
        assert 1 == len(self.node.out_edges)
        assert "123_target" == self.node.out_edges[0].target_id
        assert "123" == self.node.out_edges[0].source_id
