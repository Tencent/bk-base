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

import logging

import mock

from auth.handlers.lineage import DataNode, DataRelation, Graph, load_meta_lineage_graph
from tests import BaseTestCase

logger = logging.getLogger(__name__)


class TestLineage(BaseTestCase):
    @mock.patch("common.meta.metadata_client.query")
    def test_load_meta_lineage_graph(self, patch_query):
        patch_query.return_value = {
            "extensions": {
                "server_latency": {
                    "parsing_ns": 10831,
                    "processing_ns": 180631016,
                    "encoding_ns": 44242738,
                },
                "txn": {"start_ts": 649530},
            },
            "data": {
                "target": [
                    {
                        "id": 4280,
                        "data_directing": "input",
                        "data_set_type": "raw_data",
                        "data_set_id": "391",
                        "processing_id": "591_login_60s_sdfsdf_5348",
                    },
                    {
                        "id": 8712,
                        "data_directing": "input",
                        "data_set_type": "result_table",
                        "data_set_id": "591_login_60s_sdfsdf_5348",
                        "processing_id": "591_install_60s_tracy4_4335",
                    },
                    {
                        "id": 8716,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "591_install_60s_tracy4_4335",
                        "processing_id": "591_install_60s_tracy4_4335",
                    },
                ]
            },
        }
        graph = load_meta_lineage_graph()
        assert len(graph.nodes) == 5
        assert len(graph.relations) == 3


class TestGraph(BaseTestCase):
    def setup(self):
        graph = Graph()

        node1 = DataNode("raw_data", "1")
        node2 = DataNode("result_table", "2_xx")
        node3 = DataNode("result_table", "3_xx")
        node4 = DataNode("result_table", "4_xx")
        node5 = DataNode("result_table", "5_xx")
        node6 = DataNode("result_table", "6_xx")
        node7 = DataNode("raw_data", "7")
        node8 = DataNode("result_table", "8_xx")
        node9 = DataNode("result_table", "9_xx")

        graph.add_relation(DataRelation(node1, node2, 1))
        graph.add_relation(DataRelation(node1, node3, 2))
        graph.add_relation(DataRelation(node3, node4, 3))
        graph.add_relation(DataRelation(node2, node5, 4))
        graph.add_relation(DataRelation(node4, node5, 5))
        graph.add_relation(DataRelation(node5, node6, 6))
        graph.add_relation(DataRelation(node7, node8, 7))
        graph.add_relation(DataRelation(node8, node9, 8))
        graph.add_relation(DataRelation(node6, node9, 9))

        """
        node1 ----------> node2
          |                 |
          V                 V
        node3 -> node4 -> node5 -> node6
                                     ^
                                     |
        node7 -> node8 ----------> node9
        """

        self.graph = graph

    def test_add_relation(self):
        graph = self.graph

        # 故意重复，看下会不会出现多余数据
        node1 = DataNode("raw_data", "1")
        node2 = DataNode("result_table", "2_xx")
        graph.add_relation(DataRelation(node1, node2, 1))

        assert "result_table:2_xx" in graph.register(node1).child_identifiers
        assert "raw_data:1" in graph.register(node2).parent_identifiers

        assert len(graph.register(node1).child_nodes) == 2
        assert len(graph.relations) == 9

    def test_remove_relation(self):
        graph = self.graph

        graph.remove_relation("2")
        graph.remove_relation("2")

        node1 = DataNode("raw_data", "1")

        assert len(graph.register(node1).child_nodes) == 1
        assert len(graph.relations) == 8

    def test_calc_sources(self):
        graph = self.graph
        assert len(graph.source_nodes) == 2

        graph.calc_node_sources()

        node1 = DataNode("raw_data", "1")
        node2 = DataNode("result_table", "2_xx")
        node3 = DataNode("result_table", "3_xx")
        node4 = DataNode("result_table", "4_xx")
        node5 = DataNode("result_table", "5_xx")
        node6 = DataNode("result_table", "6_xx")
        node7 = DataNode("raw_data", "7")
        node8 = DataNode("result_table", "8_xx")
        node9 = DataNode("result_table", "9_xx")

        assert graph.register(node1).source_identifiers == ["raw_data:1"]
        assert graph.register(node2).source_identifiers == ["raw_data:1"]
        assert graph.register(node3).source_identifiers == ["raw_data:1"]
        assert graph.register(node4).source_identifiers == ["raw_data:1"]
        assert graph.register(node5).source_identifiers == ["raw_data:1"]
        assert graph.register(node6).source_identifiers == ["raw_data:1"]
        assert graph.register(node7).source_identifiers == ["raw_data:7"]
        assert graph.register(node8).source_identifiers == ["raw_data:7"]
        assert graph.register(node9).source_identifiers == ["raw_data:7", "raw_data:1"]
