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

from dataflow.shared.log import uc_logger as logger
from dataflow.uc.graph.job_graph_generator import JobGraphGenerator
from dataflow.uc.graph.unified_computing_edge import UnifiedComputingEdge
from dataflow.uc.graph.unified_computing_node import UnifiedComputingNode


class UnifiedComputingGraph(object):
    def __init__(self):
        self.chaining = True
        self.nodes = {}
        self.heads = set()
        self.project_id = None

    def set_chaining(self, chaining):
        self.chaining = chaining

    def add_node(self, node_conf):
        node = UnifiedComputingNode(node_conf)
        self.nodes[node.id] = node
        logger.info("Add the node %s" % node.id)

    def get_node(self, node_id):
        return self.nodes[node_id]

    def add_edge(self, source_node_id, target_node_id):
        source_node = self.get_node(source_node_id)
        target_node = self.get_node(target_node_id)
        edge = UnifiedComputingEdge(source_node, target_node)

        self.get_node(edge.source_id).add_out_edge(edge)
        self.get_node(edge.target_id).add_in_edge(edge)

    def add_head(self, node_id):
        self.heads.add(node_id)

    def get_job_graph(self):
        return JobGraphGenerator(self).create_job_graph()
