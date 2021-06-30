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
from dataflow.uc.graph.unified_computing_graph import UnifiedComputingGraph


class UnifiedComputingGraphGenerator(object):
    def __init__(self, chaining):
        self.chaining = chaining
        self.__graph = None

    def generate(self, graph_conf):
        """
        生成 UnifiedComputingGraph

        :param graph_conf: graph conf, ex: {"project_id": 123, "nodes": []}
        :return: UnifiedComputingGraph
        """
        logger.info("Begin to generate graph, and the conf is %s" % str(graph_conf))
        self.__graph = UnifiedComputingGraph()
        self.__graph.set_chaining(self.chaining)

        # set project id
        self.__graph.project_id = graph_conf["project_id"]

        # graph 增加 node
        for node_conf in graph_conf["nodes"]:
            self.__graph.add_node(node_conf)

        # 给 node 增加 edge
        for node_id, node in list(self.__graph.nodes.items()):
            if "children" not in node.conf:
                logger.info("The node %s without any children" % node_id)
                continue
            for child in node.conf["children"]:
                self.__graph.add_edge(node_id, child)

        # graph 设置 heads
        for node_id, node in list(self.__graph.nodes.items()):
            if len(node.in_edges) == 0:
                self.__graph.add_head(node_id)
        return self.__graph
