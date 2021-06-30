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

from dataflow.uc.graph.job_edge import JobEdge
from dataflow.uc.graph.job_graph import JobGraph
from dataflow.uc.graph.job_vertex import JobVertex
from dataflow.uc.graph.vertex_processing import VertexProcessing


class JobGraphGenerator(object):
    def __init__(self, unified_computing_graph):
        self.unified_computing_graph = unified_computing_graph
        self.job_graph = JobGraph()
        self.node_chaining_tag = None

    def create_job_graph(self):
        self.job_graph.project_id = self.unified_computing_graph.project_id
        self.node_chaining_tag = self.set_chaining()
        chain_info = self.create_chain_info(self.node_chaining_tag)
        self.__create_job_vertex(chain_info)
        self.__create_job_edge()
        self.__set_job_graph_head_vertices()
        return self.job_graph

    def __set_job_graph_head_vertices(self):
        for job_vertex_id, job_vertex in list(self.job_graph.vertices.items()):
            if len(job_vertex.inputs) == 0:
                self.job_graph.add_head_vertex_id(job_vertex_id)

    def __create_job_vertex(self, chain_info):
        for chain_tag, node_ids in list(chain_info.items()):
            job_vertex_heads = self.__collect_job_vertex_heads(node_ids)
            job_vertex = JobVertex(job_vertex_heads)
            processing_ids = []
            for head_id in job_vertex_heads:
                processing_ids.append(head_id)
                self.__sort_processing(head_id, processing_ids, node_ids)
            processing = []
            for processing_id in processing_ids:
                processing.append(
                    VertexProcessing(
                        self.unified_computing_graph.project_id,
                        processing_id,
                        self.unified_computing_graph.get_node(processing_id).conf,
                    )
                )
            job_vertex.set_processing(processing)
            job_vertex.set_job_type(
                processing[0].conf["component_type"],
                processing[0].conf["implement_type"],
                processing[0].conf["programming_language"],
            )
            self.job_graph.add_vertex(chain_tag, job_vertex)

    def __create_job_edge(self):
        for job_vertex_id, job_vertex in list(self.job_graph.vertices.items()):
            for job_head_id in job_vertex.heads:
                head_node = self.unified_computing_graph.get_node(job_head_id)
                # 如果job vertex的head node和其上游node不在相同的job vertex中，则需要创建 job edge
                for in_edge in head_node.in_edges:
                    if self.node_chaining_tag[in_edge.source_id] != job_vertex_id:
                        job_edge = JobEdge(self.node_chaining_tag[in_edge.source_id], job_vertex_id)
                        self.job_graph.vertices[self.node_chaining_tag[in_edge.source_id]].add_output(job_edge)
                        self.job_graph.vertices[job_vertex_id].add_input(job_edge)

    def __collect_job_vertex_heads(self, node_ids):
        # 查找 job vertex 内的 head
        job_vertex_heads = set()
        for node_id in node_ids:
            # 如果当前 node 没有入边，或者当前 node 的上游 node 不在相同 job vertex中，则当前 node 为 job vertex 的head
            if len(
                self.unified_computing_graph.get_node(node_id).in_edges
            ) == 0 or not self.__is_job_vertex_contain_source_node(node_ids, node_id):
                job_vertex_heads.add(node_id)
        return job_vertex_heads

    def __sort_processing(self, head_id, processing_ids, node_ids):
        # 对 job vertex 中的 processing 进行排序
        for out_edge in self.unified_computing_graph.get_node(head_id).out_edges:
            target_node = self.unified_computing_graph.get_node(out_edge.target_id)
            if (
                target_node.id in node_ids
                and self.__is_source_node_collected(target_node.id, processing_ids, node_ids)
                and target_node.id not in processing_ids
            ):
                processing_ids.append(target_node.id)
            self.__sort_processing(target_node.id, processing_ids, node_ids)

    def __is_source_node_collected(self, current_node_id, processing_ids, node_ids):
        """
        判断当前节点是否可以加入 processing id中
        判断条件为当上游节点属于当前 job vertex，并且上游节点没有加入 processing id时，不能收集此上游节点。

        :param current_node_id: 当前 node id
        :param processing_ids: 按照顺序收集processing的list
        :param node_ids: 当前job vertex的node
        :return: True 当前节点可以被收集到processing; False 当前节点不可以被收集到processing
        """
        for in_edge in self.unified_computing_graph.get_node(current_node_id).in_edges:
            source_node = self.unified_computing_graph.get_node(in_edge.source_id)
            if source_node.id in node_ids and source_node.id not in processing_ids:
                return False
        return True

    def __is_job_vertex_contain_source_node(self, node_ids, current_node_id):
        for in_edge in self.unified_computing_graph.get_node(current_node_id).in_edges:
            in_node = self.unified_computing_graph.get_node(in_edge.source_id)
            if in_node.id in node_ids:
                return True
        return False

    @classmethod
    def create_chain_info(cls, node_chaining_tag):
        chain_info = {}
        for node_id, chain_tag in list(node_chaining_tag.items()):
            if chain_tag in chain_info:
                chain_info[chain_tag].add(node_id)
            else:
                chain_info[chain_tag] = {node_id}
        return chain_info

    def set_chaining(self):
        # 记录已经 chain 的node
        chained_node_ids = set()
        node_chaining_tag = {}
        chain_tag = 0
        for source_id in self.unified_computing_graph.heads:
            chained_node_ids.add(source_id)
            node_chaining_tag[source_id] = chain_tag
            source_node = self.unified_computing_graph.get_node(source_id)
            self.chain_down_node(chained_node_ids, source_node, node_chaining_tag, chain_tag)
        return node_chaining_tag

    def chain_down_node(self, chained_node_ids, source_node, node_chaining_tag, chain_tag):
        """
        当前节点向下游 chain 其他的节点

        :param chained_node_ids:  已经参与过chain的节点
        :param source_node:  当前节点
        :param node_chaining_tag:  记录节点chain的表识
        :param chain_tag:  当前 chain 的tag
        """
        for out_edge in self.unified_computing_graph.get_node(source_node.id).out_edges:
            target_node = self.unified_computing_graph.get_node(out_edge.target_id)
            if out_edge.target_id in chained_node_ids:
                continue
            chained_node_ids.add(out_edge.target_id)
            if self.is_chaining(source_node, target_node):
                node_chaining_tag[out_edge.target_id] = chain_tag
            else:
                chain_tag += 1
                node_chaining_tag[out_edge.target_id] = chain_tag
            # 判断 target node节点上游是否包含其他节点，如果包含其他节点，判断其是否可以chain在一起
            if len(target_node.in_edges) > 1:
                self.chain_up_node(chained_node_ids, target_node, node_chaining_tag, chain_tag)
            # 继续判断下游节点是否需要chain
            self.chain_down_node(chained_node_ids, target_node, node_chaining_tag, chain_tag)

    def chain_up_node(self, chained_node_ids, current_node, node_chaining_tag, chain_tag):
        """
        从当前节点向上游 chain 其他节点。

        :param chained_node_ids:  已经参与过chain的节点
        :param current_node:  当前节点
        :param node_chaining_tag:  记录节点chain的表识
        :param chain_tag: 当前 chain 的tag
        """
        for in_edge in current_node.in_edges:
            in_node = self.unified_computing_graph.get_node(in_edge.source_id)
            if in_node.id in chained_node_ids:
                continue
            chained_node_ids.add(in_node)
            if self.is_chaining(current_node, in_node):
                node_chaining_tag[in_edge.source_id] = node_chaining_tag[current_node.id]
                if len(in_node.in_edges) > 1:
                    self.chain_up_node(chained_node_ids, in_node, node_chaining_tag, chain_tag)
            else:
                chain_tag += 1
                node_chaining_tag[in_edge.source_id] = chain_tag

    def is_chaining(self, node1, node2):
        """
        判断 node1 和 node2 是否可以 chain 在一起

        :param node1:  node1
        :param node2:  node 2
        :return:  True 代表可以chain在一起 False 代表不可以chain在一起
        """
        if not self.unified_computing_graph.chaining:
            return False
        if (
            node1.conf["component_type"] == "flink"
            and node1.conf["processor_type"] == "sql"
            and node2.conf["component_type"] == "flink"
            and node2.conf["processor_type"] == "sql"
        ):
            return True
        return False
