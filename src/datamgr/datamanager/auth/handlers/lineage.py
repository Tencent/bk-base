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
from enum import Enum

import attr

from common.meta import metadata_client

logger = logging.getLogger(__name__)


def task_apart_identifier(identifier):
    """
    @return {Tuple} (data_set_type, data_set_id)
    """
    parts = identifier.split(":")
    return DataSetType(parts[0]), parts[1]


def makeup_identifier(data_set_type, data_set_id):
    return "{}:{}".format(data_set_type.value, data_set_id)


class DataSetType(Enum):
    RESULT_TABLE = "result_table"
    RAW_DATA = "raw_data"
    DATA_PROCESSING = "data_processing"


@attr.s
class DataNode(object):
    data_set_type = attr.ib(type=DataSetType, converter=DataSetType)
    data_set_id = attr.ib(type=str, converter=str)
    identifier = attr.ib(type=str, init=False)

    # 以下字段仅注册到 Graph 才有意义
    parent_nodes = attr.ib(factory=list, init=False)
    child_nodes = attr.ib(factory=list, init=False)
    source_nodes = attr.ib(factory=list, init=False)

    is_source = attr.ib(default=False, init=False)

    graph = attr.ib(default=None, init=False)

    def __attrs_post_init__(self):
        identifier = makeup_identifier(self.data_set_type, self.data_set_id)
        object.__setattr__(self, "identifier", identifier)

    @property
    def parent_identifiers(self):
        return [node.identifier for node in self.parent_nodes]

    @property
    def child_identifiers(self):
        return [node.identifier for node in self.child_nodes]

    @property
    def source_identifiers(self):
        return [node.identifier for node in self.source_nodes]

    def __str__(self):
        return (
            "DataNode(identifier={}, is_source={}, parents={}, children={}, "
            "sources={})"
        ).format(
            self.identifier,
            self.is_source,
            self.parent_identifiers,
            self.child_identifiers,
            self.source_identifiers,
        )


@attr.s
class DataRelation(object):
    parent = attr.ib(type=DataNode)
    child = attr.ib(type=DataNode)

    # 标识唯一边
    relation_id = attr.ib(type=str, converter=str)

    def __str__(self):
        return "DataRelation(relation_id={}, parent={}, child={})".format(
            self.relation_id, self.parent, self.child
        )


class NodeManager(object):
    @classmethod
    def calc_node_sources(cls, node, **kwargs):

        if node.is_source:
            # 根节点必须往下继续遍历
            return True
        else:
            _source_nodes = list()
            for parent in node.parent_nodes:
                if len(parent.source_nodes) > 0:
                    _source_nodes.extend(
                        [s for s in parent.source_nodes if s not in _source_nodes]
                    )
                else:
                    _source_nodes = list()
                    break

        is_changed = node.source_nodes != _source_nodes
        if is_changed:
            node.source_nodes = _source_nodes

        # 如果当前节点没有发生 source_nodes 属性变更，则无需继续遍历
        return is_changed

    @classmethod
    def define_source(cls, node):
        if node.data_set_type == DataSetType.RAW_DATA:
            node.is_source = True
            node.source_nodes = [node]

        return node.is_source


class Graph(object):
    def __init__(self):
        self.nodes = dict()
        self.source_nodes = dict()
        self.relations = dict()

    def register(self, node):
        """
        已注册过的，不会重复注册
        """
        if node.graph == self:
            return node

        if NodeManager.define_source(node):
            if node.identifier not in self.source_nodes:
                self.source_nodes[node.identifier] = node

        _identifier = node.identifier
        if _identifier not in self.nodes:
            node.graph = self
            self.nodes[_identifier] = node

        return self.nodes[_identifier]

    def register_relation(self, relation):
        parent = self.register(relation.parent)
        child = self.register(relation.child)

        relation.parent = parent
        relation.child = child

        self.relations[relation.relation_id] = relation
        return relation

    def add_relation(self, relation, recalc_source=False, recalc_func=None):

        relation = self.register_relation(relation)
        parent = relation.parent
        child = relation.child

        if parent not in child.parent_nodes:
            child.parent_nodes.append(parent)

        if child not in parent.child_nodes:
            parent.child_nodes.append(child)

        if recalc_source:
            if recalc_func is None:
                recalc_func = NodeManager.assign_node_sources
            self.traverse(child, recalc_func)

    def remove_relation(self, relation_id, recalc_source=False, recalc_func=None):

        # 不存在的边，则无视即可
        if relation_id not in self.relations:
            return None

        relation = self.relations[relation_id]

        child = relation.child
        parent = relation.parent

        try:
            parent.child_nodes.remove(child)
            child.parent_nodes.remove(parent)
        except ValueError as err:
            logger.warning("Fail to remove node not in the graph, {}".format(err))

        del self.relations[relation_id]

        if recalc_source:
            if recalc_func is None:
                recalc_func = NodeManager.assign_node_sources
            self.traverse(child, recalc_func)

        return relation

    def calc_node_sources(self):
        """
        计算所有节点相关的源节点
        """
        for source in self.source_nodes.values():
            self.traverse(source, NodeManager.calc_node_sources)

    def traverse(self, node, func, traversed_nodes=None):
        node = self.register(node)

        if traversed_nodes is None:
            traversed_nodes = list()

        # Todo 回路的检测
        # 针对每个遍历到的节点，可以进行定制化操作，返回结果作为是否继续往下遍历的依据
        if func(node, traversed_nodes=traversed_nodes):
            traversed_nodes.append(node)
            for child in node.child_nodes:
                self.traverse(child, func, traversed_nodes=traversed_nodes)
        else:
            traversed_nodes.append(node)

    def __str__(self):
        return "Graph<nodes={}, relations={}, sources = {}>".format(
            len(self.nodes), len(self.relations), len(self.source_nodes)
        )

    def __unicode__(self):
        return "Graph<nodes={}, relations={}, sources = {}>".format(
            len(self.nodes), len(self.relations), len(self.source_nodes)
        )


def transform_to_graph_relation(data_processing_relaiotn):
    """
    将元数据的 data_processing 关系装换为图关系

    @paramExample data_processing_relaiotn
        {
            id: 'xx',
            'data_directing': 'input',
            'processing_id': '1_xxxxx',
            'data_set_type': 'result_table',
            'data_set_id': '1_aaa'
        }
    """
    relation_id = data_processing_relaiotn["id"]
    data_directing = data_processing_relaiotn["data_directing"]
    processing_id = data_processing_relaiotn["processing_id"]
    data_set_type = data_processing_relaiotn["data_set_type"]
    data_set_id = data_processing_relaiotn["data_set_id"]

    if data_directing == "input":
        parent = DataNode(DataSetType(data_set_type), data_set_id)
        child = DataNode(DataSetType.DATA_PROCESSING, processing_id)
        return DataRelation(parent, child, relation_id)
    elif data_directing == "output":
        parent = DataNode(DataSetType.DATA_PROCESSING, processing_id)
        child = DataNode(DataSetType(data_set_type), data_set_id)
        return DataRelation(parent, child, relation_id)
    else:
        logger.error(
            "Invalid data_directing value {} by loading meta lineage".format(
                data_directing
            )
        )

    return None


def load_meta_lineage_graph():
    statement = """
        {
            target(func: has(DataProcessingRelation.id)) {
                id: DataProcessingRelation.id
                data_directing: DataProcessingRelation.data_directing
                data_set_type: DataProcessingRelation.data_set_type
                data_set_id: DataProcessingRelation.data_set_id
                processing_id: DataProcessingRelation.processing_id
            }
        }
    """

    graph = Graph()

    content = metadata_client.query(statement)
    for item in content["data"]["target"]:

        relation = transform_to_graph_relation(item)
        if relation is not None:
            graph.add_relation(relation)

    graph.calc_node_sources()
    return graph
