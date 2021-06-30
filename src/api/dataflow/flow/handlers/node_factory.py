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

from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.models import FlowNodeInfo, FlowNodeRelation
from dataflow.flow.node_types import NodeTypes


class NodeFactory(object):
    NODE_MAP = {}

    def __init__(self):
        pass

    @classmethod
    def add_node_class(cls, node_type, node_class):
        cls.NODE_MAP[node_type] = node_class

    def get_source_nodes(self, result_table_id):
        """
        查询 result_table_id 作为 source 的节点列表
        @param {string} result_table_id
        @return {list<NodeHandler>}
        """
        try:
            f_rt = FlowNodeRelation.objects.filter(
                result_table_id=result_table_id,
                node_type__in=NodeTypes.SOURCE_CATEGORY,
            )
            return [self.get_node_handler(_r.node_id) for _r in f_rt]
        except FlowNodeRelation.DoesNotExist:
            return None

    def get_node_handler_by_node(self, node):
        """
        获得节点代理类 NodeHandler
        @param node {object} django.db.model
        """
        return self.NODE_MAP[node.node_type].init_by_node(node)

    def get_node_handler(self, node_id):
        try:
            node = FlowNodeInfo.objects.get(node_id=node_id)
        except FlowNodeInfo.DoesNotExist:
            raise Errors.NodeError(_("获取节点失败，节点(%s)不存在") % node_id)
        return self.NODE_MAP[node.node_type](node_id)

    def get_node_handler_by_type(self, node_type):
        return self.NODE_MAP[node_type]()


NODE_FACTORY = NodeFactory()
