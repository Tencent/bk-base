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

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers import forms
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.nodes.base_node.base_source_node import SourceNode
from dataflow.flow.models import FlowNodeRelation
from dataflow.flow.node_types import NodeTypes


class RTSourceNode(SourceNode):
    node_type = NodeTypes.RT_SOURCE
    node_form = forms.FlowRTsourceNodeForm
    config_attrs = ["result_table_id", "bk_biz_id", "name", "from_result_table_ids"]

    def add_after(self, username, from_node_ids, form_data):
        rts_heads_tails = {
            "result_table_ids": [form_data["result_table_id"]],
            "heads": None,
            "tails": None,
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):

        rts_heads_tails = {
            "result_table_ids": [form_data["result_table_id"]],
            "heads": None,
            "tails": None,
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(RTSourceNode, self).remove_before(username)
        self.node_info.remove_as_source()

    def get_rt_generated_node(self, category=None):
        """
        查找 RT 的源节点，即生成 RT 的节点

        @note 在内部版可能存在 RT 并不来自 DataFlow，当不存在时，返回 None
        """
        if not category:
            category = NodeTypes.CALC_CATEGORY
        source_rt = ResultTable(self.result_table_id)
        # 清洗数据的源节点为空，其它的都需要获取其源节点
        if source_rt.is_clean_data():
            return None
        try:
            frt = FlowNodeRelation.objects.filter(result_table_id=self.result_table_id, node_type__in=category)[0]
            return NODE_FACTORY.get_node_handler(node_id=frt.node_id)
        except IndexError:
            return None

    @property
    def result_table_id(self):
        result_table_ids = self.result_table_ids
        if not result_table_ids:
            raise NodeError(_("获取节点关联RT失败"))
        return self.result_table_ids[0]

    @property
    def processing_type(self):
        """
        RT数据源的processing通过元数据API查询
        @return:
        """
        o_rt = ResultTable(self.result_table_id)
        return o_rt.processing_type
