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
from dataflow.flow.handlers.nodes.source_node.rt_source_node import RTSourceNode
from dataflow.flow.node_types import NodeTypes


class BatchKVSourceNode(RTSourceNode):
    node_type = NodeTypes.BATCH_KV_SOURCE
    node_form = forms.StandardSourceForm
    config_attrs = ["result_table_id", "bk_biz_id", "name", "from_nodes"]

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(BatchKVSourceNode, self).build_before(from_node_ids, form_data, is_create)

        if not ResultTable(form_data["result_table_id"]).has_storage("ignite"):
            raise NodeError(_("作为关联数据源，需要具备 ignite 存储."))
        return form_data
