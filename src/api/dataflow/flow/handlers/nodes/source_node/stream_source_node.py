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
from dataflow.flow.handlers.nodes.source_node.rt_source_node import RTSourceNode
from dataflow.flow.node_types import NodeTypes


class StreamSourceNode(RTSourceNode):
    node_type = NodeTypes.STREAM_SOURCE
    channel_storage_type = "kafka"
    default_storage_type = "hdfs"

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(StreamSourceNode, self).build_before(from_node_ids, form_data, is_create)
        o_rt = ResultTable(form_data["result_table_id"])
        if not o_rt.has_kafka():
            raise NodeError(_("作为实时数据源，需要具备KAFKA存储."))
        # 当前其它 processing_type 还包括 batch, batch_model, storage, model_app
        if o_rt.processing_type not in [
            "batch",
            "clean",
            "stream",
            "stream_model",
            "transform",
            "model_app",
        ]:
            raise NodeError(_("当前仅支持清洗结果表、实时结果表作为实时数据源."))
        return form_data
