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

from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.tasks.debug.stream import StreamNodeDebugger


class FlinkStreamingNodeDebugger(StreamNodeDebugger):
    def __init__(self, flow_debugger, o_nodes):
        """
        @param {FlowDebuggerHandler} flow_debugger
        @param {[NodeHandler]} o_nodes 节点对象列表
        """
        self.nodes = o_nodes
        self.flow_debugger = flow_debugger
        self.context = None

    def build_context(self):
        operator = self.flow_debugger.debugger.created_by
        flow = self.flow_debugger.flow
        head_rts = self.nodes[0].from_result_table_ids
        tail_rts = self.nodes[0].result_table_ids
        geog_area_code = self.get_geog_area_code()
        job_id = FlowHandler.get_or_create_steam_job_id(
            flow.project_id, self.nodes, head_rts, tail_rts, geog_area_code, operator
        )

        return {
            "job_id": job_id,
            "heads": head_rts,
            "tails": tail_rts,
            "geog_area_code": geog_area_code,
        }
