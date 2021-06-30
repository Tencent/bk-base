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
from dataflow.flow.tasks.debug import ComputingNodeDebugger
from dataflow.shared.stream.stream_helper import StreamHelper


class StreamNodeDebugger(ComputingNodeDebugger):
    debug_type = "stream"

    def __init__(self, flow_debugger, _, **kwargs):
        """
        @param {FlowDebuggerHandler} flow_debugger
        """
        # 初始化StreamNodeDebugger时传空list，因此这里要先初始化o_nodes => nodes
        o_nodes = flow_debugger.flow.get_stream_nodes(ordered=True)
        super(StreamNodeDebugger, self).__init__(flow_debugger, o_nodes, **kwargs)

    def build_context(self):
        """
        实时任务启动参数，由 DataFlow 整体配置给出
        """
        operator = self.flow_debugger.debugger.created_by
        flow = self.flow_debugger.flow

        # 提取所有实时信息
        head_rts, tail_rts = flow.get_topo_head_tail_rts()
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

    def start_debug(self):
        """
        启动实时调试任务
        """
        request_dict = {
            "heads": ",".join(self.context["heads"]),
            "tails": ",".join(self.context["tails"]),
            "job_id": self.context["job_id"],
            "geog_area_code": self.context["geog_area_code"],
        }
        return StreamHelper.create_debug(**request_dict)["debug_id"]
