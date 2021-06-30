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

from dataflow.flow.node_types import NodeTypes
from dataflow.flow.tasks.deploy.node_tasks.stream import StreamNodeTask
from dataflow.shared.meta.tag.tag_helper import TagHelper


class StreamSDKNodeTask(StreamNodeTask):
    def get_task_type(self):
        if self.nodes[0].node_type == NodeTypes.FLINK_STREAMING:
            return "Flink Code Streaming"
        elif self.nodes[0].node_type == NodeTypes.SPARK_STRUCTURED_STREAMING:
            return "Spark Structured Code Streaming"

    def build_start_context(self, flow_task_handler, o_nodes):
        """
        实时任务启动参数，由 DataFlow 整体配置给出
        """
        operator = flow_task_handler.operator
        task_context = flow_task_handler.context
        _flow_handler = flow_task_handler.flow
        # 申请 job_id
        consuming_mode = task_context["consuming_mode"] if "consuming_mode" in task_context else "continue"
        cluster_group = task_context["cluster_group"] if "cluster_group" in task_context else None
        geog_area_code = TagHelper.get_geog_area_code_by_project(_flow_handler.project_id)
        head_rts, tail_rts = (
            o_nodes[0].from_result_table_ids,
            o_nodes[0].result_table_ids,
        )
        job_id = _flow_handler.get_or_create_steam_job_id(
            _flow_handler.project_id,
            o_nodes,
            head_rts,
            tail_rts,
            geog_area_code,
            operator,
            consuming_mode,
            cluster_group,
        )

        return {
            "operator": operator,
            "job_id": job_id,
            "heads": ".".join(head_rts),
            "tails": ".".join(tail_rts),
            "geog_area_code": geog_area_code,
        }
