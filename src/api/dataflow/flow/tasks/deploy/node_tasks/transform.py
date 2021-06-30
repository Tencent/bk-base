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

from django.utils.translation import ugettext_noop

from dataflow.flow.utils.language import Bilingual
from dataflow.shared.databus.databus_helper import DatabusHelper

from . import BaseNodeTaskHandler


class TransformNodeTask(BaseNodeTaskHandler):
    def build_start_context(self, flow_task_handler, o_nodes):
        processing_id = o_nodes[0].processing_id
        task_display = Bilingual.from_lazy(o_nodes[0].get_node_type_display())
        return {
            "task_type": "kafka",
            "task_display": task_display,
            "operator": flow_task_handler.operator,
            "processing_id": processing_id,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        processing_id = o_nodes[0].processing_id
        task_display = Bilingual.from_lazy(o_nodes[0].get_node_type_display())
        return {
            "task_type": "kafka",
            "task_display": task_display,
            "operator": flow_task_handler.operator,
            "processing_id": processing_id,
        }

    def start_inner(self):
        task_display = self.context["task_display"]
        if self.start_slightly:
            # 重新更新运行时配置
            self.log(Bilingual(ugettext_noop("刷新 {task_display} 分发进程配置")).format(task_display=task_display))
            DatabusHelper.refresh_datanode(self.context["processing_id"])
        else:
            self.log(Bilingual(ugettext_noop("启动 {task_display} 分发进程")).format(task_display=task_display))
            DatabusHelper.create_datanode_task(self.context["processing_id"])

    def stop_inner(self):
        task_type = self.context["task_type"]
        task_display = self.context["task_display"]
        self.log(Bilingual(ugettext_noop("停止 {task_display} 分发进程")).format(task_display=task_display))
        DatabusHelper.stop_task(self.context["processing_id"], [task_type.lower()])
