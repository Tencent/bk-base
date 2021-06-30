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
from dataflow.shared.storekit.storekit_helper import StorekitHelper

from . import BaseNodeTaskHandler


class ClickHouseNodeTask(BaseNodeTaskHandler):
    def get_task_type(self):
        return "ClickHouse"

    def build_start_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]

        return {
            "operator": flow_task_handler.operator,
            "result_table_id": result_table_id,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]

        return {
            "operator": flow_task_handler.operator,
            "result_table_id": result_table_id,
        }

    def start_inner(self):
        task_type = self.get_task_type()
        result_table_id = self.context["result_table_id"]
        self.log(Bilingual(ugettext_noop("检查 {task_type} 存储")).format(task_type=task_type))
        StorekitHelper.prepare(result_table_id, task_type.lower())
        self.log(Bilingual(ugettext_noop("启动 {task_type} 分发进程")).format(task_type=task_type))
        DatabusHelper.create_task(self.context["result_table_id"], [task_type.lower()])

    def stop_inner(self):
        task_type = self.get_task_type()
        self.log(Bilingual(ugettext_noop("停止 {task_type} 分发进程")).format(task_type=task_type))
        DatabusHelper.stop_task(self.context["result_table_id"], [task_type.lower()])
