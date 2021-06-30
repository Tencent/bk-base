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
from dataflow.shared.model.model_helper import ModelHelper

from . import BaseNodeTaskHandler


class ModelNodeTask(BaseNodeTaskHandler):
    def build_start_context(self, flow_task_handler, o_nodes):
        return {
            "processing_id": o_nodes[0].processing_id,
            "operator": flow_task_handler.operator,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        return {
            "processing_id": o_nodes[0].processing_id,
            "operator": flow_task_handler.operator,
        }

    def start_inner(self):
        processing_id = self.context["processing_id"]
        operator = self.context["operator"]
        self.log(Bilingual(ugettext_noop("启动算法模型实例")))
        api_params = {"submitted_by": operator, "processing_id": processing_id}
        ModelHelper.start_job(**api_params)

    def stop_inner(self):
        processing_id = self.context["processing_id"]
        operator = self.context["operator"]
        self.log(Bilingual(ugettext_noop("停止算法模型实例")))
        api_params = {"submitted_by": operator, "processing_id": processing_id}
        ModelHelper.stop_job(**api_params)
