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

import json

from dataflow.flow.models import FlowUdfLog
from dataflow.flow.utils.local import activate_request
from dataflow.shared.log import flow_logger as logger


class UdfBaseHandler(object):
    MAX_WAITING_SECONDS = 60 * 5

    def __init__(self, debugger_id):
        self.debugger_id = debugger_id
        self.debugger = FlowUdfLog.objects.get(id=debugger_id)
        self.function_name = self.debugger.__name__

    @classmethod
    def create(cls, func_name, operator, action, context, version=None):
        """
        @param func_name:
        @param operator:
        @param action:
        @param context:
            {
                'calculation_types': calculation_types,
                'debug_data': debug_data,
                'sql': sql
            }
        @param version:
        @return:
        """
        kwargs = {
            "func_name": func_name,
            "created_by": operator,
            "action": action,
            "context": json.dumps(context),
        }
        if version is not None:
            kwargs["version"] = version

        o_debugger = FlowUdfLog.objects.create(**kwargs)
        return cls(o_debugger.id)

    def execute(self):
        try:
            activate_request(self.debugger.created_by)
            self.debugger.set_status(FlowUdfLog.STATUS.RUNNING)
            self.execute_inner()
        except Exception as e:
            logger.exception(e)
            self.debugger.set_status(FlowUdfLog.STATUS.FAILURE)
            raise e

    def execute_inner(self):
        raise NotImplementedError("`execute_inner()` must be implemented.")

    @property
    def context(self):
        _context = self.debugger.get_context()
        return {} if _context is None else _context

    def save_context(self, context):
        """
        保存节点调试上下文（输入参数）
        """
        self.debugger.save_context(context)

    def log(self, display_id, msg, level="INFO", time=None, stage="1/5", detail=""):
        self.debugger.add_log(display_id, msg, level=level, time=time, stage=stage, detail=detail)
