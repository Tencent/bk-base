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
from sqlalchemy.sql.expression import true

from dataquality import session
from dataquality.exceptions import FunctionNotImplementError
from dataquality.model.base import FunctionType
from dataquality.model.core import AuditFunction
from dataquality.rule.functions.builtin import BUILTIN_FUNCTIONS


class FunctionManager(object):
    def __init__(self):
        self._functions = {}

        self.init_functions()

    def init_functions(self):
        audit_functions = session.query(AuditFunction)

        for function in audit_functions:
            if function.function_type == FunctionType.BUILTIN.value:
                if function.name in BUILTIN_FUNCTIONS:
                    self._functions[function.name] = BUILTIN_FUNCTIONS[function.name]()
            else:
                # TODO 动态加载函数
                self._functions[function.name] = self.dynamic_load_function(function)

    def get_function(self, function_name):
        function_name = function_name.lower()
        if function_name in self._functions:
            return self._functions[function_name]
        else:
            audit_function = (
                session.query(AuditFunction)
                .filter(
                    AuditFunction.function_name == function_name
                    and AuditFunction.active == true()
                )
                .first()
            )
            if not audit_function:
                raise FunctionNotImplementError(
                    message_kv={"function_name": function_name}
                )
            self._functions[function_name] = self.dynamic_load_function(audit_function)
            return self._functions[function_name]

    def dynamic_load_function(self, function):
        """
        Dynamic load rule function by configs from database.
        """
        raise NotImplementedError()
