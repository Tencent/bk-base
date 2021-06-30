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
import operator
from functools import reduce

from auth.handlers.resource.manager import (
    AuthOP,
    GlobalFilter,
    ResourceAttrFilter,
    ResourceFilter,
    ResourceIdFilter,
)
from django.db.models import Q
from iam import OP
from iam.eval.constants import KEYWORD_BK_IAM_PATH_FIELD_SUFFIX


class AuthConverter:
    def __init__(self):
        super().__init__()

    def convert_field(self, field):
        """
        转换字段格式，raw_data.xxx -> xxxx
        """
        return field.split(".")[1]

    def _and(self, content):
        # print("in _and we got", [self.convert(c) for c in content])
        return reduce(operator.and_, [self.convert(c) for c in content])

    def _or(self, content):
        # print("in _or we got", [self.convert(c) for c in content])
        return reduce(operator.or_, [self.convert(c) for c in content])

    def convert(self, data):
        # 空字典处理
        if "op" not in data:
            return None

        op = data["op"]

        if op == OP.AND:
            return self._and(data["content"])
        elif op == OP.OR:
            return self._or(data["content"])

        value = data["value"]
        field = data["field"]

        # 路径表达式特殊处理
        if field.endswith(KEYWORD_BK_IAM_PATH_FIELD_SUFFIX) and op == OP.STARTS_WITH:
            field = value[1:-1].split(",")[0]
            value = value[1:-1].split(",")[1]
            return Q(filter=ResourceFilter(field, value))

        if op == OP.ANY:
            return Q(filter=GlobalFilter())

        field = self.convert_field(field)
        op = {OP.EQ: AuthOP.EQ, OP.IN: AuthOP.IN, OP.CONTAINS: AuthOP.LIKE}.get(op)

        if op is None:
            raise ValueError("Invalid OP %s" % op)

        if field == "id":
            return Q(filter=ResourceIdFilter(op, value))

        return Q(filter=ResourceAttrFilter(op, field, value))
