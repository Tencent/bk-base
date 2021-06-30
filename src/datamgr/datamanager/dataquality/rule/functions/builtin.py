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

from collections import Iterable

from common.exceptions import ValidationError
from dataquality.model.base import FunctionType
from dataquality.rule.base import Function


class BuiltinFunction(Function):
    TYPE = FunctionType.BUILTIN


class Addition(BuiltinFunction):
    NAME = "addition"
    ALIAS = "+"
    LEAST_PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        result = 0
        for arg in args:
            result += arg
        return result


class Subtraction(BuiltinFunction):
    NAME = "subtraction"
    ALIAS = "-"
    LEAST_PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        subtracted = args[0]
        for arg in args[1:]:
            subtracted -= arg
        return subtracted


class Multiplication(BuiltinFunction):
    NAME = "multiplication"
    ALIAS = "*"
    LEAST_PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        result = 1
        for arg in args:
            result *= arg
        return result


class Division(BuiltinFunction):
    NAME = "division"
    ALIAS = "/"
    LEAST_PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        divided = args[0]
        for arg in args[1:]:
            divided /= arg
        return divided


class Greater(BuiltinFunction):
    NAME = "greater"
    ALIAS = ">"
    PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        return args[0] > args[1]


class GreaterOrEqual(BuiltinFunction):
    NAME = "greater_or_equal"
    ALIAS = ">="
    PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        return args[0] >= args[1]


class Less(BuiltinFunction):
    NAME = "less"
    ALIAS = "<"
    PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        return args[0] < args[1]


class LessOrEqual(BuiltinFunction):
    NAME = "less_or_equal"
    ALIAS = "<="
    PARAMETERS_COUNT = 2
    ONLY_NUMBER = True

    def execute(self, *args):
        return args[0] <= args[1]


class In(BuiltinFunction):
    NAME = "in"
    ALIAS = "in"
    PARAMETERS_COUNT = 2

    def execute(self, *args):
        return args[0] in args[1]

    def validate(self, *args):
        if isinstance(args[1], Iterable):
            raise ValidationError("The second parameter must be iterable")


class And(BuiltinFunction):
    NAME = "and"
    ALIAS = "and"
    PARAMETERS_COUNT = 2

    def execute(self, *args):
        return args[0] and args[1]


class Or(BuiltinFunction):
    NAME = "or"
    ALIAS = "or"
    PARAMETERS_COUNT = 2

    def execute(self, *args):
        return args[0] or args[1]


class Not(BuiltinFunction):
    NAME = "not"
    ALIAS = "not"
    PARAMETERS_COUNT = 1

    def execute(self, *args):
        return not args[0]


BUILTIN_FUNCTIONS = {
    Addition.NAME: Addition,
    Subtraction.NAME: Subtraction,
    Multiplication.NAME: Multiplication,
    Division.NAME: Division,
    And.NAME: And,
    Or.NAME: Or,
    Not.NAME: Not,
    In.NAME: In,
    Greater.NAME: Greater,
    GreaterOrEqual.NAME: GreaterOrEqual,
    Less.NAME: Less,
    LessOrEqual.NAME: LessOrEqual,
}
