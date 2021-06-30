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

from dataflow.udf.exceptions.comp_exceptions import FunctionNotExistsError
from dataflow.udf.models import BksqlFunctionConfigV3

# 函数有四种类型 类型 => 展示名
GROUP_DISPLAY = {
    "Aggregate Function": "Calculate a set of values and return a single value",
    "String Function": "String processing functions",
    "Mathematical Function": "Commonly used mathematical functions",
    "Conditional Function": "Commonly used conditional judgment functions",
    "Type Conversion Function": "Type Conversion Function",
    "Date and Time Function": "Date and Time Function",
    "Table-valued Function": "Table-valued Function",
    "Window Function": "Window Function",
    "Other Function": "User-defined Function",
}


def get_group_display(key):
    return GROUP_DISPLAY.get(key, "Not Supported Function Type")


def save(**kwargs):
    function = BksqlFunctionConfigV3(**kwargs)
    function.save()


def list_all():
    return BksqlFunctionConfigV3.objects.all()


def get(**kwargs):
    try:
        return BksqlFunctionConfigV3.objects.get(**kwargs)
    except BksqlFunctionConfigV3.DoesNotExist:
        raise FunctionNotExistsError()


def filter(**kwargs):
    return BksqlFunctionConfigV3.objects.filter(**kwargs)


def delete(function_name):
    BksqlFunctionConfigV3.objects.filter(func_name=function_name).delete()


def exists(**kwargs):
    return BksqlFunctionConfigV3.objects.filter(**kwargs).exists()


def filter_criterion(criterion, **kwargs):
    return BksqlFunctionConfigV3.objects.filter(criterion, **kwargs)
