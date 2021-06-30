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


import attr


class AuthOP:
    EQ = "eq"
    IN = "in"
    LIKE = "like"


class BaseFilter:
    pass


@attr.s
class ResourceFilter(BaseFilter):
    """
    父级资源过滤器
    """

    resource_type = attr.ib(type=str)
    resource_id = attr.ib(type=str)


@attr.s
class ResourceAttrFilter(BaseFilter):
    """
    资源属性过滤器
    """

    func = attr.ib(type=str)
    key = attr.ib(type=str)
    value = attr.ib()


@attr.s
class ResourceIdFilter(BaseFilter):
    """
    资源 ID 过滤器
    """

    func = attr.ib(type=str)
    value = attr.ib()


@attr.s
class GlobalFilter(BaseFilter):
    """
    全局过滤器
    """

    pass
