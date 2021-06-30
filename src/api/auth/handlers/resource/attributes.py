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


from abc import ABCMeta

import attr
from auth.constants import SensitivityConfigs
from django.utils.translation import ugettext_lazy as _


@attr.s
class ScopeAttribute(metaclass=ABCMeta):
    """
    权限控制模块中的可作为范围限定的属性管理类
    """

    key = attr.ib(type=str)
    name = attr.ib(type=str)


@attr.s
class ConstanceScopeAttribute(ScopeAttribute):
    items = attr.ib(type=list)


@attr.s
class ResourceScopeAttribute(ScopeAttribute):
    related_resource = attr.ib(type=str)


sensitivity_attribute = ConstanceScopeAttribute(
    key="sensitivity",
    name=_("敏感度"),
    items=[{"id": c["id"], "display_name": c["name"]} for c in SensitivityConfigs if c["active"]],
)

bk_biz_id_attribute = ResourceScopeAttribute(key="bk_biz_id", name=_("业务ID"), related_resource="biz")

processing_type_attribute = ResourceScopeAttribute(
    key="processing_type", name=_("处理类型"), related_resource="process_type"
)
