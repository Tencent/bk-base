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


import collections

import attr


@attr.s
class NoticeContent:
    """
    单据通知所需信息
    """

    # 申请人
    created_by = attr.ib(type=str, default="")
    # 申请类型（用于展示）
    ticket_type = attr.ib(type=str, default="")
    # 不同单据类型权限获得者有区别
    # 以gainer是否为空字符串判断common单据跟其他单据的区别
    gainer = attr.ib(type=str, default="")
    # 申请角色类单据/Token类单据，格式为object_class_name:action_name[角色]
    apply_content = attr.ib(type=str, default="")
    # 申请范围
    scope = attr.ib(type=list, default=attr.Factory(list))
    # 申请原因
    reason = attr.ib(type=str, default="")


if __name__ == "__main__":
    # example for NoticeContent attr
    scopes = ["[scope_id33333]scope_content", "[scope_id333]scope_content"]
    a = NoticeContent(
        created_by="12343",
        ticket_type="apply role",
        scope=scopes,
        reason="reason",
    )
    print(attr.asdict(a, dict_factory=collections.OrderedDict))
