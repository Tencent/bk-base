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
from auth.itsm import ticket_objects
from auth.itsm.ticket_objects import BaseTicket


def register_ticket_obj(objs):
    """
    动态扫描ticket_objects 里面的资源类型，生成对应的map
    @return {
        'module_apply': obj
    }
    """
    ticket_flow_config = {}

    for item in dir(objs):

        if not item.endswith("TicketObj"):
            continue

        ticket_obj = getattr(objs, item)

        if issubclass(ticket_obj, BaseTicket) and ticket_obj.ticket_type is not None:
            ticket_flow_config[ticket_obj.ticket_type] = ticket_obj()

    return ticket_flow_config


TICKET_FLOW_CONFIG = register_ticket_obj(ticket_objects)
