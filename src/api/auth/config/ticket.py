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
from django.utils.translation import ugettext_lazy as _


@attr.s
class TicketStep:
    process_role_id = attr.ib(type=str)
    process_scope_id = attr.ib(type=str, default=None)
    paased_by_system = attr.ib(type=str, default=False)


@attr.s
class TicketFlow:
    ticket_type = attr.ib(type=str)
    ticket_object = attr.ib(type=str)
    ticket_name = attr.ib(type=str)
    steps = attr.ib(type=list)


APPLY_ROLE = "apply_role"
PROJECT_BIZ = "project_biz"
TOKEN_DATA = "token_data"
BATCH_RECALC = "batch_recalc"
VERIFY_TDM_DATA = "verify_tdm_data"
RESOURCE_GROUP_USE = "use_resource_group"
RESOURCE_GROUP_CREATE = "create_resource_group"
RESOURCE_GROUP_EXPAND = "expand_resource_group"


TICKET_FLOWS = [
    TicketFlow(ticket_type=PROJECT_BIZ, ticket_name=_("项目申请业务数据"), ticket_object="ProjectDataTicketObj", steps=[]),
    TicketFlow(
        ticket_type=RESOURCE_GROUP_USE, ticket_name=_("资源组授权申请"), ticket_object="ResourceGroupTicketObj", steps=[]
    ),
    TicketFlow(ticket_type=APPLY_ROLE, ticket_name=_("申请角色"), ticket_object="RoleTicketObj", steps=[]),
    TicketFlow(ticket_type=TOKEN_DATA, ticket_name=_("授权码申请权限"), ticket_object="DataTokenTicketObj", steps=[]),
    TicketFlow(
        ticket_type=BATCH_RECALC,
        ticket_name=_("离线补录申请"),
        ticket_object="BatchReCalcObj",
        steps=[TicketStep(process_role_id="bkdata.batch_manager")],
    ),
    TicketFlow(
        ticket_type=VERIFY_TDM_DATA,
        ticket_name=_("TDM原始数据接入申请"),
        ticket_object="CommonTicketObj",
        steps=[TicketStep(process_role_id="bkdata.tdm_manager")],
    ),
    TicketFlow(
        ticket_type=RESOURCE_GROUP_CREATE,
        ticket_name=_("资源组创建申请"),
        ticket_object="CommonTicketObj",
        steps=[TicketStep(process_role_id="biz.manager"), TicketStep(process_role_id="bkdata.resource_manager")],
    ),
    TicketFlow(
        ticket_type=RESOURCE_GROUP_EXPAND,
        ticket_name=_("资源组扩容申请"),
        ticket_object="CommonTicketObj",
        steps=[TicketStep(process_role_id="biz.manager"), TicketStep(process_role_id="bkdata.ops")],
    ),
]


TICKET_FLOW_CONFIG = {_tf.ticket_type: _tf for _tf in TICKET_FLOWS}

COMMON_TICKET_TYPES = [
    _tf.ticket_type for _tf in TICKET_FLOWS if _tf.ticket_object in ["CommonTicketObj", "BatchReCalcObj"]
]
TICKET_TYPE_CHOICES = [(_tf.ticket_type, _tf.ticket_name) for _tf in TICKET_FLOWS]
