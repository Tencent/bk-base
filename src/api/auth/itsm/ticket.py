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
from datetime import datetime

from auth.exceptions import (
    CallBackError,
    ParameterErr,
    TicketCreateError,
    UnexpectedTicketTypeErr,
)
from auth.itsm.backend.backend import ItsmBackend
from auth.itsm.config.contants import (
    CALL_BACK_URL,
    TICKET_CALLBACK_STATUS,
    TICKET_STATUS,
)
from auth.itsm.config.ticket_config import TICKET_FLOW_CONFIG
from auth.itsm.ticket_objects import ModuleTicketObj
from auth.models.ticketV2_models import AuthCommonTicket


class ItsmTicketManager:
    """
    ITSM工厂，处理view层传递的数据并返回view需要的数据
    """

    def __init__(self, ticket_type):
        self.ticket_type = ticket_type

        if ticket_type not in TICKET_FLOW_CONFIG:
            raise UnexpectedTicketTypeErr()

        self.child_ticket_obj = TICKET_FLOW_CONFIG.get(ticket_type)
        self.itsm_backend = ItsmBackend()

    def generate_ticket(self, params):
        """
        生成单据
        @param params:  {
            "ticket_type": "apply_role",
            "creator": "admin",
            "itsm_service_id": "77",
            "callback_url" : "http://xxx,
            "fields" : {
                "title": "角色申请",
                "role_id": "result_table.manager",
                "role_name": "",
                "user_id": "admin",
                "scope_id": "591_test_rt",
                "apply_reason": "hello",
                "action":"add_role"
            }
        }
        @return: {AuthCommonTicket}
        """
        if self.is_have_ticket(params):
            raise TicketCreateError()

        # 去itsm那边拿到单号
        params["itsm_sn"] = self.create_itsm_ticket(params)

        # 生成主单据
        ticket = AuthCommonTicket.objects.create(**params)

        # 生成子单
        self.generate_child_ticket(ticket)
        return ticket

    def is_have_ticket(self, params):
        """
        判断是否存在单据
        @param params {dict} 单据参数
        @paramExample
            {
                "ticket_type": "apply_role",
                "creator": "admin",
                "itsm_service_id": "77",
                "callback_url" : "http://xxx,
                "fields" : {
                    "title": "角色申请",
                    "role_id": "result_table.manager",
                    "role_name": "",
                    "user_id": "admin",
                    "scope_id": "591_test_rt",
                    "apply_reason": "hello",
                    "action":"add_role"
                }
            }
        @return: Boolean
        """
        tickets = AuthCommonTicket.objects.filter(
            created_by=params["created_by"],
            fields=params["fields"],
            itsm_status=TICKET_STATUS.RUNNING,
            itsm_service_id=params["itsm_service_id"],
        )

        if len(tickets) > 0:
            return True
        return False

    def generate_child_ticket(self, ticket):
        """
        生成子单据
        """
        self.child_ticket_obj.create_ticket(ticket)

    def create_itsm_ticket(self, params):
        """
        创建ITSM单据
        """
        kwargs = {
            "service_id": params["itsm_service_id"],
            "creator": params["created_by"],
            "fields": [{"key": key, "value": value} for key, value in list(params["fields"].items())],
            "meta": {
                "callback_url": CALL_BACK_URL,
            },
        }
        return self.itsm_backend.create_ticket(kwargs)

    def operate_ticket(self, instance, params):
        """
        操作单据
        """
        kwargs = {
            "sn": instance.itsm_sn,
            "action_type": params.get("action_type"),
            "action_message": params.get("action_message"),
            "operator": params.get("operator"),
        }
        self.itsm_backend.operate_ticket(kwargs)
        # 操作完单据之后会更新单据状态，更新失败会有定时任务更新
        self.update_ticket_status(instance)

    def update_ticket_status(self, ticket):
        """
        更新单据状态
        """
        api_params = {"sn": [ticket.itsm_sn]}
        ticket.itsm_status = self.itsm_backend.get_ticket_status({"sn": ticket.itsm_sn})
        if ticket.itsm_status == TICKET_STATUS.FINISHED:
            ticket.end_time = datetime.now()
            data = self.itsm_backend.get_ticket_approval_result(api_params)
            if len(data) != 0:
                ticket.approve_result = data[0]["approve_result"]
        ticket.save()

    def add_permission(self, ticket):
        try:
            # 如果是外部的单据，直接callback回去
            if ticket.ticket_type == ModuleTicketObj.ticket_type:
                self.child_ticket_obj.add_permission(ticket)
                self.update_ticket_callback_status(ticket)
            else:
                # 如果是内部的，确认一下是否审批成功
                if ticket.approve_result:
                    self.child_ticket_obj.add_permission(ticket)
        except CallBackError:
            self.update_ticket_callback_status(ticket, TICKET_CALLBACK_STATUS.FAIL)

    def update_ticket_callback_status(self, ticket, callback_status=TICKET_CALLBACK_STATUS.SUCCESS):
        ticket.callback_status = callback_status
        ticket.save()

    def handle_call_back(self, ticket, params):
        """
        处理 回调
        """
        if not self.itsm_backend.verify_token(token=params["token"]):
            raise ParameterErr("参数校验错误，token非法")

        # 如果ticket 已经不为running，代表单据被操作过或者被定时任务跑过了
        if ticket.approve_result is not None:
            return

        # 更新单据状态信息
        ticket.itsm_status = params.get("current_status")
        ticket.approve_result = params.get("approve_result")
        ticket.end_time = datetime.now()
        ticket.save()

        self.add_permission(ticket)
