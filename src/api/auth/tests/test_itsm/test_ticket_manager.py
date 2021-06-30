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

from unittest import mock

from auth.exceptions import ParameterErr, TicketCreateError
from auth.itsm.ticket import ItsmTicketManager
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse

CREATE_TICKET_PARAMS = {
    "ticket_type": "module_apply",
    "created_by": "admin",
    "itsm_service_id": "77",
    "callback_url": "http://xxx",
    "fields": {
        "title": "模块",
        "role_id": "result_table.manager",
        "role_name": "",
        "user_id": "admin",
        "scope_id": "591_test_rt",
        "apply_reason": "hello",
        "action": "add_role",
    },
}


class TestTicketManager(BaseTestCase):
    @mock.patch("auth.api.ITSMApi.create_ticket")
    def test_generate_ticket(self, patch_create_ticket):
        patch_create_ticket.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"sn": "NO20193872603"}, "result": True}
        )
        ticket = ItsmTicketManager(CREATE_TICKET_PARAMS["ticket_type"]).generate_ticket(CREATE_TICKET_PARAMS)
        self.assertEqual(ticket.itsm_sn, "NO20193872603")

        factory = ItsmTicketManager(CREATE_TICKET_PARAMS["ticket_type"])

        self.assertRaises(TicketCreateError, factory.generate_ticket, CREATE_TICKET_PARAMS)

    @mock.patch("auth.api.ITSMApi.operate_ticket")
    @mock.patch("auth.api.ITSMApi.get_ticket_status")
    @mock.patch("auth.api.ITSMApi.create_ticket")
    def test_operate_ticket(self, patch_create_ticket, patch_get_ticket_status, patch_operate_ticket):
        patch_create_ticket.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"sn": "NO20193872603"}, "result": True}
        )

        patch_get_ticket_status.return_value = DataResponse(
            {
                "message": "success",
                "code": 0,
                "data": {
                    "ticket_url": "https://xxx.com/#/commonInfo?id=6&activeNname=all&router=request",
                    "operations": [
                        {"can_operate": True, "name": "撤单", "key": "WITHDRAW"},
                        {"can_operate": True, "name": "恢复", "key": "UNSUSPEND"},
                    ],
                    "current_status": "SUSPENDED",
                    "is_commented": True,
                },
                "result": True,
            }
        )

        patch_operate_ticket.return_value = DataResponse({"message": "success", "code": 0, "data": [], "result": True})

        factory = ItsmTicketManager(CREATE_TICKET_PARAMS["ticket_type"])

        ticket = factory.generate_ticket(CREATE_TICKET_PARAMS)

        operate_params = {"action_type": "SUSPEND", "action_message": "test"}
        factory.operate_ticket(ticket, params=operate_params)

        self.assertEqual(ticket.itsm_status, "SUSPENDED")

    @mock.patch("auth.api.ITSMApi.token_verify")
    @mock.patch("auth.api.ITSMApi.create_ticket")
    @mock.patch("auth.itsm.backend.backend.CallBackend.callback")
    def test_handle_call_back(self, patch_callback, patch_create_ticket, patch_token_verify):

        patch_callback.return_value = True

        patch_create_ticket.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"sn": "NO20193872603"}, "result": True}
        )

        patch_token_verify.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"is_passed": False}, "result": True}
        )

        params = {
            "sn": "NO20193872603",
            "title": "测试内置审批",
            "ticket_url": "#####",
            "current_status": "FINISHED",
            "updated_by": "admin",
            "update_at": "2020-08-31 20:57:22",
            "approve_result": True,
            "token": "abccssss",
        }

        factory = ItsmTicketManager(CREATE_TICKET_PARAMS["ticket_type"])

        ticket = factory.generate_ticket(CREATE_TICKET_PARAMS)

        self.assertRaises(ParameterErr, factory.handle_call_back, ticket, params)

        patch_token_verify.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"is_passed": True}, "result": True}
        )

        factory.handle_call_back(ticket, params)

        self.assertEqual(ticket.itsm_status, "FINISHED")

        self.assertEqual(patch_callback.call_count, 1)
