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

from auth.models.ticketV2_models import AuthCommonTicket
from auth.tests.utils import BaseTestCase, is_api_success
from common.api.base import DataResponse

COMMON_TICKET_DATA = {
    "ticket_type": "module_apply",
    "created_by": "test1",
    "itsm_service_id": "77",
    "callback_url": "http://www.xxx.com",
    "itsm_sn": "NO2017287282603",
    "fields": {
        "title": "角色申请",
        "role_id": "result_table.manager",
        "user_id": "test1",
        "scope_id": "591_test_rt",
        "apply_reason": "hello",
        "action": "add_role",
    },
}


class TicketTestCase(BaseTestCase):
    def setUp(self):
        AuthCommonTicket.objects.create(**COMMON_TICKET_DATA)

    @mock.patch("auth.api.ITSMApi.create_ticket")
    @mock.patch("auth.api.ITSMApi.get_service_catalogs")
    @mock.patch("auth.api.ITSMApi.get_services")
    def test_create_ticket(self, patch_get_services, patch_get_service_catalogs, patch_create_ticket):
        patch_create_ticket.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"sn": "NO2017287282603"}, "result": True}
        )

        patch_get_service_catalogs.return_value = DataResponse(
            {
                "message": "success",
                "code": 0,
                "data": [
                    {
                        "children": [
                            {
                                "name": "蓝鲸计算平台",
                                "level": 1,
                                "children": [],
                                "key": "sxssjkhjlsklksjks",
                                "id": 30,
                                "desc": "",
                            }
                        ],
                        "name": "根目录",
                        "level": 0,
                    }
                ],
                "result": True,
            }
        )

        patch_get_services.return_value = DataResponse(
            {
                "message": "success",
                "code": 0,
                "data": [
                    {"service_type": "change", "desc": "", "id": 81, "name": "测试服务"},
                ],
                "result": True,
            }
        )

        params = {
            "ticket_type": "module_apply",
            "created_by": "test2",
            "itsm_service_name": "测试服务",
            "callback_url": "http://www.xxx.com",
            "fields": {
                "title": "角色申请",
                "role_id": "result_table.manager",
                "user_id": "test2",
                "scope_id": "591_test_rt",
                "apply_reason": "hello",
                "action": "add_role",
            },
        }
        resp = self.client.post("/v3/auth/tickets_v2/", data=params, format="json")

        data = is_api_success(self, resp)
        self.assertEqual(data["itsm_sn"], "NO2017287282603")

    @mock.patch("auth.api.ITSMApi.operate_ticket")
    @mock.patch("auth.api.ITSMApi.get_ticket_status")
    def test_operate_ticket(self, patch_get_ticket_status, patch_operate_ticket):
        patch_get_ticket_status.return_value = DataResponse(
            {
                "message": "success",
                "code": 0,
                "data": {
                    "ticket_url": "https://xxxx.com/#/commonInfo?id=6&activeNname=all&router=request",
                    "operations": [
                        {"can_operate": True, "name": "撤单", "key": "WITHDRAW"},
                        {"can_operate": True, "name": "恢复", "key": "UNSUSPEND"},
                    ],
                    "current_status": "SUSPENDED",
                    "current_steps": [],
                    "is_commented": True,
                },
                "result": True,
            }
        )

        patch_operate_ticket.return_value = DataResponse({"message": "success", "code": 0, "data": [], "result": True})

        id = AuthCommonTicket.objects.first().id

        params = {"action_type": "SUSPEND", "action_message": "test"}

        resp = self.client.post(f"/v3/auth/tickets_v2/{id}/operate/", data=params, format="json")

        data = is_api_success(self, resp)
        self.assertEqual(data, "ok")
        ticket = AuthCommonTicket.objects.first()
        self.assertEqual(ticket.itsm_status, "SUSPENDED")


class TicketCallBackTestCase(BaseTestCase):
    def setUp(self):
        AuthCommonTicket.objects.create(**COMMON_TICKET_DATA)

    @mock.patch("auth.api.ITSMApi.token_verify")
    @mock.patch("auth.itsm.backend.backend.CallBackend.callback")
    def test_call_back(self, patch_callback, patch_token_verify):
        patch_callback.return_value = True

        patch_token_verify.return_value = DataResponse(
            {"message": "success", "code": 0, "data": {"is_passed": True}, "result": True}
        )

        params = {
            "sn": "NO2017287282603",
            "title": "测试内置审批",
            "ticket_url": "#####",
            "current_status": "FINISHED",
            "updated_by": "admin",
            "update_at": "2020-08-31 20:57:22",
            "approve_result": True,
            "token": "abccccaaa",
        }

        resp = self.client.post("/v3/auth/tickets_v2/call_back/", data=params, format="json")

        is_api_success(self, resp)
