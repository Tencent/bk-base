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

from django.conf import settings

from auth.constants import FAILED, PROCESSING, STOPPED, SUCCEEDED
from auth.core.ticket_objects import CommonTicketObj
from auth.exceptions import TicketCallbackErr
from auth.models import Ticket, TicketState, UserRole
from auth.tests.utils import BaseTestCase
from auth.utils import generate_md5
from common.api.base import DataResponse


class CommonTicketTest(BaseTestCase):
    multi_db = True

    def setUp(self):
        super().setUp()
        UserRole.objects.create(
            user_id="admin",
            role_id="bkdata.batch_manager",
        )

        self.dataflow_send_confirm = DataResponse({"result": True, "message": "被调用", "errors": None, "code": "1500200"})

    def test_create_common_ticket_api(self):
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "request_user",
                "reason": "test",
                "ticket_type": "batch_recalc",
                "process_id": "111_xx",
                "content": "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
            },
            format="json",
        )
        self.is_api_success(resp)
        self.assertGreater(Ticket.objects.filter(created_by="request_user").count(), 0)
        return resp.data["data"]["id"]

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_approve_common_state_api(self, patch1):
        patch1.result_value = self.dataflow_send_confirm
        ticket_id = self.test_create_common_ticket_api()
        state = TicketState.objects.filter(ticket=ticket_id, status=PROCESSING).first()
        resp = self.client.post(
            f"/v3/auth/ticket_states/{state.pk}/approve/",
            {"bk_username": "admin", "status": SUCCEEDED, "process_message": "ok"},
            format="json",
        )
        data = self.is_api_success(resp)
        self.assertEqual(data.get("status"), SUCCEEDED)
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, SUCCEEDED)
        self.assertEqual(patch1.call_count, 1)

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_common_ticket_api_autoapprove(self, patch1):
        patch1.result_value = self.dataflow_send_confirm
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "admin",
                "reason": "test",
                "ticket_type": "batch_recalc",
                "process_id": "111_xx",
                "auto_approve": False,
                "content": "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
            },
            format="json",
        )
        self.is_api_success(resp)
        ticket = Ticket.objects.filter(created_by="admin")
        self.assertGreater(ticket.count(), 0)
        self.assertEqual(ticket[0].status, PROCESSING)

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_common_ticket_api_sys_approve(self, patch1):
        patch1.result_value = self.dataflow_send_confirm
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "user01",
                "reason": "test",
                "ticket_type": "batch_recalc",
                "process_id": "111_xx",
                "auto_approve": True,
                "ticket_step_params": {"0&paased_by_system": True},
                "content": "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
            },
            format="json",
        )
        self.is_api_success(resp)
        tickets = Ticket.objects.filter(created_by="user01")
        self.assertGreater(tickets.count(), 0)
        self.assertEqual(tickets[0].status, SUCCEEDED)

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_withdraw_common_state_api(self, patch1):
        patch1.result_value = self.dataflow_send_confirm
        ticket_id = self.test_create_common_ticket_api()
        state = TicketState.objects.filter(ticket=ticket_id, status=PROCESSING).first()
        resp = self.client.post(
            f"/v3/auth/tickets/{ticket_id}/withdraw/",
            {"bk_username": "request_user", "process_message": "withdraw"},
            format="json",
        )
        data = self.is_api_success(resp)
        self.assertEqual(data.get("status"), STOPPED)
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, STOPPED)
        self.assertEqual(patch1.call_count, 1)

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_common_state_api_reject(self, patch1):
        patch1.result_value = self.dataflow_send_confirm
        ticket_id = self.test_create_common_ticket_api()
        state = TicketState.objects.filter(ticket=ticket_id, status=PROCESSING).first()
        resp = self.client.post(
            f"/v3/auth/ticket_states/{state.pk}/approve/",
            {"bk_username": "admin", "status": FAILED, "process_message": "no"},
            format="json",
        )
        data = self.is_api_success(resp)
        self.assertEqual(data.get("status"), FAILED)
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, FAILED)
        self.assertEqual(patch1.call_count, 1)

    def test_list_api(self):
        """
        测试获取通用单据列表
        """
        ticket_id = self.test_create_common_ticket_api()
        resp = self.client.get("/v3/auth/tickets/?ticket_type=batch_recalc&status=processing&process_id=111_xx")
        data = self.is_api_success(resp)
        self.assertEqual(ticket_id, data[0]["id"])

    def test_common_callback_request_yes(self):
        """
        测试通用单据的通用回调函数是否符合预期
        """
        ticket_id = 1111
        data = {}
        callbcak_url = "http://www.xxx.com"

        with self.assertRaises(TicketCallbackErr):
            CommonTicketObj(ticket_type="bkdata.tdm_manager").callback_request(callbcak_url, ticket_id, data)

    @mock.patch("requests.Session.post")
    def test_common_callback_request_no(self, patch_post):
        class ResponseSample:
            content = "fasdf"
            status_code = 200
            text = "fasdf"

            def json(self):
                return {"result": True}

        patch_post.return_value = ResponseSample()

        callbcak_url = "http://www.xxx.com"
        ticket_id = 1111
        data = {}

        CommonTicketObj(ticket_type="bkdata.tdm_manager").callback_request(callbcak_url, ticket_id, data)

    @mock.patch("auth.api.DataflowApi.batch_recalc_callback")
    def test_wx_recall_approve_api(self, patch1):
        """
        测试微信审核回调接口
        """
        patch1.result_value = self.dataflow_send_confirm
        ticket_id = self.test_create_common_ticket_api()

        encryp_str = str(ticket_id) + settings.APP_TOKEN
        md5_str = generate_md5(encryp_str)
        taskid = "{}:{}:{}".format("bkdata", ticket_id, md5_str)
        resp = self.client.post(
            "/v3/auth/ticket_states/wx_recall/",
            {"verifier": "admin", "result": "1", "taskid": taskid, "message": ""},
            format="json",
        )
        data = self.is_api_success(resp)
        self.assertEqual(data.get("status"), SUCCEEDED)

        state = TicketState.objects.filter(ticket=ticket_id).first()
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, SUCCEEDED)
        self.assertEqual(patch1.call_count, 1)


class VerifyTdmDataTicketTest(BaseTestCase):
    multi_db = True

    def setUp(self):
        super().setUp()
        UserRole.objects.create(
            user_id="admin",
            role_id="bkdata.tdm_manager",
        )

        self.success_response = DataResponse({"result": True, "message": "ok", "errors": None, "code": "1500200"})

    def test_create_api(self):
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "request_user",
                "reason": "test",
                "ticket_type": "verify_tdm_data",
                "process_id": "111",
                "content": "用户要接入 TDM XX 表数据，是否允许",
                "callback_url": "http://localhost/v3/meta/callback/",
            },
            format="json",
        )
        self.is_api_success(resp)
        self.assertGreater(Ticket.objects.filter(created_by="request_user").count(), 0)
        return resp.data["data"]["id"]

    @mock.patch("auth.core.ticket_objects.CommonTicketObj.callback_request")
    def test_approve_api(self, patch_callback_request):
        for status in [SUCCEEDED, FAILED]:
            ticket_id = self.test_create_api()
            state = TicketState.objects.filter(ticket=ticket_id, status=PROCESSING).first()
            resp = self.client.post(
                f"/v3/auth/ticket_states/{state.pk}/approve/",
                {"bk_username": "admin", "status": status, "process_message": "ok"},
                format="json",
            )
            data = self.is_api_success(resp)
            self.assertEqual(data.get("status"), status)
            state = TicketState.objects.filter(pk=state.pk).first()
            self.assertEqual(state.status, status)

            self.assertEqual(patch_callback_request.call_args[0][0], "http://localhost/v3/meta/callback/")
            self.assertEqual(patch_callback_request.call_args[0][1], ticket_id)
            self.assertEqual(
                patch_callback_request.call_args[0][2],
                {"bk_username": "admin", "operator": "admin", "status": status, "message": "ok", "process_id": "111"},
            )


class ResourceGroupTicketTest(BaseTestCase):
    multi_db = True

    def setUp(self):
        super().setUp()
        UserRole.objects.create(user_id="user01", role_id="bkdata.resource_manager")
        UserRole.objects.create(user_id="user02", role_id="bkdata.ops")
        UserRole.objects.create(user_id="user03", role_id="biz.manager", scope_id="1010")

    def test_create_resource_group_request(self):
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "request_user",
                "reason": "test",
                "ticket_type": "create_resource_group",
                "ticket_step_params": {"0&process_scope_id": "1010"},
                "process_id": "1111111",
                "content": "用户jere想要创建资源组xxx",
                "callback_url": "http://localhost/v3/meta/callback/",
            },
            format="json",
        )
        self.is_api_success(resp)

        ticket_id = resp.data["data"]["id"]
        ticket = Ticket.objects.get(id=ticket_id)
        states = TicketState.objects.filter(ticket=ticket)
        self.assertIn("user03", states[0].processors)
        self.assertIn("user01", states[1].processors)

        return ticket_id

    def test_expand_resource_group_request(self):
        resp = self.client.post(
            "/v3/auth/tickets/create_common/",
            {
                "bk_username": "request_user",
                "reason": "test",
                "ticket_type": "expand_resource_group",
                "ticket_step_params": {"0&process_scope_id": "1010"},
                "process_id": "1111111",
                "content": "用户jere想要创建资源组xxx",
                "callback_url": "http://localhost/v3/meta/callback/",
            },
            format="json",
        )
        self.is_api_success(resp)

        ticket_id = resp.data["data"]["id"]
        ticket = Ticket.objects.get(id=ticket_id)
        states = TicketState.objects.filter(ticket=ticket)
        self.assertIn("user03", states[0].processors)
        self.assertIn("user02", states[1].processors)

        return ticket_id
