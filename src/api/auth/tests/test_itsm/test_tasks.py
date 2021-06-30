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
from auth.tasks.sync_itsm_ticket_status import sync_itsm_ticket_status
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse

COMMON_TICKET_DATA = {
    "ticket_type": "module_apply",
    "created_by": "test1",
    "itsm_service_id": "77",
    "callback_url": "http://www.xxxx.com",
    "itsm_sn": "NO2019090519542603",
    "fields": {
        "title": "角色申请",
        "role_id": "result_table.manager",
        "user_id": "test1",
        "scope_id": "591_test_rt",
        "apply_reason": "hello",
        "action": "add_role",
    },
}


class TaskTest(BaseTestCase):
    def setUp(self):
        AuthCommonTicket.objects.create(**COMMON_TICKET_DATA)

    @mock.patch("auth.api.ITSMApi.get_ticket_status")
    def test_sync_itsm_ticket_status(self, patch_get_ticket_status):

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

        sync_itsm_ticket_status()

        ticket = AuthCommonTicket.objects.first()

        self.assertEqual(ticket.itsm_status, "SUSPENDED")
