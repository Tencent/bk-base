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
import json
from unittest import mock

from auth.models.auth_models import UserRole
from auth.models.audit_models import AuthAuditRecord
from auth.tasks.audit import audit_invalid_user_task
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse


class AuditTaskCase(BaseTestCase):
    @mock.patch("auth.api.meta.MetaApi.entity_complex_search")
    def test_invalid_user(self, patch_search):
        UserRole.objects.create(user_id="user01", role_id="bkdata.superuser")
        patch_search.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "data": {
                        "target": [
                            {
                                "staff_name": "user01",
                                "status_id": "2",
                                "sync_time": "2021-06-16T12:44:12.900368Z",
                            }
                        ]
                    }
                },
            }
        )

        audit_invalid_user_task()

        records = AuthAuditRecord.objects.all()
        self.assertTrue(len(records) > 0)

        for _record in records:
            self.assertTrue(len(json.loads(_record.audit_log)) > 0)
