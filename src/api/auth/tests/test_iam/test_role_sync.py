# -*- coding:utf-8 -*-
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


import os
from unittest import mock

from auth.exceptions import BKIAMPolicesCountLimitErr
from auth.handlers.role.role_handler import RoleHandler
from auth.management.handlers.sync_handler import ActionSyncHandler
from auth.tests.utils import BaseTestCase
from django.conf import settings


class RoleSyncTestCase(BaseTestCase):
    def setUp(self):
        self.sync_action_handler = ActionSyncHandler()

    @mock.patch("auth.management.handlers.do_migrate.Client.query_all_models")
    @mock.patch("auth.management.handlers.do_migrate.Client.do_operation")
    def test_exec_migrate_actions(self, patch_do_operation, patch_query_all_models):
        patch_query_all_models.return_value = set(), set(), set(), set()
        patch_do_operation.return_value = True, "test ok"

        iam_files_path = os.path.join(settings.AUHT_PROJECT_DIR, "auth/config/iam_migrations")

        file_list = os.listdir(iam_files_path)

        self.sync_action_handler.exec_migrate(iam_files_path, file_list)
        self.assertTrue(patch_do_operation.call_count > 0)
        self.assertTrue(patch_query_all_models.call_count > 0)


class RoleHandlerTestCase(BaseTestCase):
    @mock.patch("auth.bkiam.sync.RoleSync.grant")
    def test_add_user(self, patch_grant):
        RoleHandler.SUPPORT_IAM = True
        RoleHandler.BKIAM_TOP_LIMIT = 10

        with self.assertRaises(BKIAMPolicesCountLimitErr):
            for i in range(20):
                RoleHandler.add_role("admin", "project.manager", "user123", i)

        self.assertEqual(patch_grant.call_count, 10)
