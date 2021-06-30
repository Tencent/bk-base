# # -*- coding:utf-8 -*-
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
from os.path import abspath, dirname
from unittest import mock

from auth.management.handlers.sync_handler import ActionSyncHandler
from auth.tests.utils import BaseTestCase


class RoleSyncTestCase(BaseTestCase):
    def setUp(self):
        self.sync_action_handler = ActionSyncHandler()

    @mock.patch("auth.management.handlers.do_migrate.Client.query_all_models")
    @mock.patch("auth.management.handlers.do_migrate.Client.do_operation")
    def test_exec_migrate_actions(self, patch_do_operation, patch_query_all_models):
        patch_query_all_models.return_value = set(), set(), set(), set()
        patch_do_operation.return_value = True, "test ok"
        cur_path = dirname(dirname(abspath(__file__)))
        base_dir = os.path.abspath(os.path.join(cur_path, ".."))
        file_path = os.path.join(base_dir, "config/iam_migrations/")
        file_list = os.listdir(file_path)

        self.sync_action_handler.exec_migrate(file_path, file_list)
        self.assertTrue(patch_do_operation.call_count > 0)
        self.assertTrue(patch_query_all_models.call_count > 0)
