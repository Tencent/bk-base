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
from __future__ import absolute_import, print_function, unicode_literals

import logging

import mock

from auth.sync_business import sync_members
from tests import BaseTestCase

logger = logging.getLogger(__name__)


class TestBusinessSyncTask(BaseTestCase):
    @mock.patch("auth.sync_business.RoleController.update_role_members")
    @mock.patch("auth.sync_business.RoleController.get_role_members_with_all_scope")
    @mock.patch("auth.sync_business.load_v1_apps")
    @mock.patch("auth.sync_business.load_v3_apps")
    def test_sync_members(
        self,
        patch_v3_apps,
        patch_v1_apps,
        patch_role_members_with_all_scope,
        patch_update_role_members,
    ):
        patch_v3_apps.return_value = []
        patch_v1_apps.return_value = [
            {
                "bk_biz_id": 1,
                "manager": ["user01", "user02", "user03"],
                "developer": ["user04"],
                "tester": [],
                "productor": ["user05", "user06"],
            },
            {
                "bk_biz_id": 2,
                "manager": ["user04", "user02", "user03"],
                "developer": ["user04"],
                "tester": ["user06"],
                "productor": ["user05", "user06"],
            },
        ]
        patch_role_members_with_all_scope.return_value = {
            "1": ["user05", "user06"],
            "2": ["user05", "user06"],
            "3": ["user01", "user10"],
            "4": ["user01", "user10"],
        }

        sync_members()
        assert patch_update_role_members.call_count == 6
