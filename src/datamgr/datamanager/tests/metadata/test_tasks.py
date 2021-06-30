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

import mock

from metadata.sync_staff_info import UserManager
from metadata.renew_bizes import renew_bizes
from metadata.sync_bip_grade_info import BipGradeManager

from tests import BaseTestCase


class TestTask(BaseTestCase):
    def test_sync_new_staff_info(self):
        UserManager().sync_new_user_tof_info()

    @mock.patch("common.meta.metadata_client.query")
    def test_refresh_existed_staff_info(self, query_patch):
        query_patch.return_value = {
            "extensions": {
                "txn": {"start_ts": 321913},
                "server_latency": {
                    "parsing_ns": 7682,
                    "processing_ns": 24540273,
                    "encoding_ns": 3394993,
                },
            },
            "data": {
                "target": [
                    {"Staff.login_name": "user01"},
                    {"Staff.login_name": "user02"},
                ]
            },
        }
        UserManager().refresh_existed_user_tof_info()

    @mock.patch("common.meta.metadata_client.query")
    def test_sync_bip_grade_info(self, query_patch):
        BipGradeManager().sync_bip_grade_info()

    def test_renew_bizs_info(self):
        renew_bizes()
