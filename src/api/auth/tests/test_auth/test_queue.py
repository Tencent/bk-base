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
from unittest import mock

from auth.models import AuthDataToken, AuthDataTokenPermission, DataTokenQueueUser
from auth.tasks.sync_queue_auth import (
    add_queue_auth,
    extract_kafka_queue_auth,
    extract_token_queue_auth,
    sync_queue_by_project,
)
from auth.tests.utils import BaseTestCase
from auth.utils import generate_random_string
from common.api.base import DataResponse

SUCCESS_RESPONSE = DataResponse({"result": True, "data": True, "message": "ok", "errors": None, "code": "1500200"})


class QueueTestCase(BaseTestCase):
    def generate_token(self):
        return AuthDataToken.objects.create(
            data_token=generate_random_string(64),
            data_token_bk_app_code="demo_app",
            status="enabled",
            created_by="admin",
            created_at=datetime.now(),
            updated_by="admin",
            updated_at=datetime.now(),
        )

    @mock.patch("auth.api.DatabusApi.add_queue_user")
    def test_create_queue_user(self, patch_add_queue_user):
        patch_add_queue_user.return_value = SUCCESS_RESPONSE

        o_token = self.generate_token()

        queue_user1 = DataTokenQueueUser.get_or_init(o_token.data_token)
        queue_user2 = DataTokenQueueUser.get_or_init_by_id(o_token.id)

        self.assertEqual(queue_user1.queue_user, queue_user2.queue_user)

    @mock.patch("auth.api.DatabusApi.add_queue_auth")
    @mock.patch("auth.api.DatabusApi.add_queue_user")
    def test_register_result_tables(self, patch_add_queue_user, patch_add_queue_auth):
        patch_add_queue_auth.return_value = SUCCESS_RESPONSE
        patch_add_queue_user.return_value = SUCCESS_RESPONSE

        o_token = self.generate_token()

        queue_user = DataTokenQueueUser.get_or_init(o_token.data_token)
        queue_user.register_result_tables(["1_xx"])

    @mock.patch("auth.api.MetaApi.list_result_table")
    @mock.patch("auth.api.DatabusApi.query_queue_auth")
    @mock.patch("auth.api.DatabusApi.add_queue_auth")
    def test_sync_queue_task(self, patch_add_queue_auth, patch_query_queue_auth, patch_list_result_table):
        patch_add_queue_auth.return_value = SUCCESS_RESPONSE
        patch_query_queue_auth.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "127.0.0.1:2181/kafka-test-3": {
                        "topic": {
                            "Topic:queue_105_rt_xxxx": ["User:demo_app_user Read from hosts: *"],
                            "Topic:queue_105_ja_idc_online": [
                                "User:demo_app_user Describe from hosts: *",
                                "User:demo_app_user Read from hosts: *",
                                "User:demo_app_user Read from hosts: *",
                                "User:demo_app_user1 Read from hosts: *",
                                "User:demo_app_user2 Read from hosts: *",
                            ],
                        }
                    }
                },
            }
        )
        patch_list_result_table.return_value = DataResponse(
            {
                "result": True,
                "data": [
                    {
                        "bk_biz_id": 591,
                        "count_freq_unit": "s",
                        "result_table_name_alias": "xxxx",
                        "project_name": "\\u5c0f\\u6cfd\\u6d4b\\u8bd5\\u9879\\u76ee",
                        "count_freq": 3600,
                        "updated_by": "admin",
                        "platform": "bkdata",
                        "created_at": "2019-01-11 20:25:41",
                        "updated_at": "2019-01-11 20:25:41",
                        "created_by": "admin",
                        "result_table_id": "105_rt_xxxx2222",
                        "result_table_type": None,
                        "result_table_name": "xxxx2222",
                        "project_id": 1,
                        "generate_type": "user",
                        "is_managed": 1,
                        "storages": {"queue": {}, "hdfs": {}, "kafka": {}},
                    },
                    {
                        "bk_biz_id": 591,
                        "count_freq_unit": "s",
                        "result_table_name_alias": "xxxx",
                        "project_name": "\\u5c0f\\u6cfd\\u6d4b\\u8bd5\\u9879\\u76ee",
                        "count_freq": 3600,
                        "updated_by": "admin",
                        "platform": "bkdata",
                        "created_at": "2019-01-11 20:25:41",
                        "updated_at": "2019-01-11 20:25:41",
                        "created_by": "admin",
                        "result_table_id": "105_rt_xxxx",
                        "result_table_type": None,
                        "result_table_name": "rt_xxxx",
                        "project_id": 1,
                        "generate_type": "user",
                        "is_managed": 1,
                        "storages": {"queue": {}, "hdfs": {}, "kafka": {}},
                    },
                ],
            }
        )

        # 初始化数据
        o_token = self.generate_token()

        DataTokenQueueUser.objects.create(
            queue_user="demo_app_user", queue_password="demo_app_user_password", data_token=o_token.data_token
        )

        AuthDataTokenPermission.objects.create(
            status="active",
            data_token=o_token,
            action_id="result_table.query_queue",
            object_class="result_table",
            scope_id_key="project_id",
            scope_id="1",
        )

        AuthDataTokenPermission.objects.create(
            status="active",
            data_token=o_token,
            action_id="result_table.query_queue",
            object_class="result_table",
            scope_id_key="result_table_id",
            scope_id="1_xx",
        )

        ret = extract_kafka_queue_auth()
        self.assertEqual(ret["demo_app_user"], ["105_rt_xxxx", "105_ja_idc_online"])

        ret = extract_token_queue_auth()
        self.assertIn("105_rt_xxxx2222", ret["demo_app_user"])
        self.assertIn("105_rt_xxxx", ret["demo_app_user"])

        sync_queue_by_project(gap=False)
        ret_params = {"user": "demo_app_user", "result_table_ids": ["105_rt_xxxx2222"]}

        patch_add_queue_auth.assert_called_with(ret_params)

    @mock.patch("auth.api.DatabusApi.add_queue_auth")
    @mock.patch("auth.api.DatabusApi.add_queue_user")
    def test_add_queue_auth(self, patch_add_queue_user, patch_add_queue_auth):
        patch_add_queue_auth.return_value = SUCCESS_RESPONSE
        patch_add_queue_user.return_value = SUCCESS_RESPONSE
        add_queue_auth([[55, "1_xxx"], [55, "2_xxx"]])
