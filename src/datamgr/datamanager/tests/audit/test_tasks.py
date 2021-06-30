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

import datetime
import random
import unittest
import uuid

import mock

from audit.audit_role import RoleManager
from audit.check_token import (
    _build_mail_objects,
    get_action_name,
    get_data_token_list,
    get_permission_info,
    get_send_email_list,
)
from auth.models import AuthDataToken, AuthDataTokenPermission, AuthDataTokenStatus
from common.db import connections
from models.meta import AccessRawData, ProjectInfo
from tests import BaseTestCase


class TestTask(BaseTestCase):
    @mock.patch("common.meta.metadata_client.list")
    def test_audit_manage_role(self, patch_list):
        def side_effect(md):
            if md == ProjectInfo:
                return [
                    ProjectInfo.part_init(
                        project_id=1,
                        project_name="项目1",
                        description="项目1描述",
                        bk_app_code="data",
                    ),
                    ProjectInfo.part_init(
                        project_id=2,
                        project_name="项目2",
                        description="项目2描述",
                        bk_app_code="data",
                    ),
                ]

            if md == AccessRawData:
                return [
                    AccessRawData.part_init(
                        id=1,
                        raw_data_name="原始数据1",
                        description="原始数据1描述",
                        bk_app_code="data",
                        bk_biz_id=591,
                    ),
                    AccessRawData.part_init(
                        id=2,
                        raw_data_name="原始数据2",
                        description="原始数据2描述",
                        bk_app_code="data",
                        bk_biz_id=591,
                    ),
                    AccessRawData.part_init(
                        id=3,
                        raw_data_name="原始数据3",
                        description="原始数据3描述",
                        bk_app_code="data",
                        bk_biz_id=591,
                    ),
                    AccessRawData.part_init(
                        id=4,
                        raw_data_name="原始数据4",
                        description="原始数据4描述",
                        bk_app_code="data",
                        bk_biz_id=591,
                    ),
                ]

            return []

        patch_list.side_effect = side_effect
        RoleManager().audit_manage_role()


class CheckTokenTest(unittest.TestCase):
    session = connections["basic"].session()

    def test_1_generated_data(self):
        """
        生成20条测试数据，其中日期。状态随机
        :return:
        """
        for i in range(0, 20):
            auth_data_token = AuthDataToken(
                data_token=uuid.uuid4().hex,
                data_token_bk_app_code="test",
                status=random.sample(["enabled", "disabled", "expired", "removed"], 1),
                expired_at=datetime.datetime.now()
                + datetime.timedelta(days=random.randint(-14, 14)),
                created_by="test",
                updated_by="test",
                updated_at=datetime.datetime.now(),
                description="this is a test data",
            )
            self.session.add(auth_data_token)
            self.session.commit()

        data_token = get_data_token_list(
            self.session,
            [AuthDataTokenStatus.ENABLED.value, AuthDataTokenStatus.EXPIRED.value],
        )
        auth_data_token_permission = AuthDataTokenPermission(
            status="active",
            scope_id="151 script",
            action_id="biz.job_access",
            scope_id_key="bk_bkz_id",
            object_class="project",
            created_by="test",
            updated_by="test",
            updated_at=datetime.datetime.now(),
            description="this is a test data",
            data_token_id=data_token[0].id,
        )

        self.session.add(auth_data_token_permission)
        self.session.commit()

    def test_2_get_data_token_enabled(self):
        self.assertIsInstance(
            get_data_token_list(session=self.session, status="ALL"), list
        )
        self.assertIsInstance(
            get_data_token_list(
                session=self.session,
                status=[
                    AuthDataTokenStatus.ENABLED.value,
                    AuthDataTokenStatus.EXPIRED.value,
                ],
            ),
            list,
        )

    def test_3_build_mail_objects(self):
        data_token = AuthDataToken(
            id=1,
            data_token=uuid.uuid4().hex,
            data_token_bk_app_code="gsm",
            status="expired",
            expired_at=datetime.datetime.now(),
            created_by="test",
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            updated_by="test",
            description="this is a test",
        )

        mail_object = {
            "data_token": data_token,
            "permission_info": {
                "数据查询": ["591_jere_clean4 "],
                "数据订阅": ["591_login_60s_indicator_1_4232_StormSqlAggregate_2"],
            },
        }

        test_dict = {
            "id": mail_object["data_token"].id,
            "data_token": mail_object["data_token"].data_token[0:3]
            + "*" * 5
            + mail_object["data_token"].data_token[-3:],
            "app_code": mail_object["data_token"].data_token_bk_app_code,
            "expired_id": mail_object["data_token"].expired_at.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "description": mail_object["data_token"].description,
            "permission_info": mail_object["permission_info"],
        }
        self.assertDictEqual(_build_mail_objects(mail_object), test_dict)

    def test_4_get_send_email_list(self):
        data_token_list = []
        status_list = ["enabled", "expired", "disabled", "removed"]
        for status in status_list:
            data_token = AuthDataToken(
                id=1,
                data_token=uuid.uuid4().hex,
                data_token_bk_app_code="gsm",
                status=status,
                expired_at=datetime.datetime.now(),
                created_by="test",
                created_at=datetime.datetime.now(),
                updated_at=datetime.datetime.now(),
                updated_by="test",
                description="this is a test",
            )

            data_token_list.append(data_token)

        send_email_list = get_send_email_list(data_token_list)
        self.assertIs(send_email_list[0], data_token_list[0])

    def test_5_get_permission_info(self):
        permission_info = {"运维场景接入": ["151 script"]}
        data_token = get_data_token_list(
            self.session,
            [AuthDataTokenStatus.ENABLED.value, AuthDataTokenStatus.EXPIRED.value],
        )

        self.assertDictEqual(
            get_permission_info(self.session, data_token[0].id), permission_info
        )

    @mock.patch("audit.check_token.send_email", return_value="发送成功")
    def test_6_send_email(self, patch_send_email):
        self.assertEqual(patch_send_email(), "发送成功")

    @mock.patch("audit.check_token.update_token_status_batch", return_value="批量更新成功")
    def test_7_update_token_status(self, patch_update_status):
        self.assertEqual(patch_update_status(), "批量更新成功")

    def test_8_get_action_name(self):
        self.assertEqual(
            get_action_name(self.session, "result_table.query_data"), "数据查询"
        )

    def test_999_clear_data(self):
        self.session.query(AuthDataTokenPermission).filter(
            AuthDataTokenPermission.status.in_(("active", "applying"))
        ).delete(synchronize_session=False)

        self.session.query(AuthDataToken).filter(
            AuthDataToken.status.in_(("enabled", "disabled", "expired", "removed"))
        ).delete(synchronize_session=False)
        self.session.commit()
