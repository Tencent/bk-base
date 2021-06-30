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

from auth.core.data_managers import DataManagerController
from auth.core.permission import RoleController
from auth.models import AuthUserRole
from common.db import connections
from tests import BaseTestCase

logger = logging.getLogger(__name__)


class TestPermission(BaseTestCase):
    def setup(self):
        with connections["basic"].session() as session:
            users = [
                AuthUserRole(
                    role_id="raw_data.manager",
                    scope_id="1",
                    user_id="user01",
                    created_by="admin",
                    updated_by="admin",
                ),  # noqa
                AuthUserRole(
                    role_id="raw_data.manager",
                    scope_id="1",
                    user_id="user02",
                    created_by="admin",
                    updated_by="admin",
                ),  # noqa
                AuthUserRole(
                    role_id="raw_data.manager",
                    scope_id="2",
                    user_id="user03",
                    created_by="admin",
                    updated_by="admin",
                ),  # noqa
                AuthUserRole(
                    role_id="raw_data.manager",
                    scope_id="3",
                    user_id="user03",
                    created_by="admin",
                    updated_by="admin",
                ),  # noqa
            ]
            session.add_all(users)
            session.commit()

    def teardown(self):
        with connections["basic"].session() as session:
            session.execute("DELETE FROM auth_user_role")
            session.commit()

    def test_roles(self):
        role_controller = RoleController()
        with connections["basic"].session() as session:
            data = role_controller.get_role_members_with_all_scope(
                session, "raw_data.manager"
            )

            assert data["1"] == ["user01", "user02"]

            data = role_controller.get_role_members(session, "raw_data.manager", 1)
            assert data == ["user01", "user02"]

            role_controller.update_role_members(
                session, "raw_data.manager", 1, ["user03"]
            )
            data = role_controller.get_role_members(session, "raw_data.manager", 1)
            assert data == ["user03"]

    def test_datamanagers(self):
        datamanager_controller = DataManagerController()
        with connections["basic"].session() as session:
            raw_data_managers = datamanager_controller.get_raw_data_managers(session)
            assert raw_data_managers["raw_data:1"] == ["user01", "user02"]

            identifiers = ["raw_data:2", "raw_data:3"]
            data = datamanager_controller.get_managers_interset(
                session, identifiers, cache_managers=raw_data_managers
            )
            assert data == ["user03"]

            datamanager_controller.update_managers(
                session, "result_table:1_xx", ["user09", "user10"]
            )
            data = datamanager_controller.get_managers(session, "result_table:1_xx")

            assert data == ["user09", "user10"]
