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

from auth.core.permission import RoleController
from auth.event import (
    AddManagersEvent,
    CreateDataProcessingRelationEvent,
    DeleteDataProcessingRelationEvent,
    DeleteManagersEvent,
    UpdateManagersEvent,
)
from auth.models import AuthUserRole
from auth.sync_data_managers import SyncTask, TransmitMetaRecordsTask
from auth.sync_data_managers import init as init_data_managers
from common.db import connections
from tests import BaseTestCase

logger = logging.getLogger(__name__)


class TestSyncDataManagers(BaseTestCase):
    def setup(self):
        """
        raw_data:1 -> result_table:1_xxxx -> result_table:2_xxxx -> result_table:4_xxxx
                                ^                         |
                                |                         |
        raw_data:2 -> result_table:3_xxxx                 |-------> result_table:5_xxxx

        """
        self.meta_lineage_response = {
            "extensions": {
                "server_latency": {
                    "parsing_ns": 10831,
                    "processing_ns": 180631016,
                    "encoding_ns": 44242738,
                },
                "txn": {"start_ts": 649530},
            },
            "data": {
                "target": [
                    {
                        "id": 1,
                        "data_directing": "input",
                        "data_set_type": "raw_data",
                        "data_set_id": "1",
                        "processing_id": "1_xxxx",
                    },  # noqa
                    {
                        "id": 2,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "1_xxxx",
                        "processing_id": "1_xxxx",
                    },  # noqa
                    {
                        "id": 3,
                        "data_directing": "input",
                        "data_set_type": "result_table",
                        "data_set_id": "1_xxxx",
                        "processing_id": "2_xxxx",
                    },  # noqa
                    {
                        "id": 4,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "2_xxxx",
                        "processing_id": "2_xxxx",
                    },  # noqa
                    {
                        "id": 5,
                        "data_directing": "input",
                        "data_set_type": "result_table",
                        "data_set_id": "2_xxxx",
                        "processing_id": "4_xxxx",
                    },  # noqa
                    {
                        "id": 6,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "4_xxxx",
                        "processing_id": "4_xxxx",
                    },  # noqa
                    {
                        "id": 7,
                        "data_directing": "input",
                        "data_set_type": "raw_data",
                        "data_set_id": "2",
                        "processing_id": "3_xxxx",
                    },  # noqa
                    {
                        "id": 8,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "3_xxxx",
                        "processing_id": "3_xxxx",
                    },  # noqa
                    {
                        "id": 9,
                        "data_directing": "input",
                        "data_set_type": "result_table",
                        "data_set_id": "3_xxxx",
                        "processing_id": "1_xxxx",
                    },  # noqa
                    {
                        "id": 10,
                        "data_directing": "output",
                        "data_set_type": "result_table",
                        "data_set_id": "5_xxxx",
                        "processing_id": "2_xxxx",
                    },  # noqa
                ]
            },
        }

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
                    user_id="user01",
                    created_by="admin",
                    updated_by="admin",
                ),  # noqa
                AuthUserRole(
                    role_id="raw_data.manager",
                    scope_id="2",
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
            ]
            session.add_all(users)
            session.commit()

    def teardown(self):
        with connections["basic"].session() as session:
            session.execute("DELETE FROM auth_user_role")
            session.commit()

    @mock.patch("common.meta.metadata_client.query")
    def test_init(self, patch_query):
        patch_query.return_value = self.meta_lineage_response
        init_data_managers()

        with connections["basic"].session() as session:
            data = RoleController().get_role_members_with_all_scope(
                session, "result_table.manager"
            )
            assert sorted(data["1_xxxx"]) == ["user01", "user02"]
            assert sorted(data["2_xxxx"]) == ["user01", "user02"]
            assert sorted(data["3_xxxx"]) == ["user01", "user02", "user03"]
            assert sorted(data["4_xxxx"]) == ["user01", "user02"]
            assert sorted(data["5_xxxx"]) == ["user01", "user02"]

    @mock.patch("common.meta.metadata_client.query")
    def test_hanlder_with_create_dp(self, patch_query):
        patch_query.return_value = self.meta_lineage_response
        task = SyncTask()
        _event = CreateDataProcessingRelationEvent(
            id=200,
            data_directing="input",
            data_set_type="result_table",
            data_set_id="5_xxxx",
            processing_id="6_xxxx",
            change_time="2020-10-10 10:00:00",
        )
        task.handler(_event)
        _event = CreateDataProcessingRelationEvent(
            id=200,
            data_directing="output",
            data_set_type="result_table",
            data_set_id="6_xxxx",
            processing_id="6_xxxx",
            change_time="2020-10-10 10:00:00",
        )
        task.handler(_event)

        with connections["basic"].session() as session:
            data = RoleController().get_role_members(
                session, "result_table.manager", "6_xxxx"
            )
            assert sorted(data) == ["user01", "user02"]

    @mock.patch("common.meta.metadata_client.query")
    def test_hanlder_with_delete_dp(self, patch_query):
        patch_query.return_value = self.meta_lineage_response

        # 先初始化结果数据的管理员，便于检测删除的效果
        init_data_managers()

        task = SyncTask()

        _event = DeleteDataProcessingRelationEvent(
            id=3, change_time="2020-10-10 10:00:00"
        )
        task.handler(_event)

        _event = DeleteDataProcessingRelationEvent(
            id=4, change_time="2020-10-10 10:00:00"
        )
        task.handler(_event)

    @mock.patch("common.meta.metadata_client.query")
    def test_rawdata_manager(self, patch_query):
        patch_query.return_value = self.meta_lineage_response

        task = SyncTask()
        _event = UpdateManagersEvent(
            data_set_type="raw_data",
            data_set_id="1",
            managers=["user01", "user03"],
            change_time="2020-10-10 10:00:00",
        )
        task.handler(_event)

        _event = AddManagersEvent(
            data_set_type="raw_data",
            data_set_id="1",
            managers=["user02"],
            change_time="2020-10-10 10:00:00",
        )
        task.handler(_event)

        _event = DeleteManagersEvent(
            data_set_type="raw_data",
            data_set_id="1",
            managers=["user01", "user0222"],
            change_time="2020-10-10 10:00:00",
        )
        task.handler(_event)


class TestTransmitMetaRecords(BaseTestCase):
    @mock.patch("auth.event.EventController.push_event")
    def test_hanlder(self, patch_push_event):
        task = TransmitMetaRecordsTask()

        with open("./tests/auth/meta_operation_records.demo") as f:
            for index, content in enumerate(f):
                task.handler(content)

                if index == 0:
                    assert patch_push_event.call_count == 0
                elif index == 1:
                    assert patch_push_event.call_count == 0
                elif index == 2:
                    assert patch_push_event.call_count == 2
                elif index == 3:
                    assert patch_push_event.call_count == 4
                elif index == 4:
                    assert patch_push_event.call_count == 4
                elif index == 5:
                    assert patch_push_event.call_count == 5
