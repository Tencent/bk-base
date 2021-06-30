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
from __future__ import absolute_import

import pytest
from datahub.access.tests import db_helper


@pytest.fixture(scope="session")
def django_db_setup():
    """Avoid creating/setting up the test database"""
    pass


@pytest.fixture
def test_data_id():

    data_id = 3000

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "access_raw_data",
            id=data_id,
            bk_biz_id=2,
            raw_data_name="fixture_data1",
            raw_data_alias="fixture_data1_alias",
            sensitivity="private",
            data_source="server",
            data_encoding="UTF-8",
            data_category="",
            data_scenario="log",
            bk_app_code="bk_data",
            storage_channel_id=0,
            created_by="admin",
            created_at="2019-01-01 00:00:00",
            updated_by="admin",
            updated_at="2019-01-01 00:00:00",
            description="description",
            maintainer="admin1,admin2,admin3",
        )

    try:
        yield data_id
    finally:
        delete_data_id(data_id)


@pytest.fixture
def test_encode():
    encode_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "encoding_config",
            id=encode_id,
            encoding_name="UTF8",
            encoding_alias="UTF8",
            created_at="2018-10-23T12:02:18",
            updated_at="2018-10-23T12:02:18",
            created_by="admin",
            updated_by="admin",
            active=1,
            description="",
        )

    try:
        yield encode_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM encoding_config where id = 1")


@pytest.fixture
def test_field_type():
    field_type = "double"
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "field_type_config",
            field_type=field_type,
            field_type_name="double",
            field_type_alias="浮点型",
            updated_at="2021-05-28 12:02:18",
            created_at="2021-05-28 12:02:18",
            created_by="admin",
            updated_by="admin",
            active=1,
            description="",
        )

    try:
        yield
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM field_type_config")


@pytest.fixture
def test_time_format():
    time_format_id = 111
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "time_format_config",
            id=time_format_id,
            time_format_name="yyyyMMdd",
            time_format_alias="yyyyMMdd",
            time_format_example="20191010",
            timestamp_len=8,
            format_unit="y,h,m",
            created_at="2018-10-23T12:02:18",
            updated_at="2018-10-23T12:02:18",
            created_by="admin",
            updated_by="admin",
            active=1,
            description="",
        )

    try:
        yield time_format_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM time_format_config where id = 111")


@pytest.fixture
def test_scenario():
    scenario_id = 111
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "access_scenario_config",
            id=scenario_id,
            data_scenario_name="log",
            data_scenario_alias="log",
            created_at="2018-10-23T12:02:18",
            updated_at="2018-10-23T12:02:18",
            created_by="admin",
            updated_by="admin",
            active=1,
            description="",
        )

    try:
        yield scenario_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_scenario_config where id = 111")


@pytest.fixture
def test_category():
    category_id = 11
    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "data_category_config",
            id=category_id,
            data_category_alias="",
            updated_by="admin",
            data_category_name="",
            created_at="2018-10-23T12:02:18",
            updated_at="2018-10-23T12:02:18",
            created_by="admin",
            active=1,
            description="",
        )

    try:
        yield category_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(
                cur,
                """
            DELETE FROM data_category_config WHERE id = %(category_id)s
            """,
                category_id=category_id,
            )


@pytest.fixture
def test_oper_log():
    oper_log_id = 11
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "access_operation_log",
            id=oper_log_id,
            raw_data_id=123,
            updated_by="admin",
            args='{"test":"111"}',
            status="success",
            created_at="2018-10-23T12:02:18",
            updated_at="2018-10-23T12:02:18",
            created_by="admin",
            description="",
        )

    try:
        yield oper_log_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(
                cur,
                """
            DELETE FROM access_operation_log WHERE id = 11""",
            )


def delete_data_id(data_id):
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM access_raw_data WHERE id = %(data_id)s
        """,
            data_id=data_id,
        )
