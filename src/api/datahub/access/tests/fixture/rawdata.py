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
import pytest
from datahub.accesstests import db_helper


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
            maintainer="admin,admin2,admin3",
        )

    try:
        yield data_id
    finally:
        delete_data_id(data_id)


def delete_data_id(data_id):
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM access_raw_data WHERE id = %(data_id)s
        """,
            data_id=data_id,
        )
