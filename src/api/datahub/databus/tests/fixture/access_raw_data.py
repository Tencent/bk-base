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
from datahub.access.tests import db_helper


@pytest.fixture
def test_access_raw_data():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "access_raw_data",
            id=1000,
            bk_biz_id=1,
            raw_data_name="",
            raw_data_alias="",
            sensitivity="",
            data_source="",
            data_encoding="gbk",
            data_category="",
            data_scenario="tdw",
            bk_app_code="",
            storage_channel_id=1000,
            maintainer="",
            description="",
        )

        db_helper.insert(
            cur,
            "access_raw_data",
            id=42,
            bk_biz_id=1,
            raw_data_name="raw_data_test",
            raw_data_alias="",
            sensitivity="",
            data_source="",
            data_encoding="gbk",
            data_category="",
            data_scenario="tdw",
            bk_app_code="",
            storage_channel_id=1003,
            maintainer="",
            description="",
        )

    try:
        yield
    finally:
        delete_access_raw_data()


def delete_access_raw_data():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(cur, """ DELETE FROM access_raw_data""")
