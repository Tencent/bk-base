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
from datalab.tests import db_helper
from datalab.tests.constants import BK_USERNAME


@pytest.fixture
def insert_query_task_info():
    with db_helper.open_cursor("default") as cur:
        db_helper.insert(
            cur,
            "datalab_query_task_info",
            query_id=1,
            query_name="query_1",
            project_id=1,
            project_type="personal",
            sql_text="select * from 591_x",
            query_task_id="BK11",
            chart_config="",
            lock_user=BK_USERNAME,
            lock_time="2020-05-01 00:00:00",
            active=1,
            description="test",
        )

    try:
        yield
    finally:
        delete_query_task_info()


@pytest.fixture
def insert_query_history():
    with db_helper.open_cursor("default") as cur:
        db_helper.insert(
            cur, "datalab_history_task_info", id=1, query_id=1, query_task_id="BK11", active=1, description="test"
        )

    try:
        yield
    finally:
        delete_query_history()


@pytest.fixture
def insert_bksql_function():
    with db_helper.open_cursor("default") as cur:
        db_helper.insert(
            cur,
            "datalab_bksql_function_config",
            id=1,
            sql_type="onesql",
            func_name="SUBSTRING",
            func_alias="SUBSTRING",
            func_group="string-functions",
            usage="SUBSTRING(STRING var1, INT start, INT len)",
            params="string,start,length",
            explain="xxx",
            support_framework="query",
            support_storage="HDFS",
            example='SELECT SUBSTRING("k1=v1;k2=v2", 2, 3)',
            example_return_value="1=v",
            active=1,
            description="test",
        )

    try:
        yield
    finally:
        delete_bksql_function()


def delete_query_task_info():
    with db_helper.open_cursor("default") as cur:
        db_helper.execute(cur, """ DELETE FROM datalab_query_task_info""")


def delete_query_history():
    with db_helper.open_cursor("default") as cur:
        db_helper.execute(cur, """ DELETE FROM datalab_history_task_info""")


def delete_bksql_function():
    with db_helper.open_cursor("default") as cur:
        db_helper.execute(cur, """ DELETE FROM datalab_bksql_function_config""")
