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
from datalab.tests.fixture.favorite import insert_favorite_result_table  # noqa
from tests.utils import UnittestClient

client = UnittestClient()


@pytest.mark.django_db
def test_create_favorite_result_table():
    """
    测试新增置顶表
    """
    param = {"project_id": 1, "project_type": "personal", "result_table_id": "1_test"}
    res = client.post("/v3/datalab/favorites/", param)
    assert res.is_success()
    assert res.data["result_table_id"] == "1_test"


@pytest.mark.django_db()
@pytest.mark.usefixtures("insert_favorite_result_table")
def test_get_favorite_result_table():
    """
    测试获取项目的置顶表
    """
    res = client.get("/v3/datalab/favorites/1/")
    assert res.is_success()
    assert len(res.data) >= 1
    assert res.data[0]["result_table_id"] == "591_test"


@pytest.mark.django_db()
@pytest.mark.usefixtures("insert_favorite_result_table")
def test_cancel_favorite_result_table():
    """
    测试项目取消置顶表
    """
    param = {"project_id": 1, "project_type": "personal", "result_table_id": "591_test"}
    res = client.delete("/v3/datalab/favorites/cancel/", param)
    assert res.is_success()
    assert res.data
