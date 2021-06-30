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

import httpretty as hp
import pytest
from datalab.tests.constants import BK_USERNAME
from datalab.tests.mock_api.meta import mock_get_result_tables
from datalab.tests.mock_api.queryengine import mock_get_es_chart, mock_query_es
from tests.utils import UnittestClient

client = UnittestClient()


@pytest.mark.django_db
def test_es_query():
    """
    测试es查询
    """
    hp.enable()
    hp.reset()
    mock_get_result_tables()
    mock_query_es()

    param = {
        "page": 1,
        "page_size": 30,
        "start_time": "2021-05-01 00:00:00",
        "end_time": "2021-05-02 00:00:00",
        "keyword": "",
        "totalPage": "",
        "bk_username": BK_USERNAME,
    }
    res = client.post("/v3/datalab/es_query/591_xx/query/", data=param)
    assert res.is_success()
    assert res.data["list"][0]["ip"] == "xx"
    assert "ip" in res.data["select_fields_order"]


@pytest.mark.django_db
def test_get_es_chart():
    """
    测试获取es查询历史，分页
    """
    hp.enable()
    hp.reset()
    mock_get_es_chart()

    param = {
        "interval": 1,
        "start_time": "2021-05-01 00:00:00",
        "end_time": "2021-05-02 00:00:00",
        "result_table_id": "591_xx",
    }
    res = client.post("/v3/datalab/es_query/591_xx/get_es_chart/", data=param)
    assert res.is_success()
    assert "time" in res.data
    assert "cnt" in res.data


@pytest.mark.django_db
def test_list_query_history_with_page():
    """
    测试获取es查询历史，分页
    """
    param = {"page": 10, "page_size": 20}
    res = client.get("/v3/datalab/es_query/591_xx/list_query_history/", data=param)
    assert res.is_success()
    assert res.data["count"] == 0


@pytest.mark.django_db
def test_list_query_history_no_page():
    """
    测试获取es查询历史，分页
    """
    res = client.get("/v3/datalab/es_query/591_xx/list_query_history/")
    assert res.is_success()
    assert "count" in res.data
