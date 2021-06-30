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
from datahub.access.tests.utils import get
from datahub.databus.tests.fixture.access_raw_data import test_access_raw_data
from datahub.databus.tests.fixture.channel_fixture import add_channel
from datahub.databus.tests.mock_api import result_table


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_access_raw_data")
def test_search_topic():

    res = get("/v3/databus/admin/search_topic/?params=42&page=1&page_size=10")
    assert res["result"]
    assert len(res["data"]["topics"]) == 1
    assert res["data"]["page_total"] == 1

    res = get("/v3/databus/admin/search_topic/?params=42,1000&page=1&page_size=10")
    assert res["result"]
    assert len(res["data"]["topics"]) == 2
    assert res["data"]["page_total"] == 2


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_search_rt_topic():
    hp.enable()
    result_table.list_ok()

    res = get("/v3/databus/admin/search_topic/?params=42,123_table&page=1&page_size=10")
    assert res["result"]
    assert len(res["data"]["topics"]) == 2
    assert res["data"]["page_total"] == 2


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_search_cluster_topic():

    result_table.list_ok()

    res = get("/v3/databus/admin/search_topic/?channel_id=1002")
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_search_all_topic():

    result_table.list_ok()

    res = get("/v3/databus/admin/search_topic/")
    assert res["result"]
