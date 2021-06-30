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
from datahub.access.tests.fixture import conftest
from datahub.access.tests.utils import delete, post

PARAMS = {
    "raw_data_name": "bk_test_0209",
    "raw_data_alias": "bk_test",
    "data_source": "business_server",
    "data_encoding": "UTF-8",
    "bk_biz_id": 372,
    "bk_app_code": "bk_dataweb",
    "sensitivity": "private",
    "bk_username": "admin",
    "description": "desc",
    "data_scenario": "log",
    "id": 10001,
    "maintainer": "admin",
}


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.skip
def test_raw_data_sycn():
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_dmonitor_alert_create()
    conftest.mock_sycn_auth()

    res = delete("/v3/access/rawdata/123/")
    assert res["result"]

    res = post("/v3/access/rawdata/", PARAMS)
    assert res["result"]
