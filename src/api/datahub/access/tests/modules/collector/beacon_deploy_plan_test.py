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
import copy
import json

import httpretty
import pytest
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.modules.collector.conftest import common_param_error
from datahub.access.tests.utils import get, post

param = {
    "bk_app_secret": "xxx",
    "bkdata_authentication_method": "user",
    "bk_app_code": "bk_dataweb",
    "bk_username": "admin",
    "data_scenario": "beacon",
    "bk_biz_id": 591,
    "description": "这里填写描述",
    "access_raw_data": {
        "raw_data_name": "xxx",
        "maintainer": "xxxx",
        "raw_data_alias": "中文名",
        "data_category": "online",
        "data_source": "server",
        "data_encoding": "UTF-8",
        "sensitivity": "private",
        "description": "这里填写描述",
    },
    "access_conf_info": {
        "resource": {
            "scope": [
                {
                    "appkey": "appkey1",
                    "event_code": "event_code1",
                }
            ]
        }
    },
}


@pytest.mark.django_db
@pytest.mark.skip
def test_ok():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_user_perm("admin")
    conftest.mock_create_data_id()
    conftest.mock_get_data_id("beacon")

    res = post("/v3/access/deploy_plan/", param)
    want = {
        "code": "1500200",
        "data": {"raw_data_id": 123},
        "errors": None,
        "message": "ok",
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(want, sort_keys=True)


@pytest.mark.django_db
@pytest.mark.skip
def test_beacon_missing_appkey_param():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_create_data_id()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_get_data_id("beacon")

    wrong_param = copy.deepcopy(param)
    del wrong_param["access_conf_info"]["resource"]["scope"][0]["appkey"]
    res = post("/v3/access/deploy_plan/", wrong_param)
    common_param_error(res, "appkey")


@pytest.mark.django_db
@pytest.mark.skip
def test_beacon_appkey_list():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_user_perm("admin")
    conftest.mock_app_info()
    res = get(
        "/v3/access/collector/app_key/?bk_biz_id=591&bk_username=admin",
    )

    assert res["result"]
