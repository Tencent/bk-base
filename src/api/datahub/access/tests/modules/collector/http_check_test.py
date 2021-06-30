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

import httpretty
import pytest
from conf.dataapi_settings import ACCESS_API_HOST, ACCESS_API_PORT
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.fixture.db import (  # noqa
    init_access_manager_config,
    init_host_config_type,
)
from datahub.access.tests.mock_api.cc import get_biz_location_ok
from datahub.access.tests.modules.collector.conftest import (
    common_no_host_config_failure,
    common_success,
)
from datahub.access.tests.utils import post

param = {
    "url": "http://%s:%d/v3/access/rawdata/?page=1&page_size=1" % (ACCESS_API_HOST, ACCESS_API_PORT),
    "method": "get",
    "bk_username": "admin",
    "bk_biz_id": "591",
}


@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
def test_http_test_get(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_ip_log('{"data": []}')
    get_biz_location_ok()
    mocker.patch(
        "common.auth.check_perm",
        return_value=True,
    )

    url = "/v3/access/collector/http/check/"

    res = post(url, param)
    common_success(res)


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.django_db
def test_no_host_config(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_result()
    get_biz_location_ok()
    mocker.patch(
        "common.auth.check_perm",
        return_value=True,
    )

    url = "/v3/access/collector/http/check/"
    res = post(url, param)

    common_no_host_config_failure(res)
