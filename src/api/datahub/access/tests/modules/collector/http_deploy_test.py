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
from datahub.access.tests import utils
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.modules.collector.collector_demo_data import param
from datahub.access.tests.modules.collector.conftest import common_deploy_success
from datahub.access.tests.utils import post


@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.usefixtures("init_access_get_http_resource")
@pytest.mark.usefixtures("init_http_access_raw_data")
@pytest.mark.django_db
@pytest.mark.skip
def test_deploy():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_create_data_id()
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_user_perm("admin")
    conftest.mock_get_data_id("http")
    conftest.mock_collector_hub_deploy_plan()
    conftest.mock_gse_push_file()
    conftest.mock_get_task_ip_log()
    conftest.mock_get_file_result()
    conftest.mock_gse_proc_operate()
    conftest.mock_get_proc_result()
    conftest.mock_fast_execute_script()

    url = "/v3/access/collector/http/deploy/"
    res = post(url, param)

    assert res["result"]
    assert isinstance(res["data"]["task_id"], int)

    task_id = res["data"]["task_id"]

    url = "/v3/access/collector/check/?task_id=%d" % task_id
    res = utils.get(url)

    common_deploy_success(res)


@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.usefixtures("init_access_get_http_resource")
@pytest.mark.usefixtures("init_http_access_raw_data")
@pytest.mark.django_db
@pytest.mark.skip
def test_deploy_post():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_user_perm("admin")
    conftest.mock_create_data_id()
    conftest.mock_get_data_id("http")
    conftest.mock_collector_hub_deploy_plan()
    conftest.mock_gse_push_file()
    conftest.mock_get_task_ip_log()
    conftest.mock_get_file_result()
    conftest.mock_fast_execute_script()

    url = "/v3/access/collector/http/deploy/"
    res = post(url, param)

    assert res["result"]
    assert isinstance(res["data"]["task_id"], int)

    task_id = res["data"]["task_id"]

    url = "/v3/access/collector/check/?task_id=%d" % task_id
    res = utils.get(url)

    common_deploy_success(res)
