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

import copy

import httpretty
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.db import *  # noqa
from datahub.access.tests.mock_api import cc
from datahub.access.tests.modules.collector.conftest import (  # noqa
    common_no_host_config_failure,
    common_param_error,
    common_success,
)
from datahub.access.tests.utils import post

param = {
    "bk_app_secret": "xxx",
    "bkdata_authentication_method": "user",
    "bk_app_code": "bk_dataweb",
    "bk_username": "admin",
    "db_host": "x.x.x.x",
    "db_port": "3306",
    "db_pass": "xxx",
    "db_user": "xxx",
    "db_name": "test_db",
    "bk_biz_id": "2",
    "op_type": "db",
}


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
def test_db_test_db(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_ip_log("DATABASE")
    cc.get_biz_location_ok()
    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )

    url = "/v3/access/collector/db/check/"
    db_param = copy.deepcopy(param)
    db_param["op_type"] = "db"

    res = post(url, db_param)
    common_success(res)


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
def test_db_test_tb(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_ip_log("DATABASE TABLE")
    cc.get_biz_location_ok()
    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )

    url = "/v3/access/collector/db/check/"
    res = post(url, param)
    common_success(res)


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
def test_db_test_failed(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_ip_log_err()
    cc.get_biz_location_ok()
    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )

    url = "/v3/access/collector/db/check/"
    res = post(url, param)

    assert not res["result"]
    assert res["code"] == "1577207"
    assert len(res["message"]) > 0


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
def test_db_test_param_error(mocker):
    httpretty.enable()
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_ip_log()
    conftest.mock_user_perm("admin")
    cc.get_biz_location_ok()
    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )

    url = "/v3/access/collector/db/check/"
    wrong_param = copy.deepcopy(param)
    wrong_param["op_type"] = "unknown"
    res = post(url, wrong_param)
    common_param_error(res, "op_type")


@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.django_db
def test_no_host_config(mocker):
    httpretty.enable()
    conftest.mock_fast_execute_script()
    conftest.mock_get_task_result()
    conftest.mock_user_perm("admin")
    cc.get_biz_location_ok()
    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )

    url = "/v3/access/collector/db/check/"
    res = post(url, param)

    assert not res["result"]
    assert res["code"] == "1577215"
