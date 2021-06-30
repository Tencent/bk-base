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
from datahub.access.handlers import meta  # noqa
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.fixture.db import init_access_manager_config  # noqa
from datahub.access.tests.mock_api import cc
from datahub.access.tests.utils import post
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa
from datahub.databus.tests.mock_api.meta import (
    get_dm_category_ok,
    get_inland_tag_ok,
    post_sync_hook_ok,
)
from datahub.storekit.tests.utils.conftest import mock_meta_content_language_configs

param = {
    "data_scenario": "log",
    "bk_username": "admin",
    "bk_biz_id": 591,
    "config_mode": "full",
    "scope": [{"deploy_plan_id": 11, "hosts": [{"bk_cloud_id": 1, "ip": "x.x.x.x"}]}],
}


@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
@pytest.mark.django_db
def test_stop_deploy_success(mocker):
    __deploy_test_setup(mocker)

    url = "/v3/access/collectorhub/123/stop/"

    res = post(url, param)
    assert res["result"]
    assert res["code"] == "1500200"


@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
@pytest.mark.django_db
def test_stop_deploy_stop(mocker):
    __deploy_test_setup(mocker)

    url = "/v3/access/collectorhub/123/stop/"

    res = post(url, param)
    assert res["result"]


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
def test_stop_deploy_start(mocker):
    __deploy_test_setup(mocker)

    url = "/v3/access/collectorhub/123/stop/"

    res = post(url, param)
    assert res["result"]


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
def test_stop_deploy_running(mocker):
    __deploy_test_setup(mocker)

    url = "/v3/access/collectorhub/123/stop/"

    res = post(url, param)
    assert res["result"]


@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.django_db
def test_start_deploy_start(mocker):
    __deploy_test_setup(mocker)

    url = "/v3/access/collectorhub/123/start/"

    res = post(url, param)
    print(res)
    assert res["result"]


def __deploy_test_setup(mocker):
    httpretty.enable()
    httpretty.reset()
    cc.get_biz_location_ok()
    cc.get_tag_target_ok()
    get_inland_tag_ok()
    mock_meta_content_language_configs()
    post_sync_hook_ok()
    get_dm_category_ok()
    conftest.mock_user_perm("admin")
    conftest.mock_collector_hub_start("123")
    conftest.mock_collector_hub_stop("123")
    conftest.mock_collector_hub_summary("123", "failure")
    conftest.mock_databus_clean_list("123")
    conftest.mock_databus_task_start("123")
    conftest.mock_databus_task_stop("123")
    conftest.mock_get_data_auth()
    mocker.patch(
        "common.auth.check_perm",
        return_value=True,
    )
    mocker.patch(
        "datahub.access.utils.forms.check_ip_rule",
        return_value=True,
    )
    mocker.patch(
        "datahub.access.handlers.v3.paas.PaasHandler.get_app_name_by_code",
        return_value=None,
    )
    mocker.patch(
        "datahub.databus.task.puller_task.process_puller_task",
        return_value=None,
    )
