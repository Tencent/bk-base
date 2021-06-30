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
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.fixture.auth import *  # noqa
from datahub.access.tests.mock_api import cc
from datahub.access.tests.utils import delete
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa
from datahub.databus.tests.mock_api import config_server
from datahub.databus.tests.mock_api.meta import post_sync_hook_ok

param = {
    "bk_app_secret": "xxx",
    "bkdata_authentication_method": "user",
    "bk_app_code": "bk_dataweb",
    "raw_data_id": 123,
    "bk_username": "admin",
    "bk_biz_id": 591,
}


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access")
@pytest.mark.usefixtures("add_channel")
def test_delete_access(mocker):
    httpretty.enable()
    conftest.mock_user_perm("admin")
    conftest.mock_dmonitor_alert_delete(123)
    mock_update_project_role_users(123)
    conftest.mock_collector_hub_start("123")
    conftest.mock_collector_hub_stop("123")
    conftest.mock_collector_hub_summary("123", "failure")
    conftest.mock_databus_clean_list("123")
    conftest.mock_databus_task_start("123")
    conftest.mock_databus_task_stop("123")
    cc.get_biz_location_ok()
    config_server.post_delete_route_ok()
    post_sync_hook_ok()
    mocker.patch(
        "common.auth.check_perm",
        return_value=True,
    )
    mocker.patch(
        "datahub.access.utils.kafka_tool.delete_topic",
        return_value=True,
    )

    url = "/v3/access/deploy_plan/123/"

    res = delete(url, param)
    assert res["result"]
    assert res["code"] == "1500200"
