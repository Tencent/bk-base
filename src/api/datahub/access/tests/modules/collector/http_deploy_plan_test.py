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
from datahub.access.tests.fixture.db import (  # noqa
    init_access_manager_config,
    init_host_config_type,
)
from datahub.access.tests.mock_api import cc
from datahub.access.tests.modules.collector.conftest import (  # noqa
    common_no_host_config_failure,
    common_param_error,
)
from datahub.access.tests.utils import post
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa
from datahub.databus.tests.mock_api import config_server, meta
from datahub.databus.tests.mock_api.meta import get_inland_tag_ok
from datahub.storekit.tests.utils.conftest import mock_meta_content_language_configs

from datahub.databus import model_manager

get_param = {
    "bk_app_code": "bk_dataweb",
    "bk_username": "admin",
    "data_scenario": "http",
    "bk_biz_id": 591,
    "description": "xx",
    "access_raw_data": {
        "raw_data_name": "http_new_003",
        "maintainer": "xxxx",
        "raw_data_alias": "asdfsaf",
        "data_source": "svr",
        "data_encoding": "UTF-8",
        "sensitivity": "private",
        "description": "xx",
    },
    "access_conf_info": {
        "collection_model": {
            "collection_type": "pull",
            "period": 1,
            "time_format": "yyyy-MM-dd HH:mm:ss",
            "increment_field": "xx,aa",
        },
        "filters": {
            "delimiter": "|",
            "fields": [{"index": 1, "op": "=", "logic_op": "and", "value": "111"}],
        },
        "resource": {
            "scope": [
                {
                    "url": "http://www.xxxx.com",
                    "method": "get",
                }
            ]
        },
    },
}


@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("add_channel")
def test_get(mocker):
    """
    :return:
    """
    __mock_deploy_obj(mocker)

    res = post("/v3/access/deploy_plan/", get_param)

    want = {
        "code": "1500200",
        "message": "ok",
        "errors": None,
        "data": {"raw_data_id": 123},
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(want, sort_keys=True)


@pytest.mark.usefixtures("init_host_config_type")
@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("add_channel")
def test_missing_param(mocker):
    """
    :return:
    """
    __mock_deploy_obj(mocker)

    param = copy.deepcopy(get_param)
    del param["access_conf_info"]["resource"]["scope"][0]["method"]
    res = post("/v3/access/deploy_plan/", param)

    common_param_error(res, "method")

    param = copy.deepcopy(get_param)
    param["access_conf_info"]["collection_model"]["period"] = 0
    res = post("/v3/access/deploy_plan/", param)

    common_param_error(res, "period")

    param = copy.deepcopy(get_param)
    del param["access_conf_info"]["collection_model"]["time_format"]
    res = post("/v3/access/deploy_plan/", param)

    common_param_error(res, "time_format")


def __mock_deploy_obj(mocker):
    httpretty.enable()
    httpretty.reset()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_create_data_id()
    conftest.mock_create_data_auth()
    conftest.mock_get_data_auth()
    conftest.mock_create_alert()
    conftest.mock_get_data_id("http")
    conftest.mock_fast_execute_script()
    conftest.mock_db_get_task_ip_log()
    conftest.mock_get_mysql_type()
    conftest.mock_collector_hub_deploy_plan()
    conftest.mock_databus_task_puller()
    cc.get_biz_location_ok()
    cc.get_tag_target_ok()
    get_inland_tag_ok()
    mock_meta_content_language_configs()
    meta.post_sync_hook_ok()
    meta.get_dm_category_ok()
    config_server.post_add_route_ok()

    mocker.patch(
        "datahub.access.collectors.base_collector.BaseAccess.check_perm_by_scenario",
        return_value=True,
    )
    mocker.patch("datahub.access.tags.create_tags", return_value=True)
    mocker.patch("datahub.access.handlers.puller.DatabusPullerHandler.create", return_value=True)
    outer_channel = model_manager.get_channel_by_id(1002)
    assert outer_channel
    mocker.patch(
        "datahub.access.raw_data.rawdata.generate_raw_data_channel_id",
        return_value=(1002, outer_channel),
    )
    mocker.patch(
        "common.auth.check_perm",
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
