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

import json

import httpretty
import pytest
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.fixture.db import (  # noqa
    init_access_manager_config,
    init_access_time_db_resource,
    init_host_config_type,
)
from datahub.access.tests.mock_api import cc
from datahub.access.tests.utils import post, put
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa
from datahub.databus.tests.mock_api import config_server
from datahub.databus.tests.mock_api.meta import (
    get_dm_category_ok,
    get_inland_tag_ok,
    post_sync_hook_ok,
)
from datahub.storekit.tests.utils.conftest import mock_meta_content_language_configs

from datahub.databus import model_manager

param = {
    "bk_app_code": "bk_dataweb",
    "bk_username": "admin",
    "data_scenario": "log",
    "bk_biz_id": 591,
    "description": "xx",
    "access_raw_data": {
        "raw_data_name": "log_new_00011",
        "maintainer": "xxxx",
        "raw_data_alias": "asdfsaf",
        "data_source": "svr",
        "data_encoding": "UTF-8",
        "sensitivity": "private",
        "description": "xx",
    },
    "filters": {
        "delimiter": "|",
        "fields": [{"index": 1, "op": "=", "logic_op": "and", "value": "111"}],
    },
    "access_conf_info": {
        "collection_model": {
            "collection_type": "incr",
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
                    "module_scope": [
                        {
                            "bk_inst_name": "set_name",
                            "bk_obj_id": "set",
                            "bk_inst_id": 123,
                        }
                    ],
                    "host_scope": [{"bk_cloud_id": 1, "ip": "*.*.*.*"}],
                    "scope_config": {
                        "paths": [
                            {
                                "path": ["/tmp/*.log", "/tmp/*.l", "/tmp/*.aaaz"],
                                "system": "linux",
                            }
                        ]
                    },
                }
            ]
        },
    },
}


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("add_channel")
def test_post(mocker):
    deploy_test_setup(mocker)
    url = "/v3/access/deploy_plan/"
    res = post(url, param)

    want = {
        "code": "1500200",
        "message": "ok",
        "errors": None,
        "data": {"raw_data_id": 123},
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(want, sort_keys=True)


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
@pytest.mark.ship
def test_update(mocker):
    deploy_test_setup(mocker)
    conftest.mock_hub_update_deploy_plan()

    url = "/v3/access/deploy_plan/123/"
    param["access_conf_info"]["resource"]["scope"][0]["scope_config"]["paths"][0]["path"][0] = "/tmp/yyy/*.log"
    res = put(url, param)

    want = {
        "code": "1500200",
        "data": {"raw_data_id": "123"},
        "errors": None,
        "message": "ok",
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(want, sort_keys=True)


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
@pytest.mark.ship
def test_deploy_plan_collector_error(mocker):
    deploy_test_setup(mocker)
    conftest.mock_hub_deploy_plan_error()

    url = "/v3/access/deploy_plan/"
    res = post(url, param)

    assert not res["result"]


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
def test_deploy_plan_format_error(mocker):
    deploy_test_setup(mocker)
    conftest.mock_hub_deploy_plan_error()

    url = "/v3/access/deploy_plan/"
    res = post(url, param)

    assert not res["result"]


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
def test_deploy_condition_format_error(mocker):
    deploy_test_setup(mocker)

    param["access_conf_info"]["filters"] = []
    url = "/v3/access/deploy_plan/"
    res = post(url, param)
    assert res["code"] == "1500001"
    assert not res["result"]


def deploy_test_setup(mocker):
    httpretty.enable()
    httpretty.reset()
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
    conftest.mock_create_data_auth()
    conftest.mock_dmonitor_alert_create()
    config_server.post_add_route_ok()
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_create_data_id()
    cc.get_biz_location_ok()
    conftest.mock_get_data_id("log")
    conftest.mock_node_man_create()
    conftest.mock_node_man_run()
    conftest.mock_node_man_switch()
    conftest.mock_node_man_delete()
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
    mocker.patch(
        "datahub.access.tags.check_use_v2_unifytlogc",
        return_value=True,
    )
    mocker.patch(
        "common.auth.check_perm",
        return_value=True,
    )
    mocker.patch("datahub.access.tags.create_tags", return_value=True)
    outer_channel = model_manager.get_channel_by_id(1002)
    assert outer_channel
    mocker.patch(
        "datahub.access.raw_data.rawdata.generate_raw_data_channel_id",
        return_value=(1002, outer_channel),
    )
