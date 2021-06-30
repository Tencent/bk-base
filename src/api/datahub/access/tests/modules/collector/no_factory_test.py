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
from datahub.access.collectors.base_collector import BaseAccessTask
from datahub.access.tests.fixture import conftest
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.utils import post

param = {
    "bk_app_code": "bk_dataweb",
    "bk_username": "admin",
    "data_scenario": "unknow",
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
    "access_conf_info": {
        "collection_model": {"collection_type": "incr", "start_at": 1, "period": 0},
        "filters": {
            "delimiter": "|",
            "fields": [{"index": 1, "op": "=", "logic_op": "and", "value": "111"}],
        },
        "resource": {
            "scope": [
                {
                    "module_scope": [{"bk_obj_id": "set", "bk_inst_id": 123}],
                    "host_scope": [{"bk_cloud_id": 1, "ip": "x.x.x.x"}],
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
def test_error_factory_deploy_plan():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    conftest.mock_create_data_id()
    conftest.mock_get_data_id("unknow")
    conftest.mock_collector_hub_deploy_plan()

    url = "/v3/access/deploy_plan/"
    res = post(url, param)

    assert not res["result"]
    assert res["code"] == "1577209"


@pytest.mark.django_db
def test_error_factory_deploy():
    httpretty.enable()
    httpretty.reset()
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    url = "/v3/access/collector/unknow/deploy/"
    res = post(url, param)

    assert not res["result"]
    assert res["code"] == "1577209"


@pytest.mark.usefixtures("init_task_log")
@pytest.mark.django_db
def test_task_log():
    conftest.mock_user_perm("admin")
    conftest.mock_app_perm("bk_dataweb")
    task = BaseAccessTask(task_id=100)
    task.log("test debug", level="debug", task_log=False, time=None)
