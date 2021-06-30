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

import pytest
from datahub.access.tests.fixture import conftest  # noqa
from datahub.access.tests.fixture.access import *  # noqa
from datahub.access.tests.fixture.db import (  # noqa
    init_access_manager_config,
    init_access_time_db_resource,
)
from datahub.access.tests.modules.collector.log_deploy_plan_test import (
    deploy_test_setup,
)
from datahub.access.tests.utils import get
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
def test_get_deploy_plan(mocker):
    deploy_test_setup(mocker)

    url = "/v3/access/deploy_plan/123/?bk_username=admin"
    res = get(url)

    want = {
        "access_conf_info": {
            "collection_model": {
                "before_time": 100,
                "collection_type": "time",
                "increment_field": "create_at",
                "period": 0,
                "start_at": 0,
                "time_format": "yyyy-MM-dd HH:mm:ss",
            },
            "filters": {},
            "resource": {
                "scope": [
                    {
                        "db_host": "x.x.x.x",
                        "db_name": "bkdata_basic",
                        "db_pass": "XXX",
                        "db_port": 10000,
                        "db_type_id": 1,
                        "db_user": "user",
                        "deploy_plan_id": 11,
                        "table_name": "access_db_info",
                    },
                    {
                        "db_host": "x.x.x.x",
                        "db_name": "bkdata_basic",
                        "db_pass": "XXX",
                        "db_port": 10000,
                        "db_type_id": 1,
                        "db_user": "user",
                        "deploy_plan_id": 12,
                        "table_name": "access_db_info",
                    },
                    {
                        "deploy_plan_id": 132,
                        "host_scope": [{"bk_cloud_id": 1, "ip": "*.*.*.*"}],
                        "module_scope": [
                            {
                                "bk_inst_id": 123,
                                "bk_inst_name": "set_name",
                                "bk_obj_id": "set",
                            }
                        ],
                        "scope_config": {
                            "paths": [
                                {
                                    "path": [
                                        "/tmp/yyy/*.log",
                                        "/tmp/*.l",
                                        "/tmp/*.aaaz",
                                    ],
                                    "system": "linux",
                                }
                            ]
                        },
                    },
                ]
            },
        },
        "access_raw_data": {
            "active": 1,
            "bk_app_code": "",
            "bk_biz_id": 591,
            "bk_biz_name": "Unkown Business",
            "created_at": "2021-06-03 12:07:28",
            "created_by": None,
            "data_category": "",
            "data_encoding": "gbk",
            "data_scenario": "tdw",
            "data_source": "",
            "data_source_tags": [],
            "description": "",
            "id": 123,
            "maintainer": "admin",
            "permission": "all",
            "raw_data_alias": "",
            "raw_data_name": "raw_data_test",
            "sensitivity": "",
            "storage_channel_id": 1002,
            "storage_partitions": 1,
            "tags": {
                "manage": {
                    "geog_area": [
                        {
                            "alias": "中国内地",
                            "code": "inland",
                            "description": "中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，" "多用于与香港、澳门特别行政区同时出现的语境",
                            "id": 123,
                            "kpath": 0,
                            "seq_index": 0,
                            "sync": 0,
                            "tag_type": "manage",
                        }
                    ]
                },
                "system": [],
            },
            "topic": "raw_data_test591",
            "topic_name": None,
            "updated_at": None,
            "updated_by": None,
        },
        "bk_app_code": "",
        "bk_biz_id": 591,
        "data_scenario": "tdw",
        "description": "",
    }
    result = res["data"]
    assert result["access_conf_info"] == want["access_conf_info"]
    assert result["access_raw_data"]["id"] == want["access_raw_data"]["id"]
    assert result["access_raw_data"]["storage_channel_id"] == want["access_raw_data"]["storage_channel_id"]


@pytest.mark.django_db
@pytest.mark.usefixtures("init_access_manager_config")
@pytest.mark.usefixtures("init_access_time_db_resource")
@pytest.mark.usefixtures("add_channel")
def test_no_record(mocker):
    deploy_test_setup(mocker)

    url = "/v3/access/deploy_plan/321/?bk_username=admin"
    res = get(url)

    assert not res["result"]
    assert res["code"] == "1576203"
    assert len(res["message"]) > 0
    assert res["message"] == "不存在该数据"
