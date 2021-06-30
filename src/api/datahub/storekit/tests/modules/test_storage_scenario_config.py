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
import pytest

from datahub.storekit.tests.fixture.storage_scenario_config_fixture import (
    add_storage_scenario_config,
    delete_storage_scenario_config,
)
from datahub.storekit.tests.utils.common_util import get_client

client = get_client()


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_scenario_config")
def test_create():
    """
    测试创建storage_scenario_config成功
    """
    create_params = {
        "storage_scenario_name": "OLAP",
        "storage_scenario_alias": "OLAP scenario",
        "description": "OLAP scenario description",
    }
    res = client.post("/v3/storekit/storage_scenario_configs/", create_params)
    assert res.is_success() is True

    compare_dict = {
        "code": "1500200",
        "data": {
            "active": False,
            "created_by": "",
            "description": "OLAP scenario description",
            "storage_scenario_alias": "OLAP scenario",
            "storage_scenario_name": "OLAP",
            "updated_by": "",
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }
    del res.response["data"]["created_at"]
    del res.response["data"]["updated_at"]
    assert res.response == compare_dict


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync")
def test_retrieve():
    """
    测试获取单个storage_scenario_config
    """
    params = {"storage_scenario_name": "OLAP"}
    res = client.get("/v3/storekit/storage_scenario_configs/%s/" % (params["storage_scenario_name"]), params)
    assert res.is_success() is True

    compare_dict = {
        "code": "1500200",
        "data": [
            {
                "active": False,
                "created_by": "",
                "description": "OLAP scenario description",
                "storage_scenario_alias": "OLAP scenario",
                "storage_scenario_name": "OLAP",
                "updated_by": "",
            }
        ],
        "errors": None,
        "message": "ok",
        "result": True,
    }
    del res.response["data"][0]["created_at"]
    del res.response["data"][0]["updated_at"]
    assert res.response == compare_dict


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_scenario_config", "add_storage_scenario_config")
def test_list():
    """
    测试获取全部storage_scenario_config
    """
    res = client.get("/v3/storekit/storage_scenario_configs/")
    assert res.is_success() is True

    compare_dict = {
        "code": "1500200",
        "data": [
            {
                "active": False,
                "created_by": None,
                "description": "OLAP scenario description",
                "storage_scenario_alias": "OLAP scenario",
                "storage_scenario_name": "OLAP",
                "updated_by": None,
            },
            {
                "active": False,
                "created_by": None,
                "description": "Search scenario description",
                "storage_scenario_alias": "Search scenario",
                "storage_scenario_name": "Search",
                "updated_by": None,
            },
        ],
        "errors": None,
        "message": "ok",
        "result": True,
    }
    del res.response["data"][0]["created_at"]
    del res.response["data"][0]["updated_at"]
    del res.response["data"][1]["created_at"]
    del res.response["data"][1]["updated_at"]
    assert res.response == compare_dict


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_scenario_config", "add_storage_scenario_config")
def test_update():
    """
    测试更新storage_scenario_config
    """
    params = {
        "storage_scenario_name": "OLAP",
        "storage_scenario_alias": "OLAP scenario2",
        "description": "OLAP scenario description2",
    }
    res = client.put("/v3/storekit/storage_scenario_configs/%s/" % (params["storage_scenario_name"]), params)
    assert res.is_success() is True

    compare_dict = {
        "code": "1500200",
        "data": {
            "active": False,
            "created_by": None,
            "description": "OLAP scenario description2",
            "storage_scenario_alias": "OLAP scenario2",
            "storage_scenario_name": "OLAP",
            "updated_by": None,
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }
    del res.response["data"]["created_at"]
    del res.response["data"]["updated_at"]
    assert res.response == compare_dict


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "add_storage_scenario_config")
def test_delete():
    """
    测试删除storage_scenario_config
    """
    params = {"storage_scenario_name": "OLAP"}
    res = client.delete("/v3/storekit/storage_scenario_configs/%s/" % (params["storage_scenario_name"]), params)
    assert res.is_success() is True
    assert res.data == "destroy 1 records, with storage_scenario_name=OLAP"
