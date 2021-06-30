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
import httpretty as hp
import pytest

import datahub.storekit.tests.utils.conftest as conftest
from datahub.storekit.tests.fixture.storage_cluster_config_fixture import (
    add_storage_cluster_config,
    delete_storage_cluster_config,
)
from datahub.storekit.tests.http_utils import delete, get, post, put
from datahub.storekit.tests.utils.constants import MYSQL_CLUSTER_URL

CREATE_PARAM = {
    "cluster_type": "mysql",
    "cluster_name": "mysql-test",
    "cluster_group": "default",
    "tags": ["inland", "usr", "enable"],
    "connection_info": '{"host": "mysql.host", "password": "mysql.password", "port": 25000, "user": "mysql.user"}',
    "priority": 0,
    "version": "1.0.0",
    "expires": '{"min_expire":1, "max_expire":1, "list_expire":[ { "name":"1天", "value":"1d" }]}',
    "belongs_to": "anonymous",
    "created_by": "anonymous",
    "updated_by": "anonymous",
    "description": "测试集群",
}


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("patch_meta_sync")
def test_create_failed():
    """
    测试创建cluster失败
    """
    del CREATE_PARAM["expires"]
    res = post(MYSQL_CLUSTER_URL, CREATE_PARAM)
    assert res["result"] is False
    assert res["code"] == "1500001"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_cluster_config", "add_storage_cluster_config")
def test_retrieve():
    """
    测试获取单个cluster信息
    """
    hp.enable()
    conftest.mock_meta_content_language_configs()

    cluster_name = "mysql-test"
    res = get("{}{}/".format(MYSQL_CLUSTER_URL, cluster_name))
    assert res["result"] is True
    assert res["data"]["cluster_type"] == "mysql"
    assert res["data"]["cluster_name"] == "mysql-test"
    assert res["data"]["cluster_group"] == "default"
    assert res["data"]["priority"] == 0
    assert res["data"]["version"] == "1.0.0"
    assert res["data"]["belongs_to"] == "anonymous"
    assert res["data"]["description"] == "MySQL测试集群"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_cluster_config", "add_storage_cluster_config")
def test_delete():
    """
    测试删除cluster信息
    """
    conftest.mock_sycn_meta()

    cluster_name = "mysql-test"
    res = delete("{}{}/".format(MYSQL_CLUSTER_URL, cluster_name))
    assert res["result"] is True


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_cluster_config")
def test_create_success():
    """
    测试创建cluster成功
    """
    hp.enable()

    conftest.mock_sycn_meta()
    conftest.mock_meta_tag_target()
    conftest.mock_resource_create_cluster()
    conftest.mock_resource_get_cluster()
    conftest.mock_resource_update_cluster()

    res = post(MYSQL_CLUSTER_URL, CREATE_PARAM)
    assert res["result"] is True
    assert res["data"]["cluster_type"] == "mysql"
    assert res["data"]["cluster_name"] == "mysql-test"
    assert res["data"]["cluster_group"] == "default"
    assert res["data"]["priority"] == 0
    assert res["data"]["version"] == "1.0.0"
    assert res["data"]["belongs_to"] == "anonymous"
    assert res["data"]["description"] == "测试集群"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("patch_meta_sync", "delete_storage_cluster_config", "add_storage_cluster_config")
def test_update():
    """
    测试更新cluster信息
    """
    hp.enable()

    conftest.mock_sycn_meta()
    conftest.mock_meta_tag_target()
    conftest.mock_resource_create_cluster()
    conftest.mock_resource_get_cluster()
    conftest.mock_resource_update_cluster()

    CREATE_PARAM["version"] = "1.0.1"
    cluster_name = "mysql-test"
    res = put("{}{}/".format(MYSQL_CLUSTER_URL, cluster_name), CREATE_PARAM)

    assert res["result"] is True
    assert res["data"]["cluster_name"] == "mysql-test"
    assert res["data"]["version"] == "1.0.1"
