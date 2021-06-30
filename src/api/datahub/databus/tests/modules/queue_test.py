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
from datahub.access.tests.utils import delete, get, post

from datahub.databus import settings


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_queue_channel")
def test_queue_users():
    # 添加用户 test-abc，验证添加成功
    res = post("/v3/databus/queue_users/", {"user": "test-abc", "password": "fortest"})
    assert res["result"]

    # 调用list接口，确认 test-abc 账号存在
    res = get("/v3/databus/queue_users/")
    assert res["result"]
    for key in res["data"]:
        assert "test-abc" in res["data"][key]

    # 调用删除接口，删除用户 test-abc
    res = delete("/v3/databus/queue_users/test-abc/")
    assert res["result"]

    # 调用list接口，确认 test-abc 账户已不存在
    res = get("/v3/databus/queue_users/")
    assert res["result"]
    for key in res["data"]:
        assert "test-abc" not in res["data"][key]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_queue_channel")
def test_queue_auth():
    # 添加队列服务权限
    res = post(
        "/v3/databus/queue_auths/add_auths/",
        {"user": "data-app", "result_table_ids": ["101_etl_abc", "134_join_cal"]},
    )
    assert res["result"]

    # 获取队列服务权限列表，验证topic和group的权限是否正确
    res = get("/v3/databus/queue_auths/")
    assert res["result"]
    for key in res["data"]:
        assert "Topic:queue_101_etl_abc" in res["data"][key]["topic"]
        assert "Topic:queue_134_join_cal" in res["data"][key]["topic"]
        assert "Group:queue-data-app" in res["data"][key]["group"]

    # 删除队列服务权限授权，验证操作成功
    res = post(
        "/v3/databus/queue_auths/remove_auths/",
        {"user": "data-app", "result_table_ids": ["101_etl_abc", "134_join_cal"]},
    )
    assert res["result"]

    # 获取队列服务权限列表，验证topic的权限已经被删除
    res = get("/v3/databus/queue_auths/")
    assert res["result"]
    for key in res["data"]:
        assert "Topic:queue_101_etl_abc" not in res["data"][key]["topic"]
        assert "Topic:queue_134_join_cal" not in res["data"][key]["topic"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_queue_channel")
def test_queue_exceptions():
    settings.KAFKA_CONFIG_SCRIPT = "/bin/bash"
    settings.KAFKA_ACL_SCRIPT = "/bin/bash"

    # 调用list user接口
    res = get("/v3/databus/queue_users/")
    assert res["result"]
    assert res["data"]["zookeeper:2181/"] == {}

    # 获取队列服务权限列表
    res = get("/v3/databus/queue_auths/")
    assert res["result"]
    assert res["data"]["zookeeper:2181/"] == {}
