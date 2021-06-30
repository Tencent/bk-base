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

import json

import httpretty as hp
import pytest
from common.transaction import sync_model_data  # noqa
from datahub.access.tests.utils import delete, get, post, put
from datahub.databus.tests.conftest import patch_meta_sync  # noqa
from datahub.databus.tests.fixture.channel_fixture import *  # noqa
from datahub.databus.tests.fixture.channel_fixture import (  # noqa
    add_channel,
    delete_channel_id,
)
from datahub.databus.tests.mock_api import meta, result_table
from datahub.databus.tests.mock_api.config_server import post_update_stream_to_ok


@pytest.mark.usefixtures("add_channel")
@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_success():
    hp.enable()
    meta.post_sync_hook_ok()

    PARAMS = {
        "cluster_name": "testouter-1",
        "cluster_type": "kafka",
        "cluster_role": "inner",
        "cluster_domain": "xx.xx.xx",
        "cluster_backup_ips": "",
        "cluster_port": 9092,
        "zk_domain": "xx.xx.xx",
        "zk_port": 2181,
        "zk_root_path": "/kafka-test-3",
        "active": True,
        "priority": 0,
        "attribute": "bkdata",
        "description": "",
    }

    res = post("/v3/databus/channels/", PARAMS)
    compare_json = {
        "code": "1500200",
        "data": {
            "active": True,
            "attribute": "bkdata",
            "cluster_backup_ips": "",
            "cluster_domain": "xx.xx.xx",
            "cluster_name": "testouter-1",
            "cluster_port": 9092,
            "cluster_role": "inner",
            "cluster_type": "kafka",
            "created_by": "",
            "description": "",
            "ip_list": "",
            "priority": 0,
            "storage_name": None,
            "stream_to_id": None,
            "updated_by": "",
            "zk_domain": "xx.xx.xx",
            "zk_port": 2181,
            "zk_root_path": "/kafka-test-3",
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }
    del res["data"]["id"]
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)

    # 重复提交时，由于channel_name冲突，抛出异常
    res = post("/v3/databus/channels/", PARAMS)
    assert not res["result"]
    assert res["code"] == "1500005"
    assert res["message"] == u"对象已存在，无法添加：databus_channel[cluster_name=testouter-1]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_retrieve_success():
    """dataid存在"""

    res = get("/v3/databus/channels/1000/")
    assert res["result"]
    assert res["data"]["cluster_name"] == "kafka_cluster_name"
    assert res["data"]["cluster_type"] == "kafka"
    assert res["data"]["cluster_role"] == "inner"
    assert res["data"]["cluster_domain"] == "kafka.domain1"
    assert res["data"]["cluster_port"] == 9092
    assert res["data"]["zk_root_path"] == "/"
    assert res["data"]["zk_domain"] == "zk.domain1"
    assert res["data"]["zk_port"] == 2181
    assert res["data"]["priority"] == 1


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_retrieve_except_value_error():
    """dataid存在"""

    res = get("/v3/databus/channels/kafka_cluster_name/")
    assert res["result"]
    assert res["data"]["cluster_name"] == "kafka_cluster_name"
    assert res["data"]["cluster_type"] == "kafka"
    assert res["data"]["cluster_role"] == "inner"
    assert res["data"]["cluster_domain"] == "kafka.domain1"
    assert res["data"]["cluster_port"] == 9092
    assert res["data"]["zk_root_path"] == "/"
    assert res["data"]["zk_domain"] == "zk.domain1"
    assert res["data"]["zk_port"] == 2181
    assert res["data"]["priority"] == 1


@pytest.mark.httpretty
@pytest.mark.django_db
def test_retrieve_data_not_found():
    """dataid不存在"""

    res = get("/v3/databus/channels/1000/")
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_channel[id/name=1000]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_retrieve_channel_not_found():
    """channel_name不存在"""

    res = get("/v3/databus/channels/kafka1/")
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_channel[id/name=kafka1]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_update_success():
    """dataid存在"""
    hp.enable()
    post_update_stream_to_ok()
    meta.post_sync_hook_ok()

    PARAMS = {
        "cluster_name": "testouter",
        "cluster_type": "kafka",
        "cluster_role": "inner",
        "cluster_domain": "xx.xx.xx",
        "cluster_backup_ips": "",
        "cluster_port": 9092,
        "zk_domain": "xx.xx.xx",
        "zk_port": 2181,
        "zk_root_path": "/kafka-test-3",
        "active": True,
        "priority": 0,
        "attribute": "bkdata",
        "description": "",
    }

    res = put("/v3/databus/channels/1000/", PARAMS)
    assert res["result"]
    assert res["data"]["cluster_name"] == "testouter"
    assert res["data"]["cluster_type"] == "kafka"
    assert res["data"]["cluster_role"] == "inner"
    assert res["data"]["cluster_domain"] == "xx.xx.xx"
    assert res["data"]["cluster_port"] == 9092
    assert res["data"]["zk_root_path"] == "/kafka-test-3"
    assert res["data"]["zk_domain"] == "xx.xx.xx"
    assert res["data"]["zk_port"] == 2181
    assert res["data"]["priority"] == 0


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_update_except_value_error():
    """dataid存在"""
    hp.enable()
    post_update_stream_to_ok()
    meta.post_sync_hook_ok()
    PARAMS = {
        "cluster_name": "testouter",
        "cluster_type": "kafka",
        "cluster_role": "inner",
        "cluster_domain": "xx.xx.xx",
        "cluster_backup_ips": "",
        "cluster_port": 9092,
        "zk_domain": "xx.xx.xx",
        "zk_port": 2181,
        "zk_root_path": "/kafka-test-3",
        "active": True,
        "priority": 0,
        "attribute": "bkdata",
        "description": "",
    }

    res = put("/v3/databus/channels/kafka_cluster_name/", PARAMS)
    assert res["result"]
    assert res["data"]["cluster_name"] == "testouter"
    assert res["data"]["cluster_type"] == "kafka"
    assert res["data"]["cluster_role"] == "inner"
    assert res["data"]["cluster_domain"] == "xx.xx.xx"
    assert res["data"]["cluster_port"] == 9092
    assert res["data"]["zk_root_path"] == "/kafka-test-3"
    assert res["data"]["zk_domain"] == "xx.xx.xx"
    assert res["data"]["zk_port"] == 2181
    assert res["data"]["priority"] == 0


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_update_data_not_found():
    """dataid不存在"""
    hp.enable()
    post_update_stream_to_ok()
    meta.post_sync_hook_ok()
    PARAMS = {
        "cluster_name": "testouter-not-exist",
        "cluster_type": "kafka",
        "cluster_role": "inner",
        "cluster_domain": "xx.xx.xx",
        "cluster_backup_ips": "",
        "cluster_port": 9092,
        "zk_domain": "xx.xx.xx",
        "zk_port": 2181,
        "zk_root_path": "/kafka-test-3",
        "active": True,
        "priority": 0,
        "attribute": "bkdata",
        "description": "",
    }

    res = put("/v3/databus/channels/testouter-not-exist/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_channel[id/name=testouter-not-exist]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_destory_success():
    """dataid存在"""
    hp.enable()
    post_update_stream_to_ok()
    meta.post_sync_hook_ok()
    res = delete("/v3/databus/channels/1000/")
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_destory_except_value_error():
    """dataid存在"""

    res = delete("/v3/databus/channels/kafka_cluster_name/")
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_destory_data_not_found():
    """dataid不存在"""
    hp.enable()
    meta.post_sync_hook_ok()
    res = delete("/v3/databus/channels/100000/")
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_channel[id/name=100000]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_list():
    """dataid存在"""

    res = get("/v3/databus/channels/")
    assert res["result"]
    assert len(res["data"]) == 4
    assert res["data"][0]["cluster_name"] == "kafka_cluster_name"
    assert res["data"][0]["cluster_type"] == "kafka"
    assert res["data"][0]["cluster_role"] == "inner"
    assert res["data"][0]["cluster_domain"] == "kafka.domain1"
    assert res["data"][0]["cluster_backup_ips"] == ""
    assert res["data"][0]["cluster_port"] == 9092
    assert res["data"][0]["zk_domain"] == "zk.domain1"
    assert res["data"][0]["zk_port"] == 2181
    assert res["data"][0]["zk_root_path"] == "/"
    assert res["data"][0]["priority"] == 1

    assert res["data"][1]["cluster_name"] == "kafka_cluster_name2"
    assert res["data"][1]["cluster_type"] == "kafka"
    assert res["data"][1]["cluster_role"] == "inner"
    assert res["data"][1]["cluster_domain"] == "kafka.domain1"
    assert res["data"][1]["cluster_backup_ips"] == ""
    assert res["data"][1]["cluster_port"] == 9092
    assert res["data"][1]["zk_domain"] == "zk.domain1"
    assert res["data"][1]["zk_port"] == 2181
    assert res["data"][1]["zk_root_path"] == "/"
    assert res["data"][1]["priority"] == 1

    assert res["data"][2]["cluster_name"] == "kafka_cluster_name3"
    assert res["data"][2]["cluster_type"] == "kafka"
    assert res["data"][2]["cluster_role"] == "outer"
    assert res["data"][2]["cluster_domain"] == "kafka.domain1"
    assert res["data"][2]["cluster_backup_ips"] == ""
    assert res["data"][2]["cluster_port"] == 9092
    assert res["data"][2]["zk_domain"] == "zk.domain1"
    assert res["data"][2]["zk_port"] == 2181
    assert res["data"][2]["zk_root_path"] == "/"
    assert res["data"][2]["priority"] == 2


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_inner_to_use_success():
    """dataid存在"""
    hp.enable()
    result_table.get_2_output_ok()
    res = get("/v3/databus/channels/inner_to_use/?result_table_id=2_output")
    assert res["result"]
    assert res["data"]["cluster_name"] == "kafka_cluster_name"
    assert res["data"]["cluster_type"] == "kafka"
    assert res["data"]["cluster_role"] == "inner"
    assert res["data"]["cluster_domain"] == "kafka.domain1"
    assert res["data"]["cluster_port"] == 9092
    assert res["data"]["zk_root_path"] == "/"
    assert res["data"]["zk_domain"] == "zk.domain1"
    assert res["data"]["zk_port"] == 2181
    assert res["data"]["priority"] == 1
    assert res["data"]["id"] == 1000


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("delete_channel_id")
def test_inner_to_use_failed():
    """dataid不存在"""
    hp.enable()
    result_table.get_2_output_ok()
    res = get("/v3/databus/channels/inner_to_use/?result_table_id=2_output")
    assert not res["result"]
    assert res["message"] == u"没有可用的inner channel，请先初始化inner channel的信息"
