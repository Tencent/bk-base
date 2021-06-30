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

import httpretty
import pytest
from datahub.access.tests.utils import delete, get, patch, post
from datahub.databus.tests.fixture.channel_fixture import (  # noqa
    add_channel,
    del_cluster,
)
from datahub.databus.tests.fixture.cluster_fixture import add_cluster  # noqa
from datahub.databus.tests.fixture.connector_cluster import (  # noqa
    test_connector_cluster,
)
from datahub.databus.tests.fixture.databus_connector_task import (  # noqa
    test_databus_connector_task,
    test_databus_connector_task_dup,
)
from datahub.databus.tests.fixture.mock_response import Ret
from datahub.databus.tests.mock_api import meta
from datahub.databus.tests.mock_api.meta import get_inland_tag_ok


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
@pytest.mark.usefixtures("del_cluster")
def test_cluster_add_failed():
    httpretty.enable()
    meta.post_sync_hook_ok()
    PARAMS = {
        "cluster_name": "clean-testouter-M",
        "channel_name": "kafka_cluster_name_not_exists",
        "module": "clean",
        "component": "clean",
        "cluster_rest_port": 10100,
        "state": "RUNNING",
        "cluster_rest_domain": "x.x.x.x",
        "limit_per_day": 1440000,
        "priority": 10,
        "description": "",
    }
    # 测试当kafka config channel不存在时，创建总线集群会失败
    res = post("/v3/databus/clusters/", PARAMS)
    """{'code': '1570039', 'data': None, 'errors': None, 'message': '没有可用的config channel，请先初始化config channel的
    信息', ...} is None"""
    assert not res["result"]
    assert res["code"] == "1500004"
    assert res["data"] is None
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=kafka_cluster_name_not_exists]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
@pytest.mark.usefixtures("del_cluster")
def test_cluster_add_success():
    httpretty.enable()
    meta.post_sync_hook_ok()
    PARAMS = {
        "cluster_name": "clean-testouter-M",
        "channel_name": "kafka_cluster_name3",
        "module": "clean",
        "component": "clean",
        "cluster_rest_port": 10100,
        "state": "RUNNING",
        "cluster_rest_domain": "x.x.x.x",
        "limit_per_day": 1440000,
        "priority": 10,
        "description": "",
    }

    # 调用创建cluster接口
    res = post("/v3/databus/clusters/", PARAMS)
    assert res["result"]
    assert res["data"]["consumer_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_name"] == "clean-testouter-M"
    assert res["data"]["module"] == "clean"
    assert res["data"]["component"] == "clean"
    assert res["data"]["state"] == "RUNNING"
    assert res["data"]["cluster_rest_port"] == 10100
    assert res["data"]["cluster_rest_domain"] == "x.x.x.x"
    assert res["data"]["limit_per_day"] == 1440000
    assert res["data"]["priority"] == 10

    cluster_props = json.loads(res["data"]["cluster_props"])
    assert cluster_props["key.converter"] == "org.apache.kafka.connect.storage.StringConverter"
    assert cluster_props["value.converter"] == "com.tencent.bkdata.connect.common.convert.RawConverter"

    consumer_props = json.loads(res["data"]["consumer_props"])
    assert consumer_props["auto.offset.reset"] == "earliest"
    assert consumer_props["max.poll.records"] == 500

    monitor_props = json.loads(res["data"]["monitor_props"])
    assert monitor_props["producer.acks"] == "all"
    assert monitor_props["producer.topic"] == "bkdata_data_monitor_databus_metrics591"
    assert monitor_props["producer.compression.type"] == "snappy"

    other_props = json.loads(res["data"]["other_props"])
    assert other_props["producer.acks"] == "all"
    assert other_props["producer.retries"] == 0
    assert other_props["producer.max.in.flight.requests.per.connection"] == 1

    # 测试获取集群配置信息
    res = get("/v3/databus/clusters/clean-testouter-M/")
    assert res["result"]
    assert res["data"]["consumer_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_name"] == "clean-testouter-M"
    assert res["data"]["module"] == "clean"
    assert res["data"]["component"] == "clean"
    assert res["data"]["state"] == "RUNNING"
    assert res["data"]["cluster_rest_port"] == 10100
    assert res["data"]["cluster_rest_domain"] == "x.x.x.x"
    assert res["data"]["limit_per_day"] == 1440000
    assert res["data"]["priority"] == 10

    res = patch("/v3/databus/clusters/clean-testouter-M/", {"other_props": "abc"})
    assert res["result"] is False
    assert res["data"] is None
    assert res["code"] == "1500001"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
@pytest.mark.usefixtures("del_cluster")
def test_add_puller_success():
    httpretty.enable()
    meta.post_sync_hook_ok()
    PARAMS = {
        "cluster_name": "puller-datanode-M",
        "channel_name": "kafka_cluster_name3",
        "module": "puller",
        "component": "datanode",
        "cluster_rest_port": 10001,
        "state": "RUNNING",
        "cluster_rest_domain": "x.x.x.x",
        "limit_per_day": 1440000,
        "priority": 10,
        "description": "",
    }
    # 测试创建puller类型的cluster
    res = post("/v3/databus/clusters/", PARAMS)
    assert res["result"]
    assert res["data"]["consumer_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster_name"] == "puller-datanode-M"
    assert res["data"]["module"] == "puller"
    assert res["data"]["component"] == "datanode"
    assert res["data"]["state"] == "RUNNING"
    assert res["data"]["cluster_rest_port"] == 10001
    assert res["data"]["cluster_rest_domain"] == "x.x.x.x"
    assert res["data"]["limit_per_day"] == 1440000
    assert res["data"]["priority"] == 10

    other_props = json.loads(res["data"]["other_props"])
    assert other_props["producer.acks"] == "all"
    assert other_props["producer.retries"] == 5
    assert other_props["producer.max.in.flight.requests.per.connection"] == 5

    # 测试部分更新
    res = patch(
        "/v3/databus/clusters/puller-datanode-M/",
        {"cluster_rest_domain": "*.*.*.*", "limit_per_day": 14400, "priority": 100},
    )
    # assert res["result"]

    # 测试部分更新失败
    res = patch(
        "/v3/databus/clusters/eslog-testouter-M/",
        {"cluster_rest_domain": "*.*.*.*", "limit_per_day": 14400, "priority": 100},
    )
    assert res["result"] is False
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=eslog-testouter-M]"

    # 测试cluster列表接口
    res = get("/v3/databus/clusters/")
    assert res["result"]
    assert len(res["data"]) == 1
    assert res["data"][0]["consumer_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"][0]["cluster_bootstrap_servers"] == "kafka.domain1:9092"
    assert res["data"][0]["cluster_name"] == "puller-datanode-M"
    assert res["data"][0]["module"] == "puller"
    assert res["data"][0]["component"] == "datanode"

    # 测试get_cluster_info接口
    get_inland_tag_ok()
    res = get("/v3/databus/clusters/get_cluster_info/?cluster_name=puller-datanode-M")
    assert res["result"]
    assert res["data"]["cluster.group.id"] == "puller-datanode-M"
    assert res["data"]["cluster.rest.port"] == 10001
    assert res["data"]["cluster.bootstrap.servers"] == "kafka.domain1:9092"
    assert res["data"]["cluster.offset.storage.topic"] == "connect-offsets.puller-datanode-M"
    assert res["data"]["cluster.config.storage.topic"] == "connect-configs.puller-datanode-M"
    assert res["data"]["cluster.status.storage.topic"] == "connect-status.puller-datanode-M"

    # 重复提交时，由于主键冲突，抛出异常
    res = post("/v3/databus/clusters/", PARAMS)
    assert not res["result"]
    assert res["code"] == "1500007"

    res = post("/v3/databus/clusters/", PARAMS)
    assert not res["result"]
    assert res["data"] is None
    assert res["code"] == "1500007"

    res = get("/v3/databus/clusters/puller-datanode-M/check/")
    assert res["result"]
    assert res["data"]["checked_count"] == 0
    assert res["data"]["cluster"] == "puller-datanode-M"
    assert res["data"]["bad_connectors"] == {}
    assert res["data"]["check_failed"] == []


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task")
def test_find_connector():
    # 获取一个不存在的总线任务的路由信息
    res = get("/v3/databus/clusters/find_connector/?connector=abc")
    assert res["result"]
    assert res["data"] is None

    # 获取一个已存在的任务路由信息
    res = get("/v3/databus/clusters/find_connector/?connector=mysql-table_10000_test_clean_rt")
    assert res["result"]
    assert res["data"]["cluster_name"] == "mysql-kafka_cluster_name-M"
    assert res["data"]["connector_task_name"] == "mysql-table_10000_test_clean_rt"
    assert res["data"]["sink_type"] == "mysql"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
def test_list_connectors(mocker):
    # 请求 connectors rest接口返回404错误
    mocker.patch("requests.get", return_value=Ret(404, ""))
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/")
    print(res)
    assert res["result"]
    assert res["data"]["count"] == -1
    assert res["data"]["connectors"] == []

    # 请求connectors rest接口返回正常结果
    mocker.patch("requests.get", return_value=Ret(200, '["mysql-table_10000_test_clean_rt"]'))
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/")
    assert res["result"]
    assert res["data"]["count"] == 1
    assert res["data"]["connectors"] == ["mysql-table_10000_test_clean_rt"]

    # 总线集群不存在
    res = get("/v3/databus/clusters/mysql-xxx-M/connectors/")
    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=mysql-xxx-M]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
def test_delete_connectors(mocker):
    # 删除总线任务
    # 总线集群不存在
    res = delete("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/")
    assert res["result"] is False

    # 总线集群存在，删除失败
    mocker.patch(
        "requests.delete",
        return_value=Ret(
            404,
            '{"error_code":404, "message": "Connector mysql-table_10000_test_clean_rt not found"}',
        ),
    )
    res = delete("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/")
    """{'code': '1500200', 'data': None, 'errors': None, 'message': '404 {"error_code":404, "message": "Connector
    mysql-table_10000_test_clean_rt not found"}', ...}"""
    assert res["result"] is False
    assert res["message"] == '404 {"error_code":404, "message": "Connector mysql-table_10000_test_clean_rt not found"}'

    # 总线集群存在，删除成功
    mocker.patch("requests.delete", return_value=Ret(204, ""))
    res = delete("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/")
    """{'code': '1500200', 'data': None, 'errors': None, 'message': '', ...}"""
    assert res["result"]
    assert res["data"] is None
    assert (
        res["message"]
        == "delete connector mysql-table_10000_test_clean_rt success in cluster mysql-kafka_cluster_name-M"
    )


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
def test_get_connectors(mocker):
    # 获取总线任务信息
    # 总线集群不存在
    res = get("/v3/databus/clusters/mysql-xxx-M/connectors/mysql-table_10000_test_clean_rt/")
    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=mysql-xxx-M]"

    # 总线集群存在
    mocker.patch(
        "requests.get",
        side_effect=[
            Ret(
                200,
                '{"tasks": [{"connector": "mysql-table_591_sadf", "task": 0}], "config": {"tasks.max": "1"}, "name":'
                ' "mysql-table_591_sadf"}',
            ),
            Ret(
                200,
                '{"connector": {"state": "RUNNING","worker_id": "x.x.x.x:10000"},"tasks": [{"state": "RUNNING",'
                '"worker_id": "x.x.x.x:10000","id": 0}],"name": "mysql-table_591_sadf"}',
            ),
            Ret(
                200,
                '[{"config": {"tasks.max": "1"},"id": {"connector": "mysql-table_591_sadf","task": 0}}]',
            ),
        ],
    )
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_591_sadf/")
    assert res["result"]
    assert "name" in res["data"]["config"]
    assert "config" in res["data"]["config"]
    assert "tasks" in res["data"]["config"]
    assert "name" in res["data"]["status"]
    assert "connector" in res["data"]["status"]
    assert "tasks" in res["data"]["status"]
    assert len(res["data"]["tasks"]) == 1
    assert "config" in res["data"]["tasks"][0]
    assert "id" in res["data"]["tasks"][0]

    mocker.patch(
        "requests.get",
        side_effect=[
            Ret(
                200,
                '{"tasks": [{"connector": "mysql-table_591_sadf", "task": 0}], "config": {"tasks.max": "1"}, "name": '
                '"mysql-table_591_sadf"}',
            ),
            Ret(
                200,
                '{"connector": {"state": "RUNNING","worker_id": "x.x.x.x:10000"},"tasks": [{"state": "RUNNING",'
                '"worker_id": "x.x.x.x:10000","id": 0}],"name": "mysql-table_591_sadf"}',
            ),
            Ret(404, "bad args"),
        ],
    )
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_591_sadf/")
    assert not res["result"]
    assert res["message"] == "404 bad args"

    mocker.patch(
        "requests.get",
        side_effect=[
            Ret(
                200,
                '{"tasks": [{"connector": "mysql-table_591_sadf", "task": 0}], "config": {"tasks.max": "1"}, '
                '"name": "mysql-table_591_sadf"}',
            ),
            Ret(409, "no tasks"),
        ],
    )
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_591_sadf/")
    assert not res["result"]
    assert res["message"] == "409 no tasks"

    mocker.patch("requests.get", return_value=Ret(404, "connector not exists"))
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_591_sadf/")
    assert not res["result"]
    assert res["message"] == "404 connector not exists"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
def test_restart_connectors(mocker):
    # 重启总线任务
    # 总线集群不存在
    res = get("/v3/databus/clusters/mysql-xxx-M/connectors/mysql-table_10000_test_clean_rt/restart/")
    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=mysql-xxx-M]"

    # 总线集群存在，正常流程
    mocker.patch(
        "requests.get",
        return_value=Ret(
            200,
            '{"tasks": [{"connector": "mysql-table_10000_test_clean_rt", "task": 0}], '
            '"config": {"tasks.max": "1"}, "name": '
            '"mysql-table_591_sadf"}',
        ),
    )
    mocker.patch("requests.delete", return_value=Ret(200, ""))
    mocker.patch(
        "requests.post",
        return_value=Ret(
            200,
            '{"tasks": [{"connector": "mysql-table_10000_test_clean_rt", "task": 0}], "config": {"tasks.max": "1"}, '
            '"name": "mysql-table_10000_test_clean_rt"}',
        ),
    )
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/restart/")
    print(res)
    assert res["result"]
    assert res["data"] == "mysql-table_10000_test_clean_rt"
    assert res["message"] == ""

    # 再次添加到集群中失败
    mocker.patch("requests.post", return_value=Ret(409, "unable to connect"))
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/restart/")
    assert not res["result"]
    assert res["data"] == "mysql-table_10000_test_clean_rt"
    assert res["message"] == "add connector failed! 409 unable to connect"

    # 获取任务配置失败
    mocker.patch("requests.get", return_value=Ret(404, "error"))
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/restart/")
    assert not res["result"]
    assert res["data"] == "mysql-table_10000_test_clean_rt"
    assert res["message"] == "get connector config failed! 404 error"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
def test_move_connectors(mocker):
    # 集群之间迁移总线任务
    # 目的地参数未提供
    res = get("/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/")
    assert res["result"] is False

    # 目的地集群和原始集群一样
    mocker.patch(
        "requests.get",
        return_value=Ret(
            200,
            '{"tasks": [{"connector": "mysql-table_10000_test_clean_rt", "task": 0}], '
            '"config": {"tasks.max": "1"}, "name": '
            '"mysql-table_10000_test_clean_rt"}',
        ),
    )
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/?"
        "dest_cluster=mysql-kafka_cluster_name-M"
    )
    assert res["result"] is False
    assert res["message"] == "unable to move connector from mysql-kafka_cluster_name-M to mysql-kafka_cluster_name-M"

    print("源集群不存在")
    # 目的地集群和原始集群类型不一样
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/"
        "?dest_cluster=puller-datanode-M"
    )
    assert res["result"] is False
    assert res["message"] == "unable to move connector from mysql-kafka_cluster_name-M to puller-datanode-M"

    # 目的地集群不存在
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/?"
        "dest_cluster=mysql-kafka_cluster_name-xxx"
    )
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=mysql-kafka_cluster_name-xxx]"
    assert res["code"] == "1500004"
    # 源集群不存在
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M3/connectors/mysql-table_10000_test_clean_rt/move_to/?"
        "dest_cluster=mysql-kafka_cluster_name-xxx"
    )
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_cluster[cluster_name=mysql-kafka_cluster_name-M3]"
    assert res["code"] == "1500004"

    # 路由信息不对
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M1/connectors/mysql-table_10000_test_clean_rt/move_to/?"
        "dest_cluster=mysql-kafka_cluster_name-M"
    )
    assert not res["result"]
    assert res["message"] == "connector mysql-table_10000_test_clean_rt is not in mysql-kafka_cluster_name-M1"

    # 路由信息为空
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test/move_to/?dest_cluster="
        "mysql-kafka_cluster_name-M1"
    )
    assert not res["result"]
    assert res["message"] == "no route info for connector mysql-table_10000_test"

    # 路由正常，请求任务配置失败
    mocker.patch("requests.get", return_value=Ret(404, "error"))
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/?"
        "dest_cluster=mysql-kafka_cluster_name-M1"
    )
    assert not res["result"]
    assert (
        res["message"]
        == "get connector mysql-table_10000_test_clean_rt config in mysql-kafka_cluster_name-M failed! 404 error"
    )

    # 路由正常，操作正常
    mocker.patch(
        "requests.get",
        return_value=Ret(
            200,
            '{"config": {"tasks.max": "1", "group.id": "mysql-kafka_cluster_name-M"}, "name": "mysql-table_1'
            '0000_test_clean_rt"}',
        ),
    )
    mocker.patch("requests.delete", return_value=Ret(200, ""))
    mocker.patch(
        "requests.post",
        return_value=Ret(
            200,
            '{"config": {"tasks.max": "1", "group.id": "mysql-kafka_cluster_name-M1"}, "name": "mysql-table_1000'
            '0_test_clean_rt"}',
        ),
    )
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M/connectors/mysql-table_10000_test_clean_rt/move_to/?dest_"
        "cluster=mysql-kafka_cluster_name-M1"
    )
    assert res["result"]
    assert res["message"] == "moved connector mysql-table_10000_test_clean_rt to mysql-kafka_cluster_name-M1"

    mocker.patch(
        "requests.post",
        side_effect=[
            Ret(409, "unable to add"),
            Ret(
                200,
                '{"config": {"tasks.max": "1", "group.id": "mysql-kafka_cluster_name-M"}, "name": "mysql-table_1'
                '0000_test_clean_rt"}',
            ),
        ],
    )
    res = get(
        "/v3/databus/clusters/mysql-kafka_cluster_name-M1/connectors/mysql-table_10000_test_clean_rt/move_to/?dest"
        "_cluster=mysql-kafka_cluster_name-M"
    )
    assert not res["result"]
    assert (
        res["message"]
        == "add connector mysql-table_10000_test_clean_rt in mysql-kafka_cluster_name-M failed! 409 unable to add"
    )
