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

import pytest
from common.api.base import DataResponse  # noqa
from datahub.access.tests.utils import delete, get, post
from datahub.databus.tests.fixture.access_raw_data import test_access_raw_data  # noqa
from datahub.databus.tests.fixture.access_resource_info import (  # noqa
    test_access_resource_info,
)
from datahub.databus.tests.fixture.channel import test_channel_id  # noqa
from datahub.databus.tests.fixture.clean_fixture import add_clean  # noqa
from datahub.databus.tests.fixture.cluster_fixture import add_cluster  # noqa
from datahub.databus.tests.fixture.connector_cluster import (  # noqa
    test_connector_cluster,
    test_connector_cluster1,
)
from datahub.databus.tests.fixture.databus_connector_task import (  # noqa
    test_databus_connector_task,
    test_databus_connector_task_dup,
)
from datahub.databus.tests.fixture.databus_shipper_info import (  # noqa
    test_databus_shipper_info,
)
from datahub.databus.tests.fixture.databus_transform_processing import (  # noqa
    test_databus_transform_processing,
)
from datahub.databus.tests.mock_api import connector_cluster, result_table


class Ret(object):
    status_code = 200
    text = ""

    @staticmethod
    def json():
        return {
            "name": "xxx",
            "config": {},
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "x.x.x.x:10001"}],
            "connector": {"state": "RUNNING", "worker_id": "x.x.x.x:10001"},
        }


result_table_id = "10000_test_clean_rt"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "add_clean",
    "test_access_raw_data",
    "add_cluster",
    "test_databus_shipper_info",
)
@pytest.mark.usefixtures()
def test_task_create_success(mocker):
    """任务提交正常流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.channel.get_topic_partition_num", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)
    mocker.patch("requests.get", return_value=Ret)
    # mocker.patch('databus.task.start_databus_shipper_task', return_value=[])

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id}
    res = post("/v3/databus/tasks/", params)

    compare_json = {
        "code": "1500200",
        "data": "任务(result_table_id:%s)启动成功!" % result_table_id,
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "add_clean",
    "test_access_raw_data",
    "add_cluster",
    "test_databus_shipper_info",
    "test_databus_connector_task_dup",
)
@pytest.mark.usefixtures()
def test_task_create_with_task_exist(mocker):
    """任务提交正常流程"""
    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.channel.get_topic_partition_num", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)
    mocker.patch("requests.get", return_value=Ret)
    # mocker.patch('databus.task.start_databus_shipper_task', return_value=[])

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id}
    res = post("/v3/databus/tasks/", params)

    compare_json = {
        "code": "1500200",
        "data": "任务(result_table_id:%s)启动成功!" % result_table_id,
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "add_clean",
    "test_access_raw_data",
    "add_cluster",
    "test_databus_shipper_info",
)
@pytest.mark.usefixtures()
def test_task_create_start_task_failed(mocker):
    """任务提交失败流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.channel.get_topic_partition_num", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("databus.task.get_rest_url", side_effect="requests.exceptions.RequestException")

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id, "storages": ["kafka", "tsdb"]}
    res = post("/v3/databus/tasks/", params)

    compare_json = {
        "code": "1570042",
        "data": None,
        "errors": None,
        "message": "ResultTable(10000_test_clean_rt)的存储类型()任务创建失败:启动任务失败，"
        "任务名称:clean-table_10000_test_clean_rt，集群:clean-kafka_cluster_name4-M(057)",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "add_clean",
    "test_access_raw_data",
    "add_cluster",
    "test_databus_shipper_info",
)
@pytest.mark.usefixtures()
def test_task_create_storage_not_found(mocker):
    """任务提交失败流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id, "storages": ["opentsdb"]}
    res = post("/v3/databus/tasks/", params)

    compare_json = {
        "code": "1570041",
        "data": None,
        "errors": None,
        "message": "ResultTable(10000_test_clean_rt)未找到存储类型(opentsdb)的配置信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "test_databus_transform_processing",
    "add_clean",
)
@pytest.mark.usefixtures()
def test_task_datanode_success(mocker):
    """任务提交正常流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id}
    res = post("/v3/databus/tasks/datanode/", params)

    compare_json = {
        "code": "1500200",
        "data": "任务(result_table_id：%s)启动成功!" % result_table_id,
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_channel_id", "test_connector_cluster")
@pytest.mark.usefixtures()
def test_task_datanode_failed(mocker):
    """任务提交失败流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"result_table_id": result_table_id}
    res = post("/v3/databus/tasks/datanode/", params)

    compare_json = {
        "code": "1570062",
        "data": None,
        "errors": None,
        "message": "未找到对应的任务配置(databus_transform_process)信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_access_raw_data",
    "test_access_resource_info",
    "test_channel_id",
    "test_connector_cluster",
    "test_databus_transform_processing",
)
@pytest.mark.usefixtures()
def test_task_puller_success(mocker):
    """任务提交正常流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"data_id": 1000}
    res = post("/v3/databus/tasks/puller/", params)

    compare_json = {
        "code": "1500200",
        "data": "任务(data_id：1000)启动成功!",
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures()
def test_task_puller_ard_not_found(mocker):
    """任务提交失败流程，ard代表access_raw_data"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"data_id": 1000}
    res = post("/v3/databus/tasks/puller/", params)

    compare_json = {
        "code": "1570052",
        "data": None,
        "errors": None,
        "message": "未找到data_id(1000)对应的原始数据信息配置(access_raw_data)信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_access_raw_data")
@pytest.mark.usefixtures()
def test_task_puller_tdw_not_found(mocker):
    """任务提交失败流程"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"data_id": 1000}
    res = post("/v3/databus/tasks/puller/", params)

    compare_json = {
        "code": "1570063",
        "data": None,
        "errors": None,
        "message": "未找到data_id(1000)对应的xxx配置(access_resource_info)信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_access_raw_data", "test_access_resource_info")
@pytest.mark.usefixtures()
def test_task_puller_dbc_not_found(mocker):
    """任务提交失败流程，dbc代表databuschannel"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"data_id": 1000}
    res = post("/v3/databus/tasks/puller/", params)

    compare_json = {
        "code": "1570051",
        "data": None,
        "errors": None,
        "message": "未找到kafka集群(1000)对应的集群配置(databus_channel_cluster_config)信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_access_raw_data", "test_access_resource_info", "test_channel_id")
@pytest.mark.usefixtures()
def test_task_puller_dct_failed(mocker):
    """任务提交失败流程，dct代表databus_connector_task"""

    mocker.patch("databus.rt.get_partitions", return_value=1)
    mocker.patch("databus.rt.get_daily_record_count", return_value=100)
    mocker.patch("requests.put", return_value=Ret)
    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"data_id": 1000}
    res = post("/v3/databus/tasks/puller/", params)

    compare_json = {
        "code": "1570060",
        "data": None,
        "errors": None,
        "message": "集群(puller-xxx-M)的url查询失败",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "test_databus_connector_task",
    "test_databus_shipper_info",
    "add_clean",
)
@pytest.mark.usefixtures()
def test_task_destroy_success(mocker):
    """
    任务提交正常流程
    """

    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    res = delete("/v3/databus/tasks/%s/" % result_table_id)

    compare_json = {
        "code": "1500200",
        "data": "result_table_id:%s,任务已成功停止!" % result_table_id,
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "test_databus_connector_task",
    "test_databus_shipper_info",
    "add_clean",
)
@pytest.mark.usefixtures()
def test_task_destroy_success_param(mocker):
    """
    任务提交正常流程
    """

    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"storages": ["hdfs"]}
    res = delete("/v3/databus/tasks/%s/" % result_table_id, params)

    compare_json = {
        "code": "1500200",
        "data": "result_table_id:%s,任务已成功停止!" % result_table_id,
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "test_databus_connector_task",
    "test_databus_shipper_info",
    "add_clean",
)
@pytest.mark.usefixtures()
def test_task_destroy_failed(mocker):
    """
    任务提交正常流程
    """

    mocker.patch("requests.delete", return_value=Ret)

    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {"storages": ["opentsdb"]}

    res = delete("/v3/databus/tasks/%s/" % result_table_id, params)

    compare_json = {
        "code": "1570041",
        "data": None,
        "errors": None,
        "message": "ResultTable(10000_test_clean_rt)未找到存储类型(opentsdb)的配置信息",
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster1")
@pytest.mark.usefixtures()
def test_task_retrieve_success(mocker):
    """
    任务提交正常流程
    """

    res = get("/v3/databus/tasks/%s/?storage=hdfs" % result_table_id)
    res["data"].pop("id")
    res["data"].pop("created_at")
    res["data"].pop("updated_at")

    compare_json = {
        "code": "1500200",
        "data": {
            "cluster_name": "hdfs-kafka_cluster_name-M",
            "connector_task_name": "hdfs-table_10000_test_clean_rt",
            "created_by": "",
            "data_sink": "hdfs#adb",
            "data_source": "kafka#123",
            "description": "",
            "processing_id": "10000_test_clean_rt",
            "sink_type": "hdfs",
            "source_type": "kafka",
            "status": "running",
            "task_type": "puller",
            "updated_by": "",
            "module": "hdfs",
            "component": "kafka_cluster_name",
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster1")
@pytest.mark.usefixtures()
def test_task_retrieve_no_task(mocker):
    """
    任务提交失败流程
    """

    res = get("/v3/databus/tasks/%s/?storage=opentsdb" % result_table_id)

    compare_json = {
        "code": "1500200",
        "data": {},
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster1")
@pytest.mark.usefixtures()
def test_task_retrieve_2_tasks(mocker):
    """
    任务提交失败流程
    """

    res = get("/v3/databus/tasks/%s1/?storage=queue" % result_table_id)

    compare_json = {
        "code": "1500200",
        "data": {},
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster1")
@pytest.mark.usefixtures()
def test_task_retrieve_no_cluster(mocker):
    """
    任务提交失败流程
    """

    res = get("/v3/databus/tasks/%s2/?storage=queue" % result_table_id)

    compare_json = {
        "code": "1500200",
        "data": {},
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task", "test_connector_cluster")
@pytest.mark.usefixtures()
def test_task_component_success(mocker):
    """
    任务提交正常流程
    """

    res = get("/v3/databus/tasks/component/?result_table_id=10000_test_clean_rt&result_table_id=10000_test_clean_rt1")

    compare_json = {
        "code": "1500200",
        "data": {
            "10000_test_clean_rt": {
                "druid": {"component": "kafka_cluster_name", "module": "druid"},
                "es": {"component": "kafka_cluster_name", "module": "es"},
                "hdfs": {"component": "kafka_cluster_name", "module": "hdfs"},
                "hermes": {"component": "kafka_cluster_name", "module": "hermes"},
                "mysql": {"component": "kafka_cluster_name", "module": "mysql"},
                "queue": {"component": "kafka_cluster_name", "module": "queue"},
                "tredis": {"component": "kafka_cluster_name", "module": "tredis"},
                "tsdb": {"component": "kafka_cluster_name", "module": "tsdb"},
            },
            "10000_test_clean_rt1": {"queue": {"component": "kafka_cluster_name", "module": "queue"}},
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.django_db
@pytest.mark.usefixtures("test_databus_connector_task")
def test_list_task():
    """测试任务列表，这个接口主要admin用"""
    res = get("/v3/databus/tasks/")
    assert res["result"]
    assert res["data"]["page_total"] == 12
    assert res["data"]["page"] == 1
    assert len(res["data"]["tasks"]) == 12

    res = get("/v3/databus/tasks/?cluster_name=hdfs-kafka_cluster_name-M")
    assert res["result"]
    assert res["data"]["page_total"] == 1
    assert res["data"]["page"] == 1
    assert len(res["data"]["tasks"]) == 1

    res = get("/v3/databus/tasks/?page=1&page_size=2")
    assert res["result"]
    assert res["data"]["page"] == 1
    assert res["data"]["page_total"] == 12
    assert len(res["data"]["tasks"]) == 2

    res = get("/v3/databus/tasks/?status=running&page=1&page_size=2")
    assert res["result"]
    assert res["data"]["page"] == 1
    assert res["data"]["page_total"] == 12
    assert len(res["data"]["tasks"]) == 2
