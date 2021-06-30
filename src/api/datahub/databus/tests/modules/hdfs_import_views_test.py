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
from datahub.access.tests.utils import get, post
from datahub.databus.models import DatabusHdfsImportTask


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_success(mocker):
    """业务ID不为数字"""
    params = {
        "result_table_id": "103_xxx_test",
        "data_dir": "test_data_dir3",
        "description": "test",
    }

    mocker.patch(
        "datahub.databus.rt.get_databus_rt_info",
        return_value={
            "result_table_id": "103_xxx_test",
            "hdfs.data_type": "hdfs",
            "channel_name": "test_channel",
            "channel_type": "kafka",
            "geog_area": "inland",
            "hdfs": {
                "connection_info": '{"flush_size": 1000000, "servicerpc_port": 53310, "hdfs_url": "hdfs://hdfsTest", '
                '"rpc_port": 9000, "interval": 60000, "ids": "nn1", "hdfs_cluster_name": "hdfsTest",'
                ' "topic_dir": "/kafka/data/", "log_dir": "/kafka/logs", "hdfs_default_params": '
                '{"dfs.replication": 2}, "hosts": "test-master-01", "port": 8081}'
            },
            "bootstrap.servers": "test_kafka_bs",
        },
    )

    res = post("/v3/databus/import_hdfs/", params)
    assert res["result"]
    assert res["data"]["result_table_id"] == "103_xxx_test"
    assert res["data"]["data_dir"] == "test_data_dir3"
    assert res["data"]["finished"] == 0
    assert res["data"]["status"] == ""
    assert res["data"]["kafka_bs"] == "test_kafka_bs"
    assert res["data"]["hdfs_conf_dir"] == "hdfsTest"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_no_rt(mocker):
    """业务ID不为数字"""
    # mock_startShipperTask.return_val = True
    # result_table.get_ok()

    params = {
        "result_table_id": "103_xxx_test",
        "data_dir": "test_data_dir3",
        "description": "test",
    }

    mocker.patch("datahub.databus.rt.get_databus_rt_info", return_value=None)

    res = post("/v3/databus/import_hdfs/", params)

    assert not res["result"]
    assert res["data"] is None
    assert res["message"] == u"离线导入任务创建失败，result_table_id:103_xxx_test 对应的结果表不存在！ "
    assert res["code"] == "1570008"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_integrityerror(mocker):
    params = {
        "result_table_id": "103_xxx_test",
        "data_dir": "test_data_dir3",
        "description": None,
    }

    mocker.patch(
        "datahub.databus.rt.get_databus_rt_info",
        return_value={
            "result_table_id": "103_xxx_test",
            "hdfs": {
                "connection_info": '{"flush_size": 1000000, "servicerpc_port": 53310, "hdfs_url": "hdfs://hdfsTest"'
                ', "rpc_port": 9000, "interval": 60000, "ids": "nn1", "hdfs_cluster_name": '
                '"hdfsTest", "topic_dir": "/kafka/data/", "log_dir": "/kafka/logs", '
                '"hdfs_default_params": {"dfs.replication": 2}, "hosts": "test-master-01", '
                '"port": 8081}'
            },
            "bootstrap.servers": "test_kafka_bs",
        },
    )

    res = post("/v3/databus/import_hdfs/", params)
    assert res["result"] is False
    assert res["data"] is None
    assert res["message"] == u"离线导入任务创建失败！"
    assert res["code"] == "1570008"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_retrieve_success():
    res = get("/v3/databus/import_hdfs/1/")

    assert res["result"]
    assert res["data"]["status"] == "t"
    assert res["data"]["data_dir"] == "test_data_dir"
    assert res["data"]["result_table_id"] == "101_xxx_test"
    assert res["data"]["finished"] == 0
    assert res["data"]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"]["kafka_bs"] == "test_kafka_bs"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_retrieve_failed():
    res = get("/v3/databus/import_hdfs/1/")

    assert not res["result"]
    assert res["message"] == u"查询离线导入任务失败,任务Id:1"
    assert res["code"] == "1570004"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_udpate_task_finished():
    params = {
        "result_table_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finished": "1",
        "status": "t",
    }
    res = post("/v3/databus/import_hdfs/1/update/", params)
    assert res["result"]
    assert res["data"]["status"] == "t"
    assert res["data"]["data_dir"] == "test_data_dir"
    assert res["data"]["result_table_id"] == "101_xxx_test"
    assert res["data"]["finished"] == 1
    assert res["data"]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"]["kafka_bs"] == "test_kafka_bs"
    assert res["data"]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_udpate_task_not_finish():
    params = {
        "result_table_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finished": "0",
        "status": "t",
    }
    res = post("/v3/databus/import_hdfs/1/update/", params)
    assert res["result"]
    assert res["data"]["status"] == "t"
    assert res["data"]["data_dir"] == "test_data_dir"
    assert res["data"]["result_table_id"] == "101_xxx_test"
    assert res["data"]["finished"] == 0
    assert res["data"]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"]["kafka_bs"] == "test_kafka_bs"
    assert res["data"]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_udpate_task_failed(mocker):
    params = {
        "result_table_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finished": "0",
        "status": "t",
    }
    mocker.patch(
        "datahub.databus.models.DatabusHdfsImportTask.objects.get",
        side_effect=DatabusHdfsImportTask.DoesNotExist,
    )

    res = post("/v3/databus/import_hdfs/1/update/", params)

    assert not res["result"]
    assert res["data"] is None
    assert res["message"] == u"更新离线导入任务失败,任务Id:1, result_table_id:101_xxx_test, data_dir:test_data_dir, " u"finished:0"
    assert res["code"] == "1570007"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_update_hdfs_import_finished():
    params = {
        "id": 1,
        "rt_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finish": "true",
        "status_update": "t",
    }
    res = post("/v3/databus/import_hdfs/update_hdfs_import_task/", params)
    assert res["result"]
    assert res["data"]["status"] == "t"
    assert res["data"]["data_dir"] == "test_data_dir"
    assert res["data"]["result_table_id"] == "101_xxx_test"
    assert res["data"]["finished"] == 1
    assert res["data"]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"]["kafka_bs"] == "test_kafka_bs"
    assert res["data"]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_import_task_not_finish():
    params = {
        "id": 1,
        "rt_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finish": "false",
        "status_update": "t",
    }
    res = post("/v3/databus/import_hdfs/update_hdfs_import_task/", params)
    assert res["result"]
    assert res["data"]["status"] == "t"
    assert res["data"]["data_dir"] == "test_data_dir"
    assert res["data"]["result_table_id"] == "101_xxx_test"
    assert res["data"]["finished"] == 0
    assert res["data"]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"]["kafka_bs"] == "test_kafka_bs"
    assert res["data"]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_update_hdfs_import_task_failed(mocker):
    params = {
        "id": 1,
        "rt_id": "101_xxx_test",
        "data_dir": "test_data_dir",
        "finish": "false",
        "status_update": "t",
    }

    mocker.patch(
        "datahub.databus.models.DatabusHdfsImportTask.objects.get",
        side_effect=DatabusHdfsImportTask.DoesNotExist,
    )

    res = post("/v3/databus/import_hdfs/update_hdfs_import_task/", params)
    assert not res["result"]
    assert res["data"] is None
    assert (
        res["message"] == u"更新离线导入任务失败,任务Id:1, result_table_id:101_xxx_test, data_dir:test_data_dir, " u"finished:false"
    )
    assert res["code"] == "1570007"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_get_earliest_success():
    res = get("/v3/databus/import_hdfs/get_earliest/?limit=1")
    assert res["result"]
    assert res["data"][0]["status"] == "t"
    assert res["data"][0]["data_dir"] == "test_data_dir"
    assert res["data"][0]["result_table_id"] == "101_xxx_test"
    assert res["data"][0]["finished"] == 0
    assert res["data"][0]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"][0]["kafka_bs"] == "test_kafka_bs"
    assert res["data"][0]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_get_earliest_noparam():
    res = get("/v3/databus/import_hdfs/get_earliest/")
    assert res["result"]
    assert res["data"][0]["status"] == "t"
    assert res["data"][0]["data_dir"] == "test_data_dir"
    assert res["data"][0]["result_table_id"] == "101_xxx_test"
    assert res["data"][0]["finished"] == 0
    assert res["data"][0]["hdfs_conf_dir"] == "test_hdfs_conf_dir"
    assert res["data"][0]["kafka_bs"] == "test_kafka_bs"
    assert res["data"][0]["hdfs_custom_property"] == ""


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add")
def test_get_earliest_failed(mocker):

    mocker.patch(
        "datahub.databus.models.DatabusHdfsImportTask.objects.filter",
        side_effect=Exception,
    )

    res = get("/v3/databus/import_hdfs/get_earliest/")
    assert not res["result"]
    assert res["data"] is None
    assert res["message"] == u"查询最近离线导入任务列表失败！"
    assert res["code"] == "1570006"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add_finished")
def test_clean_history_success():
    params = {"days": 1}

    res = post("/v3/databus/import_hdfs/clean_history/", params)
    assert res["result"]
    assert res["data"]["days"] == 1
    assert res["data"]["del_count"] == 1


@pytest.mark.httpretty
@pytest.mark.django_db
def test_clean_history_no_data():
    params = {"days": 1}
    res = post("/v3/databus/import_hdfs/clean_history/", params)
    assert res["result"]
    assert res["data"]["days"] == 1
    assert res["data"]["del_count"] == 0


@pytest.mark.httpretty
@pytest.mark.django_db
def test_clean_history_failed(mocker):
    params = {"days": 1}

    mocker.patch(
        "datahub.databus.models.DatabusHdfsImportTask.objects.filter",
        side_effect=Exception,
    )

    res = post("/v3/databus/import_hdfs/clean_history/", params)

    assert not res["result"]
    assert res["code"] == "1570005"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add_multi")
def test_check_tasks_success():

    res = get("/v3/databus/import_hdfs/check/?interval_min=60")
    assert res["result"]
    assert res["data"]["BAD_DATA_FILE"] == 1
    assert res["data"]["EMPTY_DATA_DIR"] == 1
    assert res["data"]["FILE_TOO_BIG"] == 1
    assert res["data"]["UNFINISHED_TASKS"] == 2


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("task_add_multi")
def test_check_tasks_failed(mocker):

    mocker.patch("datahub.databus.import_hdfs.check_tasks", side_effect=Exception)

    res = get("/v3/databus/import_hdfs/check/?interval_min=60")
    assert res["result"] is False
    assert res["data"] is None
    assert res["message"] == u"离线任务状态检查失败！"
    assert res["code"] == "1570020"
