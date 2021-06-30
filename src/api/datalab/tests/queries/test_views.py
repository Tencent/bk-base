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
from datalab.tests.constants import BK_USERNAME
from datalab.tests.fixture.query_task import insert_bksql_function  # noqa
from datalab.tests.fixture.query_task import insert_query_history  # noqa
from datalab.tests.fixture.query_task import insert_query_task_info  # noqa
from datalab.tests.mock_api.queryengine import (
    mock_query_async_failed,
    mock_query_async_success,
    mock_query_info,
    mock_query_result,
    mock_query_stage,
    mock_sqltype_and_result_tables,
)
from tests.utils import UnittestClient

client = UnittestClient()


@pytest.mark.django_db
def test_create_query_task():
    """
    测试创建查询任务
    """
    param = {"project_id": 1, "project_type": "personal"}
    res = client.post("/v3/datalab/queries/", param)
    assert res.is_success()
    assert res.data["project_id"] == 1
    assert res.data["project_type"] == "personal"


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_retrieve_query_task_success():
    """
    测试成功获取查询任务
    """
    res = client.get("/v3/datalab/queries/1/")
    assert res.is_success()
    assert res.data["query_name"] == "query_1"
    assert res.data["sql_text"] == "select * from 591_x"


@pytest.mark.django_db
def test_retrieve_query_task_failed():
    """
    测试获取查询任务失败
    """
    res = client.get("/v3/datalab/queries/1/")
    assert not res.is_success()
    assert res.code == "1580013"


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_sql_query_success():
    """
    测试sql查询
    """
    hp.enable()
    hp.reset()
    mock_sqltype_and_result_tables()
    mock_query_async_success()

    sql = "select * from 591_xx"
    param = {
        "sql": sql,
        "bk_username": BK_USERNAME,
    }
    res = client.put("/v3/datalab/queries/1/", param)
    assert res.is_success()
    assert res.data["query_task_id"] == "bk123"
    assert res.data["sql_text"] == sql


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_sql_query_failed():
    """
    测试sql查询
    """
    hp.enable()
    hp.reset()
    mock_sqltype_and_result_tables()
    mock_query_async_failed()

    sql = "select * from 591_xx"
    param = {
        "sql": sql,
        "bk_username": BK_USERNAME,
    }
    res = client.put("/v3/datalab/queries/1/", param)
    assert not res.is_success()
    assert res.message == "error_message"


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_delete_query_task():
    """
    测试删除查询任务
    """
    res = client.delete("/v3/datalab/queries/1/")
    assert res.is_success()


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_history")
def test_get_query_history():
    """
    测试查看查询历史
    """
    hp.enable()
    hp.reset()
    mock_query_info()

    res = client.get("/v3/datalab/queries/1/history/", data={"page": 1, "page_size": 10})
    assert res.is_success()
    assert res.data["results"] == [{"sql_text": "xxx"}]


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_get_query_result():
    """
    测试获取查询结果集
    """
    hp.enable()
    hp.reset()
    mock_query_result()

    res = client.get("/v3/datalab/queries/1/result/")
    assert res.is_success()
    assert res.data["totalRecords"] == 1


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_get_query_stage():
    """
    测试获取查询阶段
    """
    hp.enable()
    hp.reset()
    mock_query_stage()

    res = client.get("/v3/datalab/queries/1/stage/")
    assert res.is_success()
    assert res.data["status"] == "finished"
    assert len(res.data["stages"]) == 4


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_update_query_task_chart_config():
    """
    测试更新查询配置
    """
    chart_config = '{"chart_xx": "config_xx"}'
    param = {
        "chart_config": chart_config,
    }
    res = client.put("/v3/datalab/queries/1/", param)
    assert res.is_success()
    assert res.data["chart_config"] == chart_config


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_update_query_task_query_name():
    """
    测试更新查询任务名
    """
    query_name = "query_test"
    param = {
        "query_name": query_name,
    }
    res = client.put("/v3/datalab/queries/1/", param)
    assert res.is_success()
    assert res.data["query_name"] == query_name


@pytest.mark.django_db
def test_list_query_tasks_failed():
    """
    测试获取查询任务列表失败
    """
    res = client.get("/v3/datalab/queries/")
    assert not res.is_success()


@pytest.mark.django_db
def test_list_query_tasks_success():
    """
    测试获取查询任务列表成功
    """
    res = client.get("/v3/datalab/queries/", data={"project_id": 1})
    assert res.is_success()
    assert res.data[0]["project_type"] == "personal"


@pytest.mark.django_db
def test_retrieve_available_storage():
    """
    测试获取可用存储
    """
    res = client.get("/v3/datalab/queries/available_storage/")
    assert res.is_success()
    assert len(res.data) > 3
    assert "hdfs" in res.data


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_query_task_info")
def test_download():
    """
    测试数据下载
    """
    res = client.get("/v3/datalab/queries/1/download/")
    assert res.is_success()
    assert "queryengine/dataset/download" in res.data


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_bksql_function")
def test_retrieve_function():
    """
    测试获取函数
    """
    res = client.get("/v3/datalab/queries/function/")
    assert res.is_success()
    assert len(res.data) == 4
    assert res.data[0]["func_group"] == "string-functions"
