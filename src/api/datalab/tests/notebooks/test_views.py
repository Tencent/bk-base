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
from datalab.tests.fixture.notebook_task import insert_notebook_task  # noqa
from datalab.tests.fixture.project import insert_project  # noqa
from datalab.tests.mock_api.jupyterhub import (
    mock_create_notebook,
    mock_create_template_notebook,
    mock_create_token,
    mock_create_user,
    mock_pod_metrics,
    mock_retrieve_content_name,
    mock_retrieve_notebook_contents,
    mock_start_server,
)
from datalab.tests.mock_api.meta import mock_meta_transaction
from datalab.tests.mock_api.storekit import (
    mock_create_storage,
    mock_get_hdfs_cluster,
    mock_prepare_storage,
    mock_prepare_storage_failed,
)
from tests.utils import UnittestClient

client = UnittestClient()


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_project")
def test_create_notebook_task():
    """
    测试创建笔记任务
    """
    hp.enable()
    hp.reset()
    mock_create_user(BK_USERNAME)
    mock_start_server(BK_USERNAME)
    mock_create_notebook(BK_USERNAME)
    mock_create_token(BK_USERNAME)

    param = {
        "project_id": 1,
        "project_type": "personal",
        "bk_username": BK_USERNAME,
        "auth_info": '{"bk_ticket": "xx"}',
    }
    res = client.post("/v3/datalab/notebooks/", param)
    assert res.is_success()
    assert res.data["project_id"] == 1
    assert res.data["project_type"] == "personal"
    assert res.data["notebook_name"] == "notebook_1"


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_rename_notebook_task():
    """
    测试重命名笔记任务
    """
    param = {"notebook_name": "notebook_2"}
    res = client.put("/v3/datalab/notebooks/1/", param)
    assert res.is_success()
    assert res.data["notebook_name"] == "notebook_2"


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_get_notebook_task():
    """
    测试获取笔记任务
    """
    res = client.get("/v3/datalab/notebooks/1/")
    assert res.is_success()
    assert res.data["notebook_id"] == 1
    assert res.data["notebook_name"] == "test_notebook"


@pytest.mark.django_db
def test_list_notebook_tasks():
    """
    测试获取笔记任务列表
    """
    hp.enable()
    hp.reset()
    mock_create_user(BK_USERNAME)
    mock_start_server(BK_USERNAME)
    mock_create_template_notebook(BK_USERNAME)
    mock_create_token(BK_USERNAME)

    res = client.get(
        "/v3/datalab/notebooks/", data={"project_id": 1, "bk_username": BK_USERNAME, "auth_info": '{"bk_ticket": "xx"}'}
    )
    assert res.is_success()
    assert res.data[0]["project_type"] == "personal"
    assert res.data[0]["content_name"] == "example.ipynb"


def test_get_library():
    """
    测试获取笔记支持的库
    """
    res = client.get("/v3/datalab/notebooks/library/")
    assert res.is_success()
    assert "Python3" in res.data


def test_get_pod_usage():
    """
    测试获取笔记用量
    """
    hp.enable()
    hp.reset()
    mock_pod_metrics()

    res = client.get("/v3/datalab/notebooks/pod_usage/")
    assert res.is_success()
    assert len(res.data) >= 1
    assert "pod_name" in res.data[0]


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_retrieve_id():
    """
    测试获取笔记任务id
    """
    hp.enable()
    hp.reset()
    mock_retrieve_content_name(BK_USERNAME)

    res = client.get("/v3/datalab/notebooks/retrieve_id/", data={"user": BK_USERNAME, "kernel_id": "id0"})
    assert res.is_success()
    assert res.data == 1


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_get_contents():
    """
    测试获取笔记任务id
    """
    hp.enable()
    hp.reset()
    mock_create_user(BK_USERNAME)
    mock_start_server(BK_USERNAME)
    mock_create_template_notebook(BK_USERNAME)
    mock_create_token(BK_USERNAME)
    mock_retrieve_notebook_contents(BK_USERNAME)

    res = client.get("/v3/datalab/notebooks/1/contents/")
    assert res.is_success()
    assert res.data["contents"]


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_create_result_table_success():
    """
    测试创建结果表
    """
    hp.enable()
    hp.reset()
    mock_get_hdfs_cluster()
    mock_meta_transaction()
    mock_create_storage("591_test1")
    mock_prepare_storage("591_test1")

    res = client.post(
        "/v3/datalab/notebooks/1/cells/1/result_tables/",
        data={
            "result_tables": [
                {
                    "result_table_id": "591_test1",
                    "fields": [
                        {
                            "field_name": "field1",
                            "field_alias": "alias1",
                            "description": "",
                            "field_index": 1,
                            "field_type": "string",
                        }
                    ],
                }
            ],
            "data_processings": [{"inputs": ["591_test"], "outputs": ["591_test1"], "processing_id": "591_test1"}],
            "bkdata_authentication_method": "user",
            "bk_username": BK_USERNAME,
        },
    )
    assert res.is_success()
    assert res.data


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_notebook_task")
def test_create_result_table_failed():
    """
    测试创建结果表
    """
    hp.enable()
    hp.reset()
    mock_get_hdfs_cluster()
    mock_meta_transaction()
    mock_create_storage("591_test1")
    mock_prepare_storage_failed("591_test1")

    res = client.post(
        "/v3/datalab/notebooks/1/cells/1/result_tables/",
        data={
            "result_tables": [
                {
                    "result_table_id": "591_test1",
                    "fields": [
                        {
                            "field_name": "field1",
                            "field_alias": "alias1",
                            "description": "",
                            "field_index": 1,
                            "field_type": "string",
                        }
                    ],
                }
            ],
            "bkdata_authentication_method": "user",
            "bk_username": BK_USERNAME,
        },
    )
    assert not res.is_success()
