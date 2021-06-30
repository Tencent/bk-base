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


import copy
import json
import time

import pytest
from common import exceptions as pizza_errors
from rest_framework.reverse import reverse

from meta import exceptions as meta_errors
from meta.public.models.data_processing import (
    DataProcessing,
    DataProcessingDel,
    DataProcessingRelation,
)
from meta.public.models.result_table import ResultTable
from meta.tests.public.test_result_table import create_params as result_table_params
from tests.utils import UnittestClient

processing_id = "processing1"

create_params = {
    "project_id": 1,
    "processing_id": "processing1",
    "processing_alias": "数据处理1",
    "processing_type": "stream",
    "generate_type": "user",
    "description": "数据处理描述",
    "platform": "bkdata",
    "tags": ["test", "haha"],
    "result_tables": [
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table1", "result_table_name": "processing_table1"},
        ),
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table2", "result_table_name": "processing_table2"},
        ),
    ],
    "inputs": [
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_test",
            "storage_cluster_config_id": None,
            "channel_cluster_config_id": 1,
            "storage_type": "channel",
        }
    ],
    "outputs": [
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table1",
            "storage_cluster_config_id": 2,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table2",
            "storage_cluster_config_id": 3,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
    ],
}

update_params = {
    "project_id": 1,
    "processing_id": "processing1",
    "processing_alias": "数据处理2",
    "generate_type": "system",
    "description": "数据处理描述2",
    "result_tables": [
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table1", "result_table_name": "processing_table1"},
        ),
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table2", "result_table_name": "processing_table2"},
        ),
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table3", "result_table_name": "processing_table3"},
        ),
        dict(
            copy.deepcopy(result_table_params),
            **{"result_table_id": "100_processing_table4", "result_table_name": "processing_table4"},
        ),
    ],
    "inputs": [
        {
            "data_set_type": "raw_data",
            "data_set_id": 1,
            "storage_cluster_config_id": None,
            "channel_cluster_config_id": 1,
            "storage_type": "channel",
        }
    ],
    "outputs": [
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table1",
            "storage_cluster_config_id": 2,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table2",
            "storage_cluster_config_id": 1,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table3",
            "storage_cluster_config_id": 2,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
        {
            "data_set_type": "result_table",
            "data_set_id": "100_processing_table4",
            "storage_cluster_config_id": 3,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        },
    ],
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_create_data_processing():
    global processing_id

    client = UnittestClient()
    url = reverse("data_processing-list")
    # 测试必需字段缺失
    del_keys = ["project_id", "processing_id", "processing_alias", "processing_type", "description"]
    param1 = copy.deepcopy(create_params)
    for key in del_keys:
        del param1[key]
    response1 = client.post(url, param1)
    assert response1.is_success() is False
    assert response1.code == pizza_errors.ValidationError().code
    for key in del_keys:
        assert key in response1.errors

    # 测试结果表冲突
    param2 = copy.deepcopy(create_params)
    param2["result_tables"][1]["result_table_id"] = param2["result_tables"][0]["result_table_id"]
    param2["result_tables"][1]["result_table_name"] = param2["result_tables"][0]["result_table_name"]
    response2 = client.post(url, param2)
    assert response2.is_success() is False

    # 测试正常创建数据处理
    param3 = copy.deepcopy(create_params)
    response3 = client.post(url, param3)
    # print(response3.message)
    assert response3.is_success() is True
    processing_id = response3.data
    data_processing = DataProcessing.objects.get(processing_id=processing_id)
    result_table1 = ResultTable.objects.get(result_table_id=create_params["result_tables"][0]["result_table_id"])
    result_table2 = ResultTable.objects.get(result_table_id=create_params["result_tables"][1]["result_table_id"])
    assert data_processing.created_by is not None
    assert data_processing.created_at is not None
    assert result_table1.created_by is not None
    assert result_table2.created_by is not None
    for input in param3["inputs"]:
        assert (
            DataProcessingRelation.objects.filter(
                processing_id=processing_id,
                data_directing="input",
                data_set_type=input["data_set_type"],
                data_set_id=input["data_set_id"],
            ).count()
            == 1
        )
    for output in param3["outputs"]:
        assert (
            DataProcessingRelation.objects.filter(
                processing_id=processing_id,
                data_directing="output",
                data_set_type=output["data_set_type"],
                data_set_id=output["data_set_id"],
            ).count()
            == 1
        )
    assert (
        len(param3["inputs"])
        == DataProcessingRelation.objects.filter(processing_id=processing_id, data_directing="input").count()
    )
    assert (
        len(param3["outputs"])
        == DataProcessingRelation.objects.filter(processing_id=processing_id, data_directing="output").count()
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_update_data_processing():
    global processing_id
    time.sleep(1)
    client = UnittestClient()

    url = reverse("data_processing-detail", [processing_id])
    params = copy.deepcopy(update_params)
    response = client.put(url, params)
    assert response.is_success() is True
    assert response.data is not None

    data_processing = DataProcessing.objects.get(processing_id=processing_id)
    assert data_processing.processing_alias == update_params["processing_alias"]
    assert data_processing.generate_type == update_params["generate_type"]
    assert data_processing.description == update_params["description"]
    assert data_processing.updated_by is not None
    assert data_processing.updated_at is not None
    assert data_processing.updated_at != data_processing.created_at

    for input in params["inputs"]:
        assert (
            DataProcessingRelation.objects.filter(
                processing_id=processing_id,
                data_directing="input",
                data_set_type=input["data_set_type"],
                data_set_id=input["data_set_id"],
            ).count()
            == 1
        )
    for output in params["outputs"]:
        assert (
            DataProcessingRelation.objects.filter(
                processing_id=processing_id,
                data_directing="output",
                data_set_type=output["data_set_type"],
                data_set_id=output["data_set_id"],
            ).count()
            == 1
        )
    assert (
        len(params["inputs"])
        == DataProcessingRelation.objects.filter(processing_id=processing_id, data_directing="input").count()
    )
    assert (
        len(params["outputs"])
        == DataProcessingRelation.objects.filter(processing_id=processing_id, data_directing="output").count()
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_delete_data_processing():
    global processing_id
    data_processing = DataProcessing.objects.get(processing_id=processing_id)
    client = UnittestClient()
    url = reverse("data_processing-detail", [processing_id])

    response1 = client.delete(url, {"with_data": True})
    assert response1.is_success() is True
    assert response1.data == processing_id

    response2 = client.delete(url, {"with_data": True})
    assert response2.is_success() is False
    assert response2.code == meta_errors.DataProcessingNotExistError().code

    queryset = DataProcessingDel.objects.filter(processing_id=processing_id).order_by("-id")
    assert queryset.count() > 0
    data_processing_del = queryset[0]
    assert data_processing_del.deleted_by is not None
    assert data_processing_del.deleted_at is not None
    data_processing_content = json.loads(data_processing_del.processing_content)
    assert "inputs" in data_processing_content
    assert "outputs" in data_processing_content
    assert data_processing.project_id == data_processing_content["project_id"]
    assert data_processing.processing_alias == data_processing_content["processing_alias"]
    assert data_processing.processing_type == data_processing_content["processing_type"]
    assert data_processing.generate_type == data_processing_content["generate_type"]
    assert data_processing.created_by == data_processing_content["created_by"]
    assert data_processing.created_at.strftime("%Y-%m-%d %H:%M:%S") == data_processing_content["created_at"]


# @pytest.mark.django_db(transaction=True)
# @pytest.mark.usefixtures('patch_meta_sync', 'patch_auth_check', 'init_tag_data')
# def test_bulk_delete_data_processings():
#     pass
