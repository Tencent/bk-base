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
from meta.public.models.data_transferring import (
    DataTransferring,
    DataTransferringDel,
    DataTransferringRelation,
)
from tests.utils import UnittestClient

transferring_id = "transferring1"

create_params = {
    "project_id": 1,
    "project_name": "测试项目",
    "transferring_id": "xxx",
    "transferring_alias": "xxx",
    "transferring_type": "shipper",
    "generate_type": "user",
    "created_by ": "xxx",
    "created_at": "xxx",
    "updated_by": "xxx",
    "updated_at": "xxx",
    "description": "xxx",
    "tags": ["test", "haha"],
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
    ],
}


update_params = {
    "bk_username": "admin",
    "transferring_alias": "xxx",
    "transferring_type": "shipper",
    "description": "xxx",
    "generate_type": "system",
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
            "storage_cluster_config_id": 3,
            "channel_cluster_config_id": None,
            "storage_type": "storage",
        }
    ],
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_create_data_transferring():
    global transferring_id

    client = UnittestClient()
    url = reverse("data_transferring-list")
    # 测试必需字段缺失
    del_keys = ["project_id", "transferring_id", "transferring_alias", "transferring_type", "description"]
    param1 = copy.deepcopy(create_params)
    for key in del_keys:
        del param1[key]
    response1 = client.post(url, param1)
    assert response1.is_success() is False
    assert response1.code == pizza_errors.ValidationError().code
    for key in del_keys:
        assert key in response1.errors

    # 测试正常创建数据传输
    param3 = copy.deepcopy(create_params)
    response3 = client.post(url, param3)
    # print(response3.message)
    assert response3.is_success() is True
    transferring_id = response3.data
    data_transferring = DataTransferring.objects.get(transferring_id=transferring_id)
    assert data_transferring.created_by is not None
    assert data_transferring.created_at is not None

    for input_item in param3["inputs"]:
        assert (
            DataTransferringRelation.objects.filter(
                transferring_id=transferring_id,
                data_directing="input",
                data_set_type=input_item["data_set_type"],
                data_set_id=input_item["data_set_id"],
            ).count()
            == 1
        )
    for output_item in param3["outputs"]:
        assert (
            DataTransferringRelation.objects.filter(
                transferring_id=transferring_id,
                data_directing="output",
                data_set_type=output_item["data_set_type"],
                data_set_id=output_item["data_set_id"],
            ).count()
            == 1
        )
    assert (
        len(param3["inputs"])
        == DataTransferringRelation.objects.filter(transferring_id=transferring_id, data_directing="input").count()
    )
    assert (
        len(param3["outputs"])
        == DataTransferringRelation.objects.filter(transferring_id=transferring_id, data_directing="output").count()
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_update_data_transferring():
    global transferring_id
    time.sleep(1)
    client = UnittestClient()

    url = reverse("data_transferring-detail", [transferring_id])
    params = copy.deepcopy(update_params)
    response = client.put(url, params)
    assert response.is_success() is True
    assert response.data is not None

    data_transferring = DataTransferring.objects.get(transferring_id=transferring_id)
    assert data_transferring.transferring_alias == update_params["transferring_alias"]
    assert data_transferring.generate_type == update_params["generate_type"]
    assert data_transferring.description == update_params["description"]
    assert data_transferring.updated_by is not None
    assert data_transferring.updated_at is not None
    assert data_transferring.updated_at != data_transferring.created_at

    for input_item in params["inputs"]:
        assert (
            DataTransferringRelation.objects.filter(
                transferring_id=transferring_id,
                data_directing="input",
                data_set_type=input_item["data_set_type"],
                data_set_id=input_item["data_set_id"],
            ).count()
            == 1
        )
    for output_item in params["outputs"]:
        assert (
            DataTransferringRelation.objects.filter(
                transferring_id=transferring_id,
                data_directing="output",
                data_set_type=output_item["data_set_type"],
                data_set_id=output_item["data_set_id"],
            ).count()
            == 1
        )

    assert (
        len(params["inputs"])
        == DataTransferringRelation.objects.filter(transferring_id=transferring_id, data_directing="input").count()
    )
    assert (
        len(params["outputs"])
        == DataTransferringRelation.objects.filter(transferring_id=transferring_id, data_directing="output").count()
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_delete_data_transferring():
    global transferring_id
    client = UnittestClient()
    transferring = DataTransferring.objects.get(transferring_id=transferring_id)
    url = reverse("data_transferring-detail", [transferring_id])

    response1 = client.delete(url, {"with_data": True})
    assert response1.is_success() is True
    assert response1.data == transferring_id

    response2 = client.delete(url, {"with_data": True})
    assert response2.is_success() is False
    assert response2.code == meta_errors.DataTransferringNotExistError().code

    queryset = DataTransferringDel.objects.filter(transferring_id=transferring_id).order_by("-id")
    assert queryset.count() > 0
    data_transferring_del = queryset[0]
    assert data_transferring_del.deleted_by is not None
    assert data_transferring_del.deleted_at is not None
    data_transferring_content = json.loads(data_transferring_del.transferring_content)
    assert "inputs" in data_transferring_content
    assert "outputs" in data_transferring_content
    assert transferring.project_id == data_transferring_content["project_id"]
    assert transferring.transferring_alias == data_transferring_content["transferring_alias"]
    assert transferring.transferring_type == data_transferring_content["transferring_type"]
    assert transferring.generate_type == data_transferring_content["generate_type"]
    assert transferring.created_by == data_transferring_content["created_by"]
    assert transferring.created_at.strftime("%Y-%m-%d %H:%M:%S") == data_transferring_content["created_at"]
