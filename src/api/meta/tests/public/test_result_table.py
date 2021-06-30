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
from meta.public.models.result_table import (
    ResultTable,
    ResultTableDel,
    ResultTableField,
)
from tests.utils import UnittestClient

result_table_id = None

create_params = {
    "bk_biz_id": 100,
    "project_id": 1,
    "result_table_id": "100_unittest_table",
    "result_table_name": "unittest_table",
    "result_table_name_alias": "单元测试表",
    "result_table_type": None,
    "processing_type": "clean",
    "generate_type": "user",
    "sensitivity": "public",
    "count_freq": 60,
    "description": "结果表描述",
    "fields": [
        {
            "field_index": 1,
            "field_name": "timestamp",
            "field_alias": "时间戳",
            "description": "时间字段",
            "field_type": "timestamp",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 2,
            "field_name": "field1",
            "field_alias": "字段一",
            "description": "",
            "field_type": "double",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 3,
            "field_name": "field2",
            "field_alias": "字段二",
            "description": "",
            "field_type": "int",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 4,
            "field_name": "field3",
            "field_alias": "字段三",
            "description": "",
            "field_type": "string",
            "is_dimension": 1,
            "origins": "",
        },
    ],
}

update_params = {
    "result_table_name_alias": "单元测试表更新",
    "generate_type": "system",
    "sensitivity": "private",
    "count_freq": 3600,
    "description": "结果表描述更新",
    "fields": [
        {
            "field_index": 1,
            "field_name": "timestamp",
            "field_alias": "时间戳",
            "description": "时间字段",
            "field_type": "timestamp",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 2,
            "field_name": "new_field1",
            "field_alias": "新字段一",
            "description": "",
            "field_type": "double",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 3,
            "field_name": "field3",
            "field_alias": "字段三",
            "description": "",
            "field_type": "string",
            "is_dimension": 1,
            "origins": "",
        },
    ],
}

override_params = {
    "bk_biz_id": 1000,
    "result_table_id": "1000_override_table",
    "result_table_name": "override_table",
    "result_table_name_alias": "单元测试新表",
    "result_table_type": None,
    "generate_type": "user",
    "sensitivity": "public",
    "count_freq": 60,
    "description": "新结果表描述",
    "project_id": 1,
    "fields": [
        {
            "field_index": 1,
            "field_name": "timestamp",
            "field_alias": "时间戳",
            "description": "时间字段",
            "field_type": "timestamp",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 2,
            "field_name": "field1",
            "field_alias": "字段一",
            "description": "",
            "field_type": "double",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 3,
            "field_name": "field2",
            "field_alias": "字段二",
            "description": "",
            "field_type": "int",
            "is_dimension": 0,
            "origins": "",
        },
        {
            "field_index": 4,
            "field_name": "field3",
            "field_alias": "字段三",
            "description": "",
            "field_type": "string",
            "is_dimension": 1,
            "origins": "",
        },
    ],
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_create_result_table():
    global result_table_id
    client = UnittestClient()
    url = reverse("result_table-list")

    # 测试result_table_id是否由bk_biz_id和result_table_name组成
    param1 = copy.deepcopy(create_params)
    param1["result_table_id"] = "10_unittest_table"
    response1 = client.post(url, param1)
    assert response1.is_success() is False
    assert response1.code == pizza_errors.ValidationError().code

    # 测试必需字段缺失
    del_keys = ["result_table_name", "project_id", "bk_biz_id", "description", "result_table_name_alias"]
    param2 = copy.deepcopy(create_params)
    for key in del_keys:
        del param2[key]
    response2 = client.post(url, param2)
    assert response2.is_success() is False
    assert response2.code == pizza_errors.ValidationError().code
    for key in del_keys:
        assert key in response2.errors

    # 测试字段冲突
    param3 = copy.deepcopy(create_params)
    param3["fields"][3]["field_name"] = param3["fields"][2]["field_name"]
    response3 = client.post(url, param3)
    assert response3.is_success() is False
    assert response3.code == meta_errors.ResultTableFieldConflictError().code

    # 测试是否正常创建
    param4 = copy.deepcopy(create_params)
    response4 = client.post(url, param4)
    assert response4.is_success() is True
    assert response4.data == param4["result_table_id"]
    result_table_id = response4.data

    # 检验创建结果
    result_table = ResultTable.objects.get(result_table_id=result_table_id)
    result_table_fields = ResultTableField.objects.filter(result_table_id=result_table_id)
    assert result_table.created_by is not None
    assert result_table.created_at is not None
    assert len(result_table_fields) == len(create_params["fields"])
    for item in result_table_fields:
        assert item.created_by is not None
        assert item.created_at is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_update_result_table():
    global result_table_id
    time.sleep(1)
    client = UnittestClient()

    url = reverse("result_table-detail", [result_table_id])
    params = copy.deepcopy(update_params)
    response = client.put(url, params)
    assert response.is_success() is True
    assert response.data is not None

    result_table = ResultTable.objects.get(result_table_id=result_table_id)
    assert result_table.result_table_name_alias == update_params["result_table_name_alias"]
    assert result_table.sensitivity == update_params["sensitivity"]
    assert result_table.count_freq == update_params["count_freq"]
    assert result_table.description == update_params["description"]
    assert result_table.generate_type == update_params["generate_type"]
    assert result_table.updated_by is not None
    assert result_table.updated_at is not None
    assert result_table.updated_at != result_table.created_at

    result_table_fields = ResultTableField.objects.filter(result_table_id=result_table_id).order_by("field_index")
    assert len(result_table_fields) == len(update_params["fields"])
    for index, field in enumerate(result_table_fields, start=1):
        assert field.field_index == index
        if not field.field_name.startswith("new"):
            assert field.updated_by is not None
            assert field.updated_at is not None
        else:
            assert field.created_by is not None
            assert field.created_at is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_override_result_table():
    global result_table_id
    client = UnittestClient()
    old_table = ResultTable.objects.get(result_table_id=result_table_id)

    url = reverse("result_table-detail", [result_table_id])
    url = url + "override/"
    params = copy.deepcopy(override_params)
    response = client.put(url, params)
    assert response.is_success() is True
    assert response.data is not None
    result_table_id = response.data

    new_table = ResultTable.objects.get(result_table_id=result_table_id)
    new_fields = ResultTableField.objects.filter(result_table_id=result_table_id).order_by("field_index")

    assert new_table.result_table_name == override_params["result_table_name"] != old_table.result_table_name
    assert new_table.generate_type == override_params["generate_type"] != old_table.generate_type
    assert new_table.description == override_params["description"] != old_table.description

    assert len(new_fields) == len(override_params["fields"])
    for index, field in enumerate(new_fields, start=1):
        assert field.field_index == index


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_delete_result_table():
    global result_table_id
    client = UnittestClient()

    if result_table_id is None:
        return
    result_table = ResultTable.objects.get(result_table_id=result_table_id)

    # 测试正常删除
    url1 = reverse("result_table-detail", [result_table_id])
    response1 = client.delete(url1)
    assert response1.is_success() is True
    assert response1.data == result_table_id

    # 测试结果表不存在
    url2 = reverse("result_table-detail", ["0_notexist_table"])
    response2 = client.delete(url2)
    assert response2.is_success() is False
    assert response2.code == meta_errors.ResultTableNotExistError().code

    queryset = ResultTableDel.objects.filter(result_table_id=result_table_id).order_by("-id")
    assert queryset.count() > 0
    result_table_del = queryset[0]
    assert result_table_del.deleted_by is not None
    assert result_table_del.deleted_at is not None
    result_table_content = json.loads(result_table_del.result_table_content)
    assert "fields" in result_table_content
    assert result_table.result_table_name == result_table_content["result_table_name"]
    assert result_table.sensitivity == result_table_content["sensitivity"]
    assert result_table.created_by == result_table_content["created_by"]
    assert result_table.created_at.strftime("%Y-%m-%d %H:%M:%S") == result_table_content["created_at"]
