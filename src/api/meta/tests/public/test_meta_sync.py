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
from tests.utils import UnittestClient

changed_data = {
    "bk_biz_id": "591",
    "created_at": "2019-05-16 10:57:33",
    "result_table_name_alias": "test",
    "processing_type": "batch",
    "count_freq": "1",
    "updated_by": None,
    "platform": "bkdata",
    "sensitivity": "private",
    "created_by": "admin",
    "count_freq_unit": "H",
    "result_table_type": "",
    "result_table_name": "test_item_1",
    "generate_type": "user",
    "data_category": "",
    "is_managed": "1",
    "project_id": "12497",
    "result_table_id": "591_test_item_14",
    "description": "admin",
}

sync_data = """
{
    "affect_original": true,
    "content_mode": "id",
    "db_operations_list": [{
        "changed_data": "",
        "change_time": "2021-05-18 01:26:47",
        "primary_key_value": "591_test_item_14",
        "db_name": "bkdata_basic",
        "table_name": "result_table",
        "method": "__METHOD__"
    }],
    "bk_username": "admin",
    "batch": false,
    "affect_backup_only": false
}
"""
pk_id = "591_test_item_14"


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_sync_data_create():

    client = UnittestClient()

    # 创建
    url1 = "/v3/meta/meta_sync/"
    post_data = json.loads(sync_data.replace("__METHOD__", "CREATE"))
    post_data["db_operations_list"][0]["changed_data"] = json.dumps(changed_data)
    sync_fail = False
    try:
        client.post(url1, data=post_data)
    except Exception as e:
        sync_fail = True
        print("error: {}".format(e))
    assert sync_fail is False


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_sync_data_update():

    client = UnittestClient()

    # 创建
    url1 = "/v3/meta/meta_sync/"
    post_data = json.loads(sync_data.replace("__METHOD__", "UPDATE"))
    post_data["db_operations_list"][0]["changed_data"] = json.dumps(changed_data)
    sync_fail = False
    try:
        client.post(url1, data=post_data)
    except Exception as e:
        sync_fail = True
        print("error: {}".format(e))
    assert sync_fail is False


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_sync_data_delete():

    client = UnittestClient()

    # 创建
    url1 = "/v3/meta/meta_sync/"
    post_data = json.loads(sync_data.replace("__METHOD__", "DELETE"))
    post_data["db_operations_list"][0]["changed_data"] = json.dumps(changed_data)
    sync_fail = False
    try:
        client.post(url1, data=post_data)
    except Exception as e:
        sync_fail = True
        print("error: {}".format(e))
    assert sync_fail is False
