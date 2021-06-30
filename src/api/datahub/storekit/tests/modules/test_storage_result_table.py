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

import datahub.storekit.tests.utils.conftest as conftest
from common.api.base import DataResponse
from datahub.storekit.tests.fixture.storage_cluster_config_fixture import (
    add_storage_cluster_config,
    delete_storage_cluster_config,
)
from datahub.storekit.tests.fixture.storage_result_table_fixture import (
    add_storage_result_table,
    delete_storage_result_table,
)
from datahub.storekit.tests.http_utils import delete, get, post, put
from datahub.storekit.tests.mocker.mocker_result_table import result_table
from datahub.storekit.tests.utils.constants import STORAGE_RESULT_TABLE_URL
from datahub.storekit.tests.utils.helper import (
    test_get_schema_and_sql_compare_dict,
    test_get_schema_and_sql_compare_dict_all,
)


@pytest.mark.django_db
@pytest.mark.usefixtures(
    "patch_meta_sync", "delete_storage_cluster_config", "add_storage_cluster_config", "delete_storage_result_table"
)
def test_create_success_first():
    """
    测试创建storage_result_table成功, 第一次创建rt与指定存储的关联关系
    """
    hp.enable()

    conftest.mock_sycn_meta()
    conftest.mock_rt_tag_code()
    conftest.mock_meta_tag_target()
    conftest.mock_disable_tag_cluster()
    create_params = {
        "result_table_id": "591_anonymous_1217_02",
        "cluster_name": "mysql-test",
        "cluster_type": "mysql",
        "expires": "7d",
        "storage_config": '{"indexed_fields": ["field1"]}',
    }

    res = post("{}{}/".format(STORAGE_RESULT_TABLE_URL, create_params["result_table_id"]), create_params)

    compare_dict = {
        "code": "1500200",
        "data": {
            "active": 1,
            "cluster_name": "mysql-test",
            "cluster_type": "mysql",
            "config": {"indexed_fields": ["field1"]},
            "created_by": "",
            "description": "",
            "expires": "7d",
            "generate_type": "user",
            "physical_table_name": "mapleleaf_591.anonymous_1217_02_591",
            "priority": 0,
            "result_table_id": "591_anonymous_1217_02",
            "storage_channel_id": 0,
            "storage_config": '{"indexed_fields": ["field1"]}',
            "updated_by": "",
        },
        "errors": None,
        "message": "ok",
        "result": True,
    }
    assert res.is_success() is True
    del res.response["data"]["created_at"]
    del res.response["data"]["updated_at"]
    assert res.response == compare_dict
