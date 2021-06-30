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

from datahub.storekit.tests.utils import db_helper


@pytest.fixture
def add_storage_result_table():
    with db_helper.open_cursor("mapleleaf") as cur:

        # 插入测试数据
        db_helper.insert(
            cur,
            "storage_result_table",
            result_table_id="591_anonymous_1217_02",
            storage_cluster_config_id=25,
            physical_table_name="mapleleaf_591.anonymous_1217_02_591",
            expires="7d",
            storage_channel_id=0,
            storage_config='{"indexed_fields": ["field1"]}',
            priority=1,
            created_by="anonymous",
            updated_by="anonymous",
            description="DataFlow增加存储",
            active=1,
            generate_type="user",
        )

        db_helper.insert(
            cur,
            "storage_result_table",
            result_table_id="591_anonymous_1217_02_2",
            storage_cluster_config_id=19,
            physical_table_name="591_anonymous_1217_02_2",
            expires="30d",
            storage_channel_id=0,
            storage_config='{"analyzedFields": ["ip", "report_time"], "dateFields": ["dtEventTime", '
            '"dtEventTimeStamp", "localTime", "thedate"]}',
            priority=2,
            created_by="anonymous",
            updated_by="anonymous",
            description="DataFlow增加存储2",
            active=1,
            generate_type="user",
        )
        yield
        _delete_data()


@pytest.fixture
def delete_storage_result_table():
    # 删除测试数据
    _delete_data()


def _delete_data():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(cur, "DELETE FROM storage_result_table")
