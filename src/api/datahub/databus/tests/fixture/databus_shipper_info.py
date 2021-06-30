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
from datahub.access.tests import db_helper


@pytest.fixture
def test_databus_shipper_info():

    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="hermes-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="druid-kafka_cluster_name-M",
            created_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            updated_by="",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="es-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="hdfs-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="tredis-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="queue-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="mysql-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
        db_helper.insert(
            cur,
            "databus_shipper_info",
            processing_id="10000_test_clean_rt",
            connector_task_name="tsdb-kafka_cluster_name-M",
            created_by="",
            updated_by="",
            created_at="2021-06-01 12:00:00",
            updated_at="2021-06-01 12:00:00",
            description="",
            config="",
        )
    try:
        yield 1000
    finally:
        delete_databus_connector_task()


def delete_databus_connector_task():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_shipper_info
        """,
        )
