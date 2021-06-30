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
def add_cluster():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="clean-testouter-M",
            cluster_rest_port=10100,
            cluster_bootstrap_servers="xx.xx.xx:9092",
            consumer_bootstrap_servers="x.x.x.x:9092",
            consumer_props='{"consumer.conf": 5}',
            state="RUNNING",
            cluster_props='{"cluster.conf": 111}',
            cluster_rest_domain="x.x.x.x",
            limit_per_day=1440000,
            priority=10,
            description="",
            created_by="",
            updated_by="",
            id=1000,
        )

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="clean-kafka_cluster_name4-M",
            cluster_rest_port=10100,
            cluster_bootstrap_servers="xx.xx.xx:9092",
            consumer_bootstrap_servers="",
            consumer_props="21221",
            state="RUNNING",
            cluster_props="dddd",
            cluster_rest_domain="x.x.x.x",
            limit_per_day=1440000,
            priority=3,
            description="",
            created_by="",
            updated_by="",
            id=1003,
        )

    try:
        yield 1
    finally:
        _delete_cluster_info()


def _delete_cluster_info():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_connector_cluster_config
        """,
        )
