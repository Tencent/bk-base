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
def add_queue_channel():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="kafka_cluster_name",
            cluster_type="kafka",
            cluster_role="inner",
            cluster_domain="kafka.domain1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zk.domain1",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=1,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=1000,
        )

        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="kafka_cluster_name2",
            cluster_type="kafka",
            cluster_role="outer",
            cluster_domain="kafka.domain1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zk.domain1",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=1,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=1001,
        )

        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="kafka_cluster_name3",
            cluster_type="kafka",
            cluster_role="queue",
            cluster_domain="kafka.domain1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zookeeper",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=2,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=1002,
        )
    try:
        yield 1
    finally:
        _delete_channel_id()


# delete方法在最后执行，清空数据库中所有数据
def _delete_channel_id():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_channel_cluster_config
        """,
        )
