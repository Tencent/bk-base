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
def add_storage_cluster_config():
    with db_helper.open_cursor("mapleleaf") as cur:

        # 插入测试数据
        db_helper.insert(
            cur,
            "storage_cluster_config",
            id=25,
            cluster_type="mysql",
            cluster_name="mysql-test",
            cluster_group="default",
            connection_info='{"host": "mysql.host", "password": "mysql.password", "port": 25000, "user": "mysql.user"}',
            expires='{"min_expire": 1, "max_expire": 1, "list_expire": [{"name": "1天", "value": "1d"}]}',
            priority=0,
            version="1.0.0",
            belongs_to="anonymous",
            created_by="anonymous",
            updated_by="anonymous",
            description="MySQL测试集群",
        )

        db_helper.insert(
            cur,
            "storage_cluster_config",
            id=19,
            cluster_type="es",
            cluster_name="es-test",
            cluster_group="default",
            connection_info='{"enable_auth": true, "host": "es.host", "user": "user", "password": "es.passwd", '
            '"port": 8080, "transport": 9300}',
            expires='{"min_expire": 1, "max_expire": 1, "list_expire": [{"name": "1天", "value": "1d"}]}',
            priority=1,
            version="1.0.1",
            belongs_to="anonymous2",
            created_by="anonymous2",
            updated_by="anonymous2",
            description="ES测试集群",
        )

        db_helper.insert(
            cur,
            "storage_cluster_config",
            id=32,
            cluster_type="druid",
            cluster_name="druid-test",
            cluster_group="default",
            connection_info='{"coordinator_port": 8081, "zookeeper.connect": "zk_ip:2181", "overlord_host": '
            '"overlord_ip", "host": "broker_ip", "overlord_port": 8090, "port": 8082, '
            '"coordinator_host": "coordinator_ip"}',
            expires='{"min_expire": 1, "max_expire": 1, "list_expire": [{"name": "1天", "value": "1d"}]}',
            priority=2,
            version="1.0.2",
            belongs_to="anonymous3",
            created_by="anonymous3",
            updated_by="anonymous3",
            description="Druid测试集群",
        )
        # 通过yield实现单元测试中的setup与teardown
        yield
        _delete_data()


@pytest.fixture
def delete_storage_cluster_config():
    # 删除测试数据
    _delete_data()


def _delete_data():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(cur, "DELETE FROM storage_cluster_config")
