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
def test_puller_jdbc_config():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id, bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","db","bkdata",'
            '11,"admin","admin","","admin" '
            ");",
        )
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            "values "
            '(12, 123, \'{"db_host": "x.x.x.x", '
            '"db_port": 10000, "db_user": "user", "db_pass": "XXX",'
            ' "db_name": "bkdata_basic", "table_name": "access_db_info",'
            ' "db_type_id": 1}\','
            ' "time", 1, "create_at" ,1 ,"yyyy-MM-dd HH:mm:ss", 1, "[]", "" '
            ");",
        )
        # 首先添加一条inner channel记录，用来指向kafka
        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="test_kafka",
            cluster_type="kafka",
            cluster_role="outer",
            cluster_domain="127.0.0.1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zookeeper",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=5,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=11,
        )
    try:
        yield
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info")
            db_helper.execute(cur, "DELETE FROM access_raw_data")
            db_helper.execute(cur, "DELETE FROM databus_channel_cluster_config")
            db_helper.execute(cur, "DELETE FROM databus_connector_task")
