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

from ...tests import db_helper


@pytest.fixture(scope="session")
def django_db_setup():
    """Avoid creating/setting up the test database"""
    pass


@pytest.fixture()
def init_access_time_db_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description )"
            'values (11, 123, \'{"db_host": "x.x.x.x", '
            '"db_port": 10000, "db_user": "user", "db_pass": "XXX",'
            ' "db_name": "bkdata_basic", "table_name": "access_db_info",'
            ' "db_type_id": 1}\','
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "{}", ""'
            ");",
        )
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (12, 123, \'{"db_host": "x.x.x.x", '
            '"db_port": 10000, "db_user": "user", "db_pass": "XXX",'
            ' "db_name": "bkdata_basic", "table_name": "access_db_info",'
            ' "db_type_id": 1}\','
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "{}", "" '
            ");",
        )
        db_helper.insert(
            cur,
            "access_raw_data",
            id=123,
            bk_biz_id=591,
            raw_data_name="raw_data_test",
            raw_data_alias="",
            sensitivity="",
            data_source="",
            data_encoding="gbk",
            data_category="",
            data_scenario="tdw",
            bk_app_code="",
            storage_channel_id=1002,
            maintainer="",
            description="",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id in (11,12)")
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id in (123)")


@pytest.fixture()
def init_access_manager_config():
    type_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_manager_config "
            "(names, type, active, created_by, description) "
            "VALUES "
            '("admin","job",1,"admin",""),'
            '("admin","http",1,"admin",""),'
            '("591","bk",1,"admin","");',
        )
    try:
        yield type_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_manager_config")


@pytest.fixture()
def init_host_config_type():
    type_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_host_config "
            "(action, ip, data_scenario,operator, description) "
            'VALUES ("deploy","x.x.x.x","db","admin","db接入host信息");',
        )
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_host_config "
            "(action, ip, data_scenario,operator, description) "
            'VALUES ("deploy","x.x.x.x","http","admin","http接入host信息");',
        )
    try:
        yield type_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_host_config")


@pytest.fixture()
def init_access_db():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_db_info "
            "(raw_data_id, db_connstr, table_name, db_type_id, frequency, poll_type, "
            "primary_key, before_time, time_field,username, password, active, created_by, "
            'updated_by,description) value(123,"jdbc:mysql://test.db.com:3306/hub?autoReconnect=true", '
            '"test_table_name", 1, 20, "time", "timestamp", 60, "created_at", '
            '"root", "passwd", 1, "admin", "admin", "test data");',
            data_id=data_id,
        )
        db_helper.execute(
            cur,
            "insert into access_db_info "
            "(raw_data_id, db_connstr, table_name, db_type_id, frequency, poll_type, "
            "primary_key, before_time, time_field,username, password, active, "
            "created_by, updated_by,description) "
            'value(124,"jdbc:mysql://test.db.com:3306/hub?autoReconnect=true", '
            '"test_table_name", 1, 20, "pri", "id", 60, "created_at", '
            '"root", "passwd", 1, "admin", "admin", "test data");',
            data_id=data_id,
        )
        db_helper.execute(
            cur,
            "insert into access_task "
            "(id, data_scenario, bk_biz_id, deploy_plans, result, action, created_by, description) "
            'value(968, "db", 591, \'[{"host_list": [{"ip": "xxx", "bk_cloud_id": 1}], '
            '"config": [{"raw_data_id": 297, "deploy_plan_id": [17]}]}]\', '
            '"pending", "deploy", "admin", "");',
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data")
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_task")
