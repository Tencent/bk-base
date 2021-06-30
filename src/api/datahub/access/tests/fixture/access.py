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
def init_access():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id,bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","db","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )
        db_helper.execute(
            cur,
            """insert into access_operation_log
                          (id,raw_data_id,args,created_by, updated_by, description, status)
                          values (1,123,"{}","admin","admin","","stopped");""",
        )
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (1, 123, \'{"scope_config": {"paths": [{"path":["/tmp/*.log"], '
            '"system":"linux"}]}, "module_scope": [{"bk_obj_id": '
            '"xxxx", "bk_inst_id": 123}], "host_scope": [{"ip": "x.x.x.x",'
            ' "bk_cloud_id": 222}]}\','
            ' "incr", 0, null ,0 ,null, null, \'{"fields": [{"index": 1,'
            ' "logic_op": "and", "value": "111", "op": "=11111"}], "delimiter": "|"}\',"" '
            ");",
        )
        db_helper.insert(
            cur,
            "access_raw_data_task",
            id=data_id,
            task_id=1,
            raw_data_id="123",
            created_by="admin",
            created_at="2019-01-01 00:00:00",
            updated_by="admin",
            updated_at="2019-01-01 00:00:00",
            description="description",
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data")
            db_helper.execute(cur, "DELETE FROM access_operation_log")
            db_helper.execute(cur, "DELETE FROM access_resource_info")
            db_helper.execute(cur, "DELETE FROM access_raw_data_task")


@pytest.fixture()
def init_access_raw_data():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id,bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","db","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id = 123")


@pytest.fixture()
def init_access_operation_log():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """insert into access_operation_log
                          (id,raw_data_id,args,created_by, updated_by, description, status)
                          values (1,123,"{}","admin","admin","","success");""",
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_operation_log")


@pytest.fixture
def init_access_raw_data_task():

    data_id = 1

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "access_raw_data_task",
            id=data_id,
            task_id=1,
            raw_data_id="123",
            created_by="admin",
            created_at="2019-01-01 00:00:00",
            updated_by="admin",
            updated_at="2019-01-01 00:00:00",
            description="description",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data_task where id = 1")


@pytest.fixture
def init_access_task():
    data_id = 1

    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.insert(
            cur,
            "access_task",
            id=data_id,
            data_scenario="db",
            bk_biz_id=591,
            deploy_plans="",
            logs=u"INFO|2019-01-25 14:43:44|start",
            logs_en="",
            context="",
            result="success",
            action="deploy",
            created_at="2019-01-01 00:00:00",
            updated_by="admin",
            created_by="admin",
            updated_at="2019-01-01 00:00:00",
            description="description",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_task where id = 1")


@pytest.fixture()
def init_tqos_zone_conf():
    tqos_zone_id = 11
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_tqos_zone_config "
            "(tqos_zone_id, tqos_zone_name, ip_set,description) "
            'VALUES (11,"测试大区ID","x.x.x.x","测试大区ID");',
        )
    try:
        yield tqos_zone_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_tqos_zone_config where tqos_zone_id = 11")


@pytest.fixture()
def init_db_access_raw_data():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id,bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","log","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id = 123")


@pytest.fixture()
def init_script_access_raw_data():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id,bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","script","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id = 123")


@pytest.fixture()
def init_http_access_raw_data():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id,bk_biz_id, raw_data_name, raw_data_alias, data_source, data_scenario, bk_app_code,"
            "storage_channel_id, created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server","http","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id = 123")


@pytest.fixture()
def init_log_access_raw_data():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_raw_data"
            "(id, bk_biz_id, raw_data_name, raw_data_alias, data_source, data_encoding,"
            "data_scenario, bk_app_code, storage_channel_id,"
            "created_by, updated_by, description, maintainer)"
            'values (123,2,"test","test","business_server", "gbk", "log","bkdata",'
            '1002,"admin","admin","","admin" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_raw_data where id = 123")


@pytest.fixture()
def init_access_log_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (11, 123, \'{"scope_config": {"paths": [{"path":["/tmp/*.log"], '
            '"system":"linux"}]}, "module_scope": [{"bk_obj_id": '
            '"xxxx", "bk_inst_id": 123}], "host_scope": [{"ip": "x.x.x.x",'
            ' "bk_cloud_id": 222}]}\','
            ' "incr", 0, null ,0 ,null, null, \'{"fields": [{"index": 1,'
            ' "logic_op": "and", "value": "111", "op": "=11111"}], "delimiter": "|"}\',"" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_tqos_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (11, 123, \'{"scope_config": {"paths": [{"path":["/tmp/*.log"], '
            '"system":"linux"}]}, "module_scope": [{"bk_obj_id": '
            '"xxxx", "bk_inst_id": 123}], "host_scope": [{"ip": "x.x.x.x",'
            ' "bk_cloud_id": 222}]}\','
            ' "incr", 0, null ,0 ,null, null,null,""  '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_ondition_format_error():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (11, 123, \'{"scope_config": {"paths": ["/tmp/*.log", "/tmp/*.l",'
            ' "/tmp/*.aaaz"]}, "module_scope": [{"bk_obj_id": "xxxx", '
            '"bk_inst_id": 123}], "host_scope": [{"ip": "x.x.x.x",'
            ' "bk_cloud_id": 222}]}\','
            ' "incr", 0, null ,0 ,"yyyy-MM-dd HH:mm:ss", '
            'null, \'{"fields": [{"inxxdex": 1, "logxic_op": "and",'
            ' "valxue": "111", "oxp": "=11111"}], "delimiter": "|"}\',"" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_time_db_resource_filter():
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
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, \'[{"key":"key","op":"="'
            ',"logic_op":"and","value":"val"}]\', ""'
            ");",
        )
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description )"
            'values (12, 123, \'{"db_host": "x.x.x.x", '
            '"db_port": 10000, "db_user": "user", "db_pass": "XXX",'
            ' "db_name": "bkdata_basic", "table_name": "access_db_info",'
            ' "db_type_id": 1}\','
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, \'[{"key":"key","op":"="'
            ',"logic_op":"and","value":"val"}]\', ""'
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id in (11,12)")


@pytest.fixture()
def init_access_script_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description )"
            'values (11, 123, "",'
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "", ""'
            ");",
        )

        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description )"
            'values (12, 123, "",'
            ' "time", 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "", ""'
            ");",
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id in (11,12)")


@pytest.fixture()
def init_access_pri_db_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions)"
            'values (11, 123, \'{"db_host": "x.x.x.x", '
            '"db_port": 10000, "db_user": "user", "db_pass": "pwd", '
            '"db_name": "bkdata_basic", "table_name": "access_db_info",'
            ' "db_type_id": 1}\','
            ' "pri", 0, "pid" ,0 ,"", 100, "" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_all_db_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions)"
            'values (11, 123, \'{"db_host": "x.x.x.x",'
            ' "db_port": 10000, "db_user": "user", "db_pass": "pwd", '
            '"db_name": "bkdata_basic", "table_name": "access_db_info", '
            '"db_type_id": 1}\','
            ' "all", 0, null ,0 ,"", 100, "" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_db_no_poll_type():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions)"
            'values (11, 123, \'{"db_host": "x.x.x.x",'
            ' "db_port": 10000, "db_user": "user",'
            '"db_pass": "pwd", "db_name": "bkdata_basic", '
            '"table_name": "access_db_info", "db_type_id": 1}\','
            ' "unknow", 0, null ,0 ,"", 100, "" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


def clean_access_db_info():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(cur, "DELETE FROM access_db_info")


@pytest.fixture()
def init_access_get_http_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions, description)"
            'values (11, 123, \'{"url": "http://xxx", "method": "get"}\','
            ' null, 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "", "" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_post_http_resource():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "insert into access_resource_info"
            "(id, raw_data_id, resource, collection_type, start_at, increment_field, period, time_format,"
            "before_time, conditions)"
            'values (11, 123, \'{"url": "http://xxx", "method": "post", "body":"x"}\','
            ' null, 0, "create_at" ,0 ,"yyyy-MM-dd HH:mm:ss", 100, "" '
            ");",
        )

    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_resource_info where id = 11")


@pytest.fixture()
def init_access_tlog():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_tlog_info "
            "(id, raw_data_id, scope_type, scope_objects, scope_config, conditions, "
            "created_by, updated_by, description) "
            'VALUES (11, 123, "module", '
            '\'[{"bk_obj_id": "set", "bk_inst_id": 123}, {"bk_obj_id": "module", "bk_inst_id": 321}]\', '
            '\'{"paths": ["/tmp/*.log"]}\', '
            '\'{"delimiter":"|","fields":[{"index":1,"op":"=","value":"111","logic_op":""}]}\', '
            '"admin", "admin", "");',
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(
                cur,
                """
            DELETE FROM access_tlog_info WHERE id = %(data_id)s
            """,
                data_id=data_id,
            )


@pytest.fixture()
def init_task_log():
    type_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_task "
            "(id, data_scenario, bk_biz_id, deploy_plans, logs, logs_en, context, result, created_by, "
            "description) "
            'VALUES (100, "log", 591, "{}", "", "", "", "", "", "");',
        )
    try:
        yield type_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_task where id = 100")


@pytest.fixture()
def init_log_type():
    type_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_host_config "
            "(action, ip, data_scenario,operator, description) "
            'VALUES ("package","x.x.x.x","log","admin","log接入host信息");',
        )
    try:
        yield type_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_host_config")


@pytest.fixture()
def init_tlog_type():
    type_id = 1
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_host_config "
            "(action, ip, data_scenario,operator, description) "
            'VALUES ("package","x.x.x.x","tlog","admin","tlog接入host信息");',
        )
    try:
        yield type_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_host_config")


@pytest.fixture()
def init_access_log():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_log_info "
            "(id, raw_data_id, scope_type, scope_objects, scope_config, conditions, "
            "created_by, updated_by, description) "
            'VALUES (999, 123, "module", '
            '\'[{"bk_obj_id": "set", "bk_inst_id": 123}, {"bk_obj_id": "module", "bk_inst_id": 321}]\', '
            '\'{"paths": ["/tmp/*.log"]}\', '
            '\'{"delimiter":"|","fields":[{"fieldindex":1,"op":"=","word":"111","logic_op":""}]}\', '
            '"admin", "admin", "");',
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(
                cur,
                """
            DELETE FROM access_log_info WHERE id = %(data_id)s
            """,
                data_id=data_id,
            )


@pytest.fixture()
def init_access_log_end_with_patten():
    data_id = 123
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            "INSERT INTO access_log_info "
            "(id, raw_data_id, scope_type, scope_objects, scope_config, conditions, "
            "created_by, updated_by, description) "
            'VALUES (999, 123, "module", '
            '\'[{"bk_obj_id": "set", "bk_inst_id": 123}, {"bk_obj_id": "module", "bk_inst_id": 321}]\', '
            '\'{"paths": ["/tmp/*.log*"]}\', '
            '\'{"delimiter":"|","fields":[{"fieldindex":1,"op":"=","word":"111","logic_op":""}]}\', '
            '"admin", "admin", "");',
        )
    try:
        yield data_id
    finally:
        with db_helper.open_cursor("mapleleaf") as cur:
            db_helper.execute(cur, "DELETE FROM access_log_info")
