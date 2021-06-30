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
from common.api.base import DataResponse
from datahub.access.tests.utils import get, post
from datahub.databus.exceptions import TaskChannelNotFound
from django.db import IntegrityError

from datahub.databus import settings

JSON_CONF = (
    '{"extract": {"args": [], "next": {"subtype": "assign_obj", "next": null, "type": "assign", "assign": '
    '[{"assign_to": "utc_time", "type": "string", "key": "_utctime_"}, {"assign_to": "gseindex", "type": '
    '"long", "key": "_gseindex_"}], "label": "label02"}, "result": "json_data", "label": "json_data", "type":'
    ' "fun", "method": "from_json"}, "conf": {"timestamp_len": 0, "encoding": "UTF8", "time_format": '
    '"yyyy-MM-dd HH:mm:ss", "timezone": 0, "output_field_name": "timestamp", "time_field_name": "utc_time"}}'
)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_success(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 591, "count_freq": 1}}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("databus.rt.get_databus_rt_info", return_value=DataResponse({"result": True}))

    mocker.patch(
        "databus.task.process_clean_task_in_kafka",
        return_value=["puller_clean_591_xxx_test in databus_clean_M"],
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)

    assert res["result"]
    assert res["data"]["result_table_id"] == "591_etl_pizza_abcabc"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_rt_error(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": False, "message": "failed"}),
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"创建DataProcessing失败：failed"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_no_usable_inner_channel(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 591, "count_freq": 1}}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"没有可用的inner channel，请先初始化inner channel的信息"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_delete_rt_warning(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 591, "count_freq": 1}}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_storage_failed(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": "清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": False, "message": "failed"}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"创建ResultTable的存储失败：failed"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_data_not_found_error(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"对象不存在：result_table[id=591_etl_pizza_abcabc]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_no_rawdata(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 591, "count_freq": 1}}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("databus.model_manager.get_raw_data_by_id", return_value=None)

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"RawData不存在：42"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_integrityerror(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("databus.models.DatabusClean.objects.create", side_effect=IntegrityError)

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)
    assert not res["result"]

    assert res["message"] == u"对象已存在，无法添加：databus_clean[processing_id=591_etl_pizza_abcabc]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_latch_task_error(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }
    mocker.patch(
        "databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 591, "count_freq": 1}}),
    )

    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )
    mocker.patch(
        "databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("databus.rt.get_databus_rt_info", return_value=DataResponse({"result": True}))

    mocker.patch("databus.task.process_clean_task_in_kafka", side_effect=TaskChannelNotFound)

    res = post("/v3/databus/scenarios/setup_clean/", PARAMS)

    assert not res["result"]
    assert res["message"] == u"无法找到目标channel"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean")
def test_update_success(mocker):
    """dataid存在"""

    PARAMS = {
        "result_table_id": "591_etl_pizza_abcabc",
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }

    mocker.patch(
        "databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/update_clean/", PARAMS)
    assert res["result"]

    PARAMS["json_config"] = "not a json"
    res = post("/v3/databus/scenarios/update_clean/", PARAMS)
    assert res["result"] is False
    assert res["code"] == "1570014"
    assert res["message"] == u"清洗配置错误"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean")
def test_update_update_rt_error(mocker):
    """dataid存在"""

    PARAMS = {
        "result_table_id": "591_etl_pizza_abcabc",
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }

    mocker.patch(
        "databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": False, "message": "failed"}),
    )

    res = post("/v3/databus/scenarios/update_clean/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"更新ResultTable失败：failed"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_update_no_record(mocker):
    """dataid存在"""

    PARAMS = {
        "result_table_id": "591_etl_pizza_abcabc",
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 591,
        "result_table_name": "etl_pizza_abcabc",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "report_time",
                "field_alias": "时间",
                "field_type": "report_time",
                "is_dimension": False,
                "field_index": 1,
            },
            {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 2,
            },
            {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": False,
                "field_index": 3,
            },
        ],
    }

    mocker.patch(
        "databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/update_clean/", PARAMS)

    assert not res["result"]
    assert res["message"] == u"找不到清洗process id: 591_etl_pizza_abcabc"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_shipper_scenario_data")
def test_stop_clean(mocker):
    # 检查rt不存在的场景
    params = {"raw_data_id": 10000, "result_table_id": "591_bad_rt"}
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )
    # rt不存在，抛出异常
    res = post("/v3/databus/scenarios/stop_clean/", params)
    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：result_table[id=591_bad_rt]"

    # 清洗rt和rawdata不匹配
    params["result_table_id"] = "591_no_raw_data"

    # patch一些调用的返回结果
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "result_table_name_alias": "test scenario 5",
                    "processing_type": "clean",
                    "storages": {
                        "es": {
                            "storage_channel": {},
                            "expires": "7d",
                            "storage_config": '{"analyzedFields": [], "dateFields": ["dtEventTime", "dtEventTimeStamp"'
                            ', "localTime"]}',
                            "storage_cluster": {
                                "cluster_name": "es-test",
                                "cluster_type": "es",
                                "priority": 0,
                                "connection_info": '{"port": 8080, "transport": 9300}',
                                "cluster_group": "default",
                            },
                            "physical_table_name": "591_with_raw_data",
                        },
                        "kafka": {
                            "storage_channel": {
                                "id": 11,
                                "zk_port": 2181,
                                "cluster_name": "inner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 5,
                                "cluster_domain": "xx.xx.xx.xx",
                                "cluster_port": 9092,
                                "zk_domain": "xx.xx.xx.xx",
                                "zk_root_path": "/kafka-test-3",
                            },
                            "expires": "3d",
                            "priority": 5,
                            "active": 1,
                            "storage_config": "{}",
                            "storage_cluster": {},
                        },
                    },
                    "project_id": 4,
                    "result_table_id": "591_with_raw_data",
                    "project_name": "测试项目",
                    "bk_biz_id": 591,
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_index": 1,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "report_time",
                            "field_index": 2,
                        },
                        {
                            "field_type": "long",
                            "is_dimension": False,
                            "field_name": "gseindex",
                            "field_index": 3,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "path",
                            "field_index": 4,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": True,
                            "field_name": "log",
                            "field_index": 5,
                        },
                    ],
                    "result_table_name": "with_raw_data",
                },
                "result": True,
            }
        ),
    )

    # rawdata不存在
    res = post("/v3/databus/scenarios/stop_clean/", params)
    assert res["result"] is False
    assert res["code"] == "1570033"
    assert res["message"] == u"清洗ResultTable和源数据不匹配: 591_no_raw_data 10000"

    # rawdata存在
    params["raw_data_id"] = 101
    params["result_table_id"] = "591_with_raw_data"

    mocker.patch("databus.task.stop_databus_task", return_value=None)

    res = post("/v3/databus/scenarios/stop_clean/", params)
    assert res["result"]
    assert res["data"]


def test_available_storages():
    # 测试available_storage接口
    res = get("/v3/databus/scenarios/available_storage/")
    assert res["result"]
    for storage in res["data"]:
        assert storage in settings.AVAILABLE_STORAGE_LIST


def test_storage_clusters(mocker):
    # 测试调用storekit接口失败的场景
    mocker.patch(
        "databus.api.StoreKitApi.storage_cluster_configs.list",
        return_value=DataResponse({"result": False}),
    )

    res = get("/v3/databus/scenarios/storage_clusters/")
    assert res["result"] is False
    assert res["code"] == "1570031"
    assert res["message"] == u"获取存储集群信息失败"

    # 测试调用storekit接口成功的场景
    mocker.patch(
        "databus.api.StoreKitApi.storage_cluster_configs.list",
        return_value=DataResponse(
            {
                "result": True,
                "data": [
                    {
                        "id": 10,
                        "cluster_name": "es-test",
                        "cluster_type": "es",
                        "cluster_group": "default",
                    },
                    {
                        "id": 11,
                        "cluster_name": "hdfs-test",
                        "cluster_type": "hdfs",
                        "cluster_group": "default",
                    },
                    {
                        "id": 12,
                        "cluster_name": "mysql-test",
                        "cluster_type": "mysql",
                        "cluster_group": "default",
                    },
                    {
                        "id": 13,
                        "cluster_name": "tsdb-test",
                        "cluster_type": "tsdb",
                        "cluster_group": "default",
                    },
                    {
                        "id": 14,
                        "cluster_name": "tredis-test",
                        "cluster_type": "tredis",
                        "cluster_group": "default",
                    },
                    {
                        "id": 15,
                        "cluster_name": "druid-test",
                        "cluster_type": "druid",
                        "cluster_group": "default",
                    },
                    {
                        "id": 16,
                        "cluster_name": "hermes-test",
                        "cluster_type": "hermes",
                        "cluster_group": "default",
                    },
                    {
                        "id": 17,
                        "cluster_name": "queue-test",
                        "cluster_type": "queue",
                        "cluster_group": '["default","default2"]',
                    },
                    {
                        "id": 18,
                        "cluster_name": "hdfs-test",
                        "cluster_type": "hdfs",
                        "cluster_group": "default",
                    },
                    {
                        "id": 19,
                        "cluster_name": "xxx-test",
                        "cluster_type": "tredis",
                        "cluster_group": "default",
                    },
                    {
                        "id": 20,
                        "cluster_name": "hdfs-test2",
                        "cluster_type": "hdfs",
                        "cluster_group": "default",
                    },
                    {
                        "id": 21,
                        "cluster_name": "hdfs-test3",
                        "cluster_type": "hdfs",
                        "cluster_group": "default",
                    },
                ],
            }
        ),
    )

    res = get("/v3/databus/scenarios/storage_clusters/")
    assert res["result"] is True
    assert "es" in res["data"]
    assert "eslog" in res["data"]
    assert "mysql" in res["data"]
    assert "hdfs" in res["data"]
    assert res["data"]["es"] == res["data"]["eslog"]
    assert "es-test" in res["data"]["es"]["clusters"]
    assert [7, 15, 30] == res["data"]["es"]["expires"]
    assert "hdfs-test" in res["data"]["hdfs"]["clusters"]
    assert "hdfs-test2" in res["data"]["hdfs"]["clusters"]
    assert "hdfs-test3" in res["data"]["hdfs"]["clusters"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_shipper_scenario_data")
def test_setup_shipper(mocker):
    # 检查rt不存在的场景
    params = {
        "raw_data_id": 10000,
        "result_table_id": "591_bad_rt",
        "cluster_type": "es",
        "create_storage": False,
    }
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )
    # rt不存在，抛出异常
    res = post("/v3/databus/scenarios/setup_shipper/", params)

    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：result_table[id=591_bad_rt]"

    # 检查不符合要求的存储
    params["result_table_id"] = "591_no_raw_data"
    params["cluster_type"] = "bad_storage"
    # 存储类型不存在，抛出异常
    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570032"
    assert res["message"] == u"存储类型不支持: bad_storage"

    # 不创建存储，仅仅启动分发任务
    params["cluster_type"] = "es"

    # patch一些调用的返回结果
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "result_table_name_alias": "test scenario 5",
                    "processing_type": "clean",
                    "storages": {
                        "es": {
                            "storage_channel": {},
                            "expires": "7d",
                            "storage_config": '{"analyzedFields": [], "dateFields": ["dtEventTime", "dtEventTimeStamp"'
                            ', "localTime"]}',
                            "storage_cluster": {
                                "cluster_name": "es-test",
                                "cluster_type": "es",
                                "priority": 0,
                                "connection_info": '{"port": 8080, "transport": 9300}',
                                "cluster_group": "default",
                            },
                            "physical_table_name": "591_with_raw_data",
                        },
                        "kafka": {
                            "storage_channel": {
                                "id": 11,
                                "zk_port": 2181,
                                "cluster_name": "inner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 5,
                                "cluster_domain": "xx.xx.xx.xx",
                                "cluster_port": 9092,
                                "zk_domain": "xx.xx.xx.xx",
                                "zk_root_path": "/kafka-test-3",
                            },
                            "expires": "3d",
                            "priority": 5,
                            "active": 1,
                            "storage_config": "{}",
                            "storage_cluster": {},
                        },
                    },
                    "project_id": 4,
                    "result_table_id": "591_with_raw_data",
                    "project_name": "测试项目",
                    "bk_biz_id": 591,
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_index": 1,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "report_time",
                            "field_index": 2,
                        },
                        {
                            "field_type": "long",
                            "is_dimension": False,
                            "field_name": "gseindex",
                            "field_index": 3,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "path",
                            "field_index": 4,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": True,
                            "field_name": "log",
                            "field_index": 5,
                        },
                    ],
                    "result_table_name": "with_raw_data",
                },
                "result": True,
            }
        ),
    )

    # rawdata不存在
    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570033"
    assert res["message"] == u"清洗ResultTable和源数据不匹配: 591_no_raw_data 10000"

    # rawdata存在
    params["raw_data_id"] = 101
    params["result_table_id"] = "591_with_raw_data"

    mocker.patch("databus.task.add_databus_shipper_task", return_value=None)
    mocker.patch("databus.task.start_databus_shipper_task", return_value=None)

    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"]
    assert res["data"]["cluster_type"] == "es"
    assert res["data"]["result_table_id"] == "591_with_raw_data"

    # 测试创建存储的逻辑
    params["create_storage"] = True

    # es存储已存在，创建失败
    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570034"
    assert res["message"] == u"清洗ResultTable已关联存储es"

    # 改为创建不存在的存储mysql
    params["cluster_type"] = "mysql"
    params["cluster_name"] = "mysql-test"
    params["storage_config"] = "{}"
    params["expire_days"] = 15

    # 创建存储失败，抛出异常
    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": False, "message": ""}),
    )
    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570012"
    assert res["message"] == u"创建ResultTable的存储失败："

    # 创建存储成功，初始化存储失败
    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )
    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create_table",
        return_value=DataResponse({"result": False, "message": ""}),
    )

    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570036"
    assert res["message"] == u"创建物理表失败。"

    # 创建存储成功，初始化存储成功，然后启动分发任务
    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create",
        return_value=DataResponse({"result": True}),
    )
    mocker.patch(
        "databus.api.StoreKitApi.storage_result_tables.create_table",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"]
    assert res["data"]["cluster_type"] == "mysql"
    assert res["data"]["result_table_id"] == "591_with_raw_data"

    params["cluster_type"] = "eslog"
    params["create_storage"] = False
    mocker.patch("databus.task.process_eslog_task_in_cluster", return_value=None)

    res = post("/v3/databus/scenarios/setup_shipper/", params)
    assert res["result"]
    assert res["data"]["cluster_type"] == "eslog"
    assert res["data"]["result_table_id"] == "591_with_raw_data"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_shipper_scenario_data")
def test_stop_shipper(mocker):
    # 检查rt不存在的场景
    params = {
        "raw_data_id": 10000,
        "result_table_id": "591_bad_rt",
        "cluster_type": "es",
    }
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )
    # rt不存在，抛出异常
    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1500004"
    assert res["message"] == u"对象不存在：result_table[id=591_bad_rt]"

    # 检查不符合要求的存储
    params["result_table_id"] = "591_no_raw_data"
    params["cluster_type"] = "bad_storage"
    # 存储类型不存在，抛出异常
    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570032"
    assert res["message"] == u"存储类型不支持: bad_storage"

    # 不创建存储，仅仅启动分发任务
    params["cluster_type"] = "mysql"

    # patch一些调用的返回结果
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "result_table_name_alias": "test scenario 5",
                    "processing_type": "clean",
                    "storages": {
                        "es": {
                            "storage_channel": {},
                            "expires": "7d",
                            "storage_config": '{"analyzedFields": [], "dateFields": ["dtEventTime", "dtEventTimeStamp"'
                            ', "localTime"]}',
                            "storage_cluster": {
                                "cluster_name": "es-test",
                                "cluster_type": "es",
                                "priority": 0,
                                "connection_info": '{"port": 8080, "transport": 9300}',
                                "cluster_group": "default",
                            },
                            "physical_table_name": "591_with_raw_data",
                        },
                        "kafka": {
                            "storage_channel": {
                                "id": 11,
                                "zk_port": 2181,
                                "cluster_name": "inner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 5,
                                "cluster_domain": "xx.xx.xx.xx",
                                "cluster_port": 9092,
                                "zk_domain": "xx.xx.xx.xx",
                                "zk_root_path": "/kafka-test-3",
                            },
                            "expires": "3d",
                            "priority": 5,
                            "active": 1,
                            "storage_config": "{}",
                            "storage_cluster": {},
                        },
                    },
                    "project_id": 4,
                    "result_table_id": "591_with_raw_data",
                    "project_name": "测试项目",
                    "bk_biz_id": 591,
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_index": 1,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "report_time",
                            "field_index": 2,
                        },
                        {
                            "field_type": "long",
                            "is_dimension": False,
                            "field_name": "gseindex",
                            "field_index": 3,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "path",
                            "field_index": 4,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": True,
                            "field_name": "log",
                            "field_index": 5,
                        },
                    ],
                    "result_table_name": "with_raw_data",
                },
                "result": True,
            }
        ),
    )

    # rawdata不存在
    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570033"
    assert res["message"] == u"清洗ResultTable和源数据不匹配: 591_no_raw_data 10000"

    # rawdata存在
    params["raw_data_id"] = 101
    params["result_table_id"] = "591_with_raw_data"

    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"] is False
    assert res["code"] == "1570035"
    assert res["message"] == u"清洗ResultTable未关联存储mysql"

    # 对于存在的es存储，停止其分发任务
    params["cluster_type"] = "es"
    mocker.patch("databus.task.stop_databus_task", return_value=None)

    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"]
    assert res["data"]

    # 对于eslog类型存储的任务，停止对应的分发任务
    params["cluster_type"] = "eslog"
    res = post("/v3/databus/scenarios/stop_shipper/", params)
    assert res["result"]
    assert res["data"]
