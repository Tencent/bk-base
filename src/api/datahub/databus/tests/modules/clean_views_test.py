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

from __future__ import print_function

import json

import httpretty as hp
import pytest
from common.api.base import DataResponse
from datahub.access.tests.utils import delete, get, post, put
from datahub.databus.api import MetaApi, StoreKitApi  # noqa
from datahub.databus.models import DatabusChannel  # noqa
from datahub.databus.tests.fixture.channel_fixture import (  # noqa
    add_channel,
    del_clean_info,
)
from datahub.databus.tests.fixture.clean_factors_fixture import (  # noqa
    add_clean_factors,
)
from datahub.databus.tests.fixture.clean_fixture import add_clean  # noqa
from datahub.databus.tests.mock_api import result_table
from datahub.databus.tests.mock_api.meta import get_inland_tag_ok
from datahub.databus.tests.mock_api.result_table import get_databus_old_ok
from datahub.pizza_settings import META_API_URL
from django.db.backends.dummy.base import ignore

from datahub.databus import model_manager

JSON_CONF = (
    '{"extract": {"args": [], "next": {"subtype": "assign_obj", "next": null, "type": "assign", "assign": '
    '[{"assign_to": "utc_time", "type": "string", "key": "_utctime_"}, {"assign_to": "gseindex", "type":'
    ' "long", "key": "_gseindex_"}], "label": "label02"}, "result": "json_data", "label": "json_data", "type":'
    ' "fun", "method": "from_json"}, "conf": {"timestamp_len": 0, "encoding": "UTF8", "time_format": '
    '"yyyy-MM-dd HH:mm:ss", "timezone": 0, "output_field_name": "timestamp", "time_field_name": "utc_time"}}'
)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_create_failed(mocker):
    PARAMS = {
        "raw_data_id": 42,
        "json_config": "{}",
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
    hp.enable()
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"bk_biz_id": 2, "count_freq": 1}}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=model_manager.get_inner_channel_by_priority("inner"),
    )
    get_inland_tag_ok()

    res = post("/v3/databus/cleans/", PARAMS)
    assert not res["result"]
    assert res["code"] == "1570014"
    assert res["message"] == u"清洗配置错误"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "del_clean_info")
def test_create_success(mocker):
    inner_channels = model_manager.get_inner_channel_by_priority("kafka")
    assert inner_channels
    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=inner_channels[0],
    )

    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    PARAMS = {
        "raw_data_id": 42,
        "json_config": "{}",
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    PARAMS["json_config"] = JSON_CONF
    res = post("/v3/databus/cleans/", PARAMS)
    print(res)
    assert res["result"]
    assert res["data"]["processing_id"] == "2_output"
    assert res["data"]["raw_data_id"] == 42
    assert (
        res["data"]["json_config"]
        == '{"extract": {"args": [], "next": {"subtype": "assign_obj", "next": null, "type": "assign", "assign": '
        '[{"assign_to": "utc_time", "type": "string", "key": "_utctime_"}, {"assign_to": "gseindex", "type": '
        '"long", "key": "_gseindex_"}], "label": "label02"}, "result": "json_data", "label": "json_data", "type": '
        '"fun", "method": "from_json"}, "conf": {"timestamp_len": 0, "encoding": "UTF8", "time_format": "yyyy-MM-dd '
        'HH:mm:ss", "timezone": 0, "output_field_name": "timestamp", "time_field_name": "utc_time"}}'
    )
    assert res["data"]["result_table_id"] == "2_output"
    assert res["data"]["status"] == "stopped"
    assert res["data"]["status_display"] == u"stopped"
    assert res["data"]["result_table"]["result_table_id"] == "2_output"

    PARAMS["json_config"] = "not a json"
    res = post("/v3/databus/cleans/", PARAMS)

    assert res["result"] is False
    assert res["code"] == "1570014"
    assert res["message"] == u"清洗配置错误"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "del_clean_info")
@ignore
def test_create_rt_error(mocker):
    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    result_table.post_2_output_ok()

    inner_channels = model_manager.get_inner_channel_by_priority("kafka")
    assert inner_channels
    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=inner_channels[0],
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": False, "message": "failed to create data processing"}),
    )

    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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

    res = post("/v3/databus/cleans/", PARAMS)
    print(res)
    assert not res["result"]
    assert res["message"] == u"创建DataProcessing失败：failed to create data processing"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "del_clean_info")
def test_create_no_usable_inner_channel(mocker):
    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.common_helper.add_task_log",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/cleans/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"没有可用的inner channel，请先初始化inner channel的信息"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "del_clean_info")
def test_create_storage_failed(mocker):
    inner_channels = model_manager.get_inner_channel_by_priority("kafka")
    assert inner_channels
    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=inner_channels[0],
    )

    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": "清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": False, "message": "failed to create storage"}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/cleans/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"创建ResultTable的存储失败：failed to create storage"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "del_clean_info")
def test_create_data_not_found_error(mocker):
    inner_channels = model_manager.get_inner_channel_by_priority("kafka")
    assert inner_channels
    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=inner_channels[0],
    )

    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/cleans/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"对象不存在：result_table[id=2_output]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel", "add_clean")
def test_retrieve_success(mocker):
    """dataid存在"""
    _clean_success_parpare(mocker)
    res = get("/v3/databus/cleans/2_output/")
    assert res["result"]
    assert res["data"]["processing_id"] == "2_output"
    assert res["data"]["raw_data_id"] == 42
    assert res["data"]["json_config"] == "test_json_config"
    assert res["data"]["result_table_id"] == "2_output"
    assert res["data"]["status"] == "stopped"
    assert res["data"]["status_display"] == u"stopped"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_retrieve_data_not_found(mocker):
    """dataid不存在"""

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False}),
    )

    res = get("/v3/databus/cleans/2_output/")
    assert not res["result"]
    assert res["message"] == u"对象不存在：result_table[id=2_output]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_update_success(mocker):
    """dataid存在"""
    _clean_success_parpare(mocker)
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = put("/v3/databus/cleans/2_output/", PARAMS)
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_update_update_rt_error(mocker):
    """dataid存在"""
    _clean_success_parpare(mocker)
    PARAMS = {
        "raw_data_id": 42,
        "json_config": JSON_CONF,
        "pe_config": "",
        "bk_biz_id": 2,
        "clean_config_name": "output",
        "result_table_name": "output",
        "result_table_name_alias": "清洗表测试",
        "description": u"清洗测试",
        "bk_username": "admin",
        "fields": [
            {
                "field_name": "timestamp",
                "field_alias": "时间戳",
                "field_type": "timestamp",
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
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": False, "message": "failed"}),
    )

    res = put("/v3/databus/cleans/2_output/", PARAMS)
    assert not res["result"]
    assert res["message"] == u"更新ResultTable失败：failed"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_destroy_not_support(mocker):
    """dataid存在"""
    _clean_success_parpare(mocker)
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    res = delete("/v3/databus/cleans/2_output/")
    assert not res["result"]
    assert res["message"] == "暂不支持删除存在下游存储/计算节点的清洗任务"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_destroy_success(mocker):
    """dataid存在"""
    _clean_success_parpare(mocker)
    hp.register_uri(
        hp.GET,
        META_API_URL + "result_tables/2_output/",
        body=json.dumps(
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": True,
                "data": {
                    "bk_biz_id": 2,
                    "project_id": 2331,
                    "result_table_id": "2_output",
                    "result_table_name": "output",
                    "result_table_name_alias": "output_alias",
                    "processing_type": "clean",
                    "sensitivity": "output_alias",
                    "created_by": "committer",
                    "created_at": "2018-09-19 17:03:50",
                    "updated_by": None,
                    "updated_at": "2018-09-19 17:03:50",
                    "description": "输出",
                    "concurrency": 0,
                    "data_processing": {
                        "project_id": 1,
                        "processing_id": "xxx",
                        "processing_name": "xxx",
                        "processing_type": "xxx",
                        "created_by ": "xxx",
                        "created_at": "xxx",
                        "updated_by": "xxx",
                        "updated_at": "xxx",
                        "description": "xxx",
                    },
                    "count_freq": 3600,
                    "tags": {"manage": {"geog_area": [{"code": "inland"}]}},
                    "fields": [
                        {
                            "id": 13730,
                            "result_table_id": "2_output",
                            "field_index": 1,
                            "field_name": "timestamp",
                            "field_alias": "时间",
                            "description": "",
                            "field_type": "timestamp",
                            "is_dimension": 0,
                            "origins": "",
                            "created_by": None,
                            "created_at": None,
                            "updated_by": None,
                            "updated_at": None,
                        }
                    ],
                    "storages": {
                        "kafka": {
                            "id": 1000,
                            "storage_channel_id": 1000,
                            "result_table_id": "xxx",
                            "storage_cluster": {},
                            "storage_channel": {
                                "id": 1000,
                                "cluster_name": "xxx",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "cluster_domain": "xxx",
                                "cluster_backup_ips": "xxx",
                                "cluster_port": 2432,
                                "zk_domain": "127.0.0.1",
                                "zk_port": 3481,
                                "zk_root_path": "/abc/defg",
                                "priority": 234,
                                "attribute": "bkdata",
                                "description": "sdfdsf",
                            },
                            "physical_table_name": "xxx",
                            "expires": "xxx",
                            "storage_config": "{}",
                            "priority": 1,
                            "description": "xxx",
                            "created_by": None,
                            "created_at": None,
                            "updated_by": None,
                            "updated_at": None,
                        }
                    },
                },
            }
        ),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    res = delete("/v3/databus/cleans/2_output/")
    assert not res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_channel")
def test_destroy_processing_not_found(mocker):
    """dataid存在"""

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    res = delete("/v3/databus/cleans/2_output/")
    assert not res["result"]
    assert res["message"] == u"找不到清洗process id: 2_output"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_list(mocker):
    """dataid存在"""

    res = get("/v3/databus/cleans/?raw_data_id=42")
    assert res["result"]
    assert len(res["data"]) == 2
    assert res["data"][0]["processing_id"] == "2_output"
    assert res["data"][0]["raw_data_id"] == 42
    assert res["data"][0]["json_config"] == "test_json_config"
    assert res["data"][0]["status"] == "stopped"
    assert res["data"][0]["status_display"] == u"stopped"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean_factors")
def test_factors():
    """dataid存在"""

    res = get("/v3/databus/cleans/factors/")
    assert res["result"]
    assert res["data"][0]["factor_alias"] == u"Json反序列化"
    assert res["data"][0]["factor_name"] == "from_json"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_time_formats():
    """dataid存在"""

    res = get("/v3/databus/cleans/time_formats/")
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
def test_list_errors():

    res = get("/v3/databus/cleans/list_errors/")
    assert res["result"]
    assert "AccessByIndexFailedError" in res["data"]
    assert "AssignNodeNeededError" in res["data"]
    assert "BadJsonListError" in res["data"]
    assert "NotListDataError" in res["data"]
    assert "TypeConversionError" in res["data"]


@pytest.mark.httpretty
@pytest.mark.django_db
def test_get_processing_id_not_found():
    """业务ID不为数字"""

    res = get("/v3/databus/cleans/99999/")
    assert not res["result"]
    assert res["message"] == u"对象不存在：databus_clean[processing_id=99999]"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_clean", "add_channel")
def test_search_data_id_not_found():
    """dataid不存在"""

    res = get("/v3/databus/cleans/?raw_data_id=99999")
    assert res["result"]


def _clean_success_parpare(mocker):
    inner_channels = model_manager.get_inner_channel_by_priority("kafka")
    assert inner_channels
    mocker.patch(
        "datahub.databus.channel.filter_clusters_by_geog_area",
        return_value=inner_channels[0],
    )
    hp.enable()
    get_inland_tag_ok()
    result_table.get_2_output_ok()
    result_table.get_2_output_storage_ok()
    get_databus_old_ok()
