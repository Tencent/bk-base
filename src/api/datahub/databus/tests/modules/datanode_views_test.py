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

import json

import pytest
from common.api.base import DataResponse
from datahub.access.tests.mock_api.cc import get_tag_target_ok  # noqa
from datahub.access.tests.utils import delete, patch, post
from datahub.databus.models import DatabusChannel, TransformProcessing  # noqa
from datahub.databus.tests.fixture.datanode_fixture import add_split_transform  # noqa
from datahub.databus.tests.fixture.datanode_fixture import add_transform  # noqa

rt_response = {
    "result": True,
    "data": {
        "tags": {"manage": {"geog_area": [{"code": "inland"}]}},
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
    },
}

rt_response2 = {
    "result": True,
    "data": {
        "storages": [{"kafka": {"storage_channel": "test_channel"}}],
        "tags": {"manage": {"geog_area": [{"code": "inland"}]}},
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
    },
}


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_success(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    res = post("/v3/databus/datanodes/", params)
    assert res["result"]
    compare_json = {
        "message": "ok",
        "errors": None,
        "code": "1500200",
        "data": {
            "heads": ["1000_test_merge"],
            "result_table_ids": ["1000_test_merge"],
            "tails": ["1000_test_merge"],
        },
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_0(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": False, "message": "DbError"}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570012"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_1(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570012"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_2(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570029"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_3(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1500002"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_4(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("datahub.databus.channel.get_inner_channel_to_use", return_value=None)

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570013"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_failed_5(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("datahub.databus.channel.get_inner_channel_to_use", return_value=None)

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570013"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_not_valid():
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1",
        "config": "test",
    }

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]

    compare_json = {
        "message": u"固化节点配置参数校验失败,merge节点的源rt数要大于等于2，小于10。当前rt数:1",
        "errors": None,
        "code": "1570021",
        "data": None,
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_success(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1, "logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2, "logic_exp"'
        ':"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    res = post("/v3/databus/datanodes/", params)

    assert res["result"]
    compare_json = {
        "message": "ok",
        "errors": None,
        "code": "1500200",
        "data": {
            "heads": ["1000_test_split"],
            "result_table_ids": ["1_test_split", "2_test_split", "1000_test_split"],
            "tails": ["1_test_split", "2_test_split"],
        },
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_0(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,'
        '"logic_exp":"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": False}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570012"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_1(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )
    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value="OK",
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )
    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1500002"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_2(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1500002"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_3(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        side_effect=[
            DataResponse({"result": True}),
            DataResponse({"result": False}),
            DataResponse({"result": False}),
        ],
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570012"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_4(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        side_effect=[
            DataResponse({"result": True}),
            DataResponse({"result": False}),
            DataResponse({"result": False}),
        ],
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": False}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570012"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_failed_5(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("datahub.databus.channel.get_inner_channel_to_use", return_value=None)

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        side_effect=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    assert res["code"] == "1570013"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_split_not_valid():
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]

    compare_json = {
        "message": u"固化节点配置参数校验失败,split节点的源rt数要等于1。当前rt数:2",
        "errors": None,
        "code": "1570021",
        "data": None,
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_other_type():
    params = {
        "node_type": "filter",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "101_xxx_test",
        "config": "",
    }

    res = post("/v3/databus/datanodes/", params)

    assert not res["result"]
    compare_json = {
        "message": u"以下固化节点暂未支持,节点类型:%s" % params["node_type"],
        "errors": None,
        "code": "1570023",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_merge_success_2(mocker):
    params = {
        "node_type": "merge",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_test_1,rt_test_2",
        "config": "test",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )

    mocker.patch(
        "datahub.databus.rt.get_channel_id_for_rt_list",
        return_value={"rt_test_1": 5, "rt_test_2": 8},
    )

    res = post("/v3/databus/datanodes/", params)

    assert res["result"]
    assert res["data"]["result_table_ids"] == ["1000_test_merge"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_transform")
def test_destroy_success(mocker):
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    res = delete("/v3/databus/datanodes/100_xxx_test/")
    assert res["result"]
    assert res["data"]


@pytest.mark.httpretty
@pytest.mark.django_db
def test_destroy_error(mocker):

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    res = delete("/v3/databus/datanodes/100_xxx_test/")

    assert not res["result"]
    assert res["code"] == "1570028"
    assert res["message"] == u"固化节点删除失败,节点id:100_xxx_test！"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_transform")
def test_partition_update_merge_success(mocker):
    param = {
        "node_type": "merge",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1,rt_2",
        "config": "test_config",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    res = patch("/v3/databus/datanodes/100_xxx_test/", param)

    assert res["result"]
    assert res["data"]


@pytest.mark.httpretty
@pytest.mark.django_db
def test_partition_update_not_valid():
    params = {
        "node_type": "merge",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1",
        "config": "test_config",
    }

    res = patch("/v3/databus/datanodes/100_xxx_test/", params)

    assert not res["result"]

    compare_json = {
        "message": u"固化节点配置参数校验失败,merge节点的源rt数要大于等于2，小于10。当前rt数:1",
        "errors": None,
        "code": "1570021",
        "data": None,
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_transform")
def test_partition_update_merge_failed0(mocker):
    param = {
        "node_type": "merge",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1,rt_2",
        "config": "test_config",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": False}),
    )

    res = patch("/v3/databus/datanodes/100_xxx_test/", param)
    assert not res["result"]
    assert res["code"] == "1570011"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_transform")
def test_partition_update_merge_failed1(mocker):
    param = {
        "node_type": "merge",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1,rt_2",
        "config": "test_config",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": False}),
    )

    res = patch("/v3/databus/datanodes/100_xxx_test/", param)

    assert not res["result"]

    assert res["code"] == "1570066"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_partition_update_merge_error(mocker):
    param = {
        "node_type": "merge",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1,rt_2",
        "config": "test_config",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = patch("/v3/databus/datanodes/100_xxx_test/", param)

    assert not res["result"]
    assert res["code"] == "1570025"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_partition_update_other_type(mocker):
    params = {
        "node_type": "filter",
        "description": "test222",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_merge",
        "result_table_name_alias": u"测试merge",
        "source_result_table_ids": "rt_1,rt_2",
        "config": "test_config",
    }

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = patch("/v3/databus/datanodes/100_xxx_test/", params)

    assert not res["result"]
    compare_json = {
        "message": u"以下固化节点暂未支持,节点类型:%s" % params["node_type"],
        "errors": None,
        "code": "1570023",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_success(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":2,"logic_exp":"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert res["result"]
    assert res["data"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_transform")
def test_partition_update_split_error():
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1,rt_2",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":2,"logic_exp":"field2==\\"string 2\\""}]}',
    }

    res = patch("/v3/databus/datanodes/100_xxx_test/", params)

    assert not res["result"]

    compare_json = {
        "message": u"固化节点配置参数校验失败,split节点的源rt数要等于1。当前rt数:2",
        "errors": None,
        "code": "1570021",
        "data": None,
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_1(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )
    mocker.patch(
        "datahub.databus.datanode.add_storages",
        return_value=True,
    )
    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
    )
    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert res["result"]

    compare_json = {
        "message": "ok",
        "errors": None,
        "code": "1500200",
        "data": {
            "heads": ["101_xxx_test"],
            "result_table_ids": [
                "1_test_split",
                "4_test_split",
                "3_test_split",
                "101_xxx_test",
            ],
            "tails": ["1_test_split", "4_test_split", "3_test_split"],
        },
        "result": True,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed1(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":2,"logic_exp":"field2==\\"string 2\\""}]}',
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.update",
        return_value=DataResponse({"result": True}),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert not res["result"]
    assert res["code"] == "1500002"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed2(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
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
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    mocker.patch("datahub.databus.datanode.add_storages", return_value=False)

    mocker.patch("datahub.databus.datanode.delete_storages", return_value=True)

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert not res["result"]
    assert res["message"] == u"固化节点更新失败,节点id:101_xxx_test！"
    compare_json = {
        "message": u"固化节点更新失败,节点id:101_xxx_test！",
        "errors": None,
        "code": "1570027",
        "data": None,
        "result": False,
    }

    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed3(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": False}),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert not res["result"]
    assert res["code"] == "1570066"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed4(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.channel.get_inner_channel_to_use",
        return_value=DatabusChannel(id=1, priority=1),
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
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    mocker.patch("datahub.databus.datanode.add_storages", return_value=True)

    mocker.patch("datahub.databus.datanode.delete_storages", return_value=False)

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)
    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed5(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response2),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.create",
        return_value=DataResponse({"result": True}),
    )
    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.retrieve",
        return_value=DataResponse(rt_response),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.update",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch("datahub.databus.channel.get_inner_channel_to_use", return_value=None)
    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.create",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DatabusChannel({"result": True}),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert not res["result"]
    assert res["code"] == "1570013"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_partition_update_split_failed6(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "rt_1",
        "config": '{"split_logic":['
        '{"bk_biz_id":5,"logic_exp":"field2==\\"string 5\\""},'
        '{"bk_biz_id":4,"logic_exp":"field2==\\"string 4\\""}, '
        '{"bk_biz_id":3,"logic_exp":"field2==\\"string 3\\""}'
        "]}",
    }
    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(rt_response),
    )

    res = patch("/v3/databus/datanodes/101_xxx_test/", params)

    assert not res["result"]
    assert res["code"] == "1500002"
