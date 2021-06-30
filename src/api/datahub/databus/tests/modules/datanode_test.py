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
from common.exceptions import DataAlreadyExistError
from datahub.databus.constants import NodeType
from django.db import IntegrityError

from datahub.databus import datanode


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_add_transform_exception(mocker):
    params = {
        "node_type": "split",
        "description": "test",
        "bk_biz_id": 1000,
        "project_id": 25,
        "result_table_name": "test_split",
        "result_table_name_alias": u"测试split",
        "source_result_table_ids": "1_xxx",
        "config": '{"split_logic":[{"bk_biz_id":1,"logic_exp":"field2==\\"string 1\\""},{"bk_biz_id":2,"logic_exp":'
        '"field2==\\"string 2\\""}]}',
    }

    mocker.patch(
        "datahub.databus.models.TransformProcessing.objects.create",
        side_effect=IntegrityError,
    )

    try:
        datanode.add_transform_processiong(params)
    except DataAlreadyExistError:
        pass


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_split_transform")
def test_delete_split_datanode_success(mocker):

    mocker.patch(
        "datahub.databus.api.MetaApi.data_processings.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DataResponse({"result": True}),
    )

    res = datanode.delete_datanode_config("101_xxx_test", True)

    assert res


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_filter_transform")
def test_delete_other_type_datanode(mocker):

    mocker.patch(
        "datahub.databus.api.MetaApi.result_tables.delete",
        return_value=DataResponse({"result": True}),
    )

    mocker.patch(
        "datahub.databus.api.StoreKitApi.result_tables.delete",
        return_value=DataResponse({"result": True}),
    )

    res = datanode.delete_datanode_config("102_xxx_test", True)

    assert not res


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.skip
def test_add_storages():
    node_dict = {
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
    bid_group = []
    res = datanode.add_storages(node_dict, bid_group)
    assert not res


@pytest.mark.httpretty
@pytest.mark.django_db
def test_delete_storages():
    node_dict = {
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
    bid_group = []
    res = datanode.delete_storages(node_dict, bid_group)
    assert not res


@pytest.mark.httpretty
@pytest.mark.django_db
def test_add_data_processing_other_node():
    node_dict = {
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

    res = datanode.add_data_processing(node_dict, {}, None, NodeType.FILTER, {})

    assert not res


@pytest.mark.httpretty
@pytest.mark.django_db
def test_add_data_processing_failed():
    res = datanode.add_data_processing(None, {}, None, NodeType.SPLIT, {})
    assert not res
