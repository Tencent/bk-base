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

import mock

from api.base import DataResponse
from collection.cmdb.datacheck.set_info_check import CMDBSetInfoChecker
from collection.cmdb.set_info import CMDBSetInfoCollector
from tests import BaseTestCase

BIZ_ONE_SET_INFO = {
    "bk_service_status": "1",
    "creator": "",
    "bk_open_time": None,
    "大区ID": "",
    "bk_platform": "",
    "bk_customer": "",
    "bk_is_gcs": False,
    "bk_alias_name": "",
    "worldID": "75",
    "序号ID": "",
    "openstate": "1",
    "TclsWorldID": "",
    "IDC": "",
    "bk_set_env": "3",
    "bk_enable_relate_webplat": "0",
    "bk_outer_source": 0,
    "bk_capacity": 20000,
    "bk_operation_state": "",
    "last_time": "2021-04-13T12:29:00.989+08:00",
    "bk_world_id": "75",
    "isOpen": "1",
    "群组容量": "",
    "bk_category": "1",
    "ICE": "",
    "是否合服": "否",
    "中文名称": "DEMO王城",
    "bk_set_name": "75",
    "bk_parent_id": 113,
    "数据仓库": "",
    "bk_biz_id": 113,
    "bk_uniq_id": "",
    "bk_svc_name": "DEMO王城",
    "default": 0,
    "bk_chn_name": "DEMO王城",
    "set_template_id": 334,
    "网络类型": "",
    "NBOptime": "",
    "create_time": None,
    "bk_supplier_account": "tencent",
    "bk_set_id": 18292,
    "bk_set_desc": "",
    "群组ID": "",
    "bk_system": "",
}


BIZ_SETS = {
    "result": True,
    "data": {"count": 1, "info": [BIZ_ONE_SET_INFO]},
    "code": "0",
}


SET_EVENT = {
    "bk_event_type": "update",
    "bk_resource": "set",
    "bk_detail": {
        "bk_service_status": "1",
        "creator": "",
        "bk_open_time": None,
        "大区ID": "",
        "bk_platform": "",
        "bk_customer": "",
        "bk_is_gcs": False,
        "bk_alias_name": "",
        "worldID": "1006",
        "TclsWorldID": "",
        "IDC": "",
        "bk_set_env": "3",
        "bk_enable_relate_webplat": "0",
        "bk_outer_source": 0,
        "bk_capacity": 0,
        "bk_svc_name": "iwx06",
        "last_time": "2020-12-19T20:59:42.645+08:00",
        "bk_operation_state": "",
        "bk_world_id": "1006",
        "bk_category": "1",
        "中文名称": "AA球馆",
        "bk_set_name": "1006",
        "bk_parent_id": 853,
        "bk_biz_id": 853,
        "bk_uniq_id": "",
        "default": 0,
        "bk_chn_name": "AA球馆",
        "set_template_id": 9979,
        "create_time": None,
        "bk_supplier_account": "tencent",
        "bk_set_id": 53958,
        "bk_set_desc": "",
        "bk_system": "",
    },
    "bk_cursor": "aaaa",
}


def get_biz_sets_side_effect(params, raise_exception=True, retry_times=10):
    if params["page"]["start"] == 0:
        return DataResponse(BIZ_SETS)
    else:
        return DataResponse({**BIZ_SETS, **{"data": {"count": 0, "info": []}}})


def dataflow_model_side_effect(params, raise_exception=True, retry_times=10):
    return DataResponse({"result": True, "data": {"flow_id": 4528376}, "code": "0"})


class TestSetInfo(BaseTestCase):
    @mock.patch("collection.cmdb.set_info.cmdb_api.search_set")
    @mock.patch("collection.cmdb.set_info.cmdb_api.get_all_business_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_report(self, patch_producer, patch_get_bk_biz_ids, patch_search_set):
        patch_get_bk_biz_ids.return_value = [113, 853]
        patch_search_set.return_value = DataResponse(BIZ_SETS)

        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        CMDBSetInfoCollector(config).batch_report()

        assert patch_search_set.call_count == 2
        assert producer_mock.produce_message.call_count == 2

    @mock.patch("collection.common.collect.RawDataProducer")
    def test_handle_event(self, patch_producer):
        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        collector = CMDBSetInfoCollector(config)

        collector.handle_event(SET_EVENT)
        message = json.loads(producer_mock.produce_message.call_args[0][0])
        assert message["event_content"]["bk_set_id"] == 53958
        assert message["event_content"]["bk_set_name"] == "1006"

    @mock.patch("collection.cmdb.set_info.cmdb_api.search_set")
    @mock.patch("collection.cmdb.datacheck.base.dataquery_api.query_new")
    @mock.patch("collection.cmdb.base.get_bk_biz_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_data_check(self, patch_producer, patch_get_bk_biz_ids, patch_query, patch_search_set):
        patch_get_bk_biz_ids.return_value = [113, 853]
        patch_search_set.return_value = DataResponse(BIZ_SETS)

        def query_side_effect(params, raise_exception):
            if "113" in params["sql"]:
                return DataResponse({"result": True, "data": {"list": [{"bk_set_id": 18292}]}})
            else:
                return DataResponse({"result": True, "data": {"list": [{"bk_set_id": 123455}]}})

        patch_query.side_effect = query_side_effect
        collect_cmdb_host_config = {"bk_biz_id": 1, "raw_data_name": "xxxx"}
        checker = CMDBSetInfoChecker(collect_cmdb_host_config)
        result = checker.check_all_biz()
        assert result["same_count"] == 1
        assert result["diff_count"] == 1
