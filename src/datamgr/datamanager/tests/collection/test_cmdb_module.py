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
from collection.cmdb.datacheck.module_info_check import CMDBModuleInfoChecker
from collection.cmdb.module_info import CMDBModuleInfoCollector
from collection.cmdb.processes.module import process_cmdb_module_info
from tests import BaseTestCase

BIZ_ONE_MODULE_INFO = {
    "creator": "",
    "host_apply_enabled": True,
    "create_time": None,
    "bk_module_name": "LogServer",
    "bk_module_type": "1",
    "bk_db_port": "",
    "last_time": "2021-04-11T01:00:29.192+08:00",
    "bk_svr_port": "",
    "bk_db_charset": "",
    "bk_db_ip": "",
    "bk_set_id": 1122,
    "bk_svr_dns": "",
    "bk_parent_id": 1122,
    "bk_biz_id": 105,
    "bk_module_id": 41841,
    "service_category_id": 2,
    "default": 0,
    "set_template_id": 422,
    "bk_supplier_account": "tencent",
    "service_template_id": 10044,
    "customize": '{"bk_wan_ip_num": 0, "bk_isp": ""}',
}


BIZ_MODULES = {
    "result": True,
    "data": {"count": 1, "info": [BIZ_ONE_MODULE_INFO]},
    "code": "0",
}


MODULE_EVENT = {
    "bk_event_type": "create",
    "bk_resource": "module",
    "bk_detail": {
        "bk_phone_warn_start": None,
        "creator": "",
        "host_apply_enabled": False,
        "create_time": None,
        "bk_module_name": "DEMO机房",
        "operator": None,
        "bk_module_type": "1",
        "bk_db_port": "",
        "last_time": "2021-04-14T23:01:13.336+08:00",
        "bk_svr_port": "",
        "bk_validity": None,
        "bk_wan_need": None,
        "bk_db_charset": "",
        "bk_db_type": None,
        "service_template_id": 55668,
        "bk_phone_warn_end": None,
        "bk_db_ip": "",
        "bk_set_id": 51889,
        "bk_parent_id": 51889,
        "bk_biz_id": 373,
        "bk_need_phone_warn": None,
        "bk_module_id": 893298,
        "service_category_id": 2,
        "default": 0,
        "set_template_id": 3107,
        "bs3_name_id": None,
        "bk_bak_operator": None,
        "bk_is_single_point": None,
        "bk_supplier_account": "tencent",
        "bk_svr_dns": "",
        "bk_machine_type": None,
    },
    "bk_cursor": "abcd",
}


def get_biz_modules_side_effect(params, raise_exception=True, retry_times=10):
    if params["page"]["start"] == 0:
        return DataResponse(BIZ_MODULES)
    else:
        return DataResponse({**BIZ_MODULES, **{"data": {"count": 0, "info": []}}})


def dataflow_model_side_effect(params, raise_exception=True, retry_times=10):
    return DataResponse({"result": True, "data": {"flow_id": 4528376}, "code": "0"})


class TestModuleInfo(BaseTestCase):
    @mock.patch("collection.cmdb.module_info.cmdb_api.search_module")
    @mock.patch("collection.cmdb.module_info.cmdb_api.get_all_business_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_report(self, patch_producer, patch_get_bk_biz_ids, patch_search_module):
        patch_get_bk_biz_ids.return_value = [105, 373]
        patch_search_module.return_value = DataResponse(BIZ_MODULES)

        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        CMDBModuleInfoCollector(config).batch_report()

        assert patch_search_module.call_count == 2
        # assert patch_find_host_topo_relation.call_count == 4
        assert producer_mock.produce_message.call_count == 2

    @mock.patch("collection.common.collect.RawDataProducer")
    def test_handle_event(self, patch_producer):
        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        collector = CMDBModuleInfoCollector(config)

        collector.handle_event(MODULE_EVENT)
        message = json.loads(producer_mock.produce_message.call_args[0][0])
        assert message["event_content"]["bk_module_id"] == 893298
        assert message["event_content"]["bk_module_name"] == "DEMO机房"

    @mock.patch("collection.cmdb.host_info.cmdb_api.search_module")
    @mock.patch("collection.cmdb.datacheck.base.dataquery_api.query_new")
    @mock.patch("collection.cmdb.base.get_bk_biz_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_data_check(self, patch_producer, patch_get_bk_biz_ids, patch_query, patch_search_module):
        patch_get_bk_biz_ids.return_value = [105, 100605]
        patch_search_module.return_value = DataResponse(BIZ_MODULES)

        def query_side_effect(params, raise_exception):
            if "105" in params["sql"]:
                return DataResponse({"result": True, "data": {"list": [{"bk_module_id": 41841}]}})
            else:
                return DataResponse({"result": True, "data": {"list": [{"bk_module_id": 123455}]}})

        patch_query.side_effect = query_side_effect
        collect_cmdb_module_config = {"bk_biz_id": 1, "raw_data_name": "xxxx"}
        checker = CMDBModuleInfoChecker(collect_cmdb_module_config)
        result = checker.check_all_biz()
        assert result["same_count"] == 1
        assert result["diff_count"] == 1

    @mock.patch("api.dataflow_api.flows.start")
    @mock.patch("api.datamanage_api.generate_datamodel_instance")
    def test_process_node_register(self, patch_datamanager_api, patch_flow_api):
        patch_datamanager_api.side_effect = dataflow_model_side_effect
        patch_flow_api.side_effect = dataflow_model_side_effect
        process_cmdb_module_info()
        assert patch_flow_api.call_count == 1
