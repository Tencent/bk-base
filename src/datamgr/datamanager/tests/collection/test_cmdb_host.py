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
from collection.cmdb.datacheck.host_info_check import CMDBHostInfoChecker
from collection.cmdb.host_info import CMDBHostInstCollector
from collection.cmdb.processes.host import process_cmdb_host_info
from tests import BaseTestCase

BIZ_ONE_HOST_INFO = {
    "bk_cpu": None,
    "bk_isp_name": None,
    "domain": "",
    "net_struct_id": "",
    "bk_os_name": "",
    "srv_out_band_manage_type": "",
    "bk_province_name": None,
    "bk_host_id": 2000000007,
    "import_from": "1",
    "bk_os_version": "",
    "group_name": "",
    "bk_disk": None,
    "operator": "admin",
    "vip": "",
    "create_time": "2020-03-03T15:01:04.395+08:00",
    "bk_mem": None,
    "svr_device_class": "",
    "bk_host_name": "host01",
    "sub_zone_id": None,
    "net_device_id": "",
    "last_time": "2021-03-30T15:16:42.002+08:00",
}

BIZ_ONE_HOST_RELATIONS = [
    {
        "bk_biz_id": 100605,
        "bk_module_id": 526761111,
        "bk_supplier_account": "tencent",
        "bk_host_id": 2000000007,
        "bk_set_id": 76522,
    },
    {
        "bk_biz_id": 100605,
        "bk_module_id": 526761222,
        "bk_supplier_account": "tencent",
        "bk_host_id": 2000000007,
        "bk_set_id": 76522,
    },
]

BIZ_HOSTS = {
    "result": True,
    "data": {"count": 1, "info": [BIZ_ONE_HOST_INFO]},
    "code": "0",
}

BIZ_HOST_RELATIONS = {
    "result": True,
    "data": {
        "count": 1,
        "data": [
            *BIZ_ONE_HOST_RELATIONS,
            {
                "bk_biz_id": 100605,
                "bk_module_id": 526761,
                "bk_supplier_account": "tencent",
                "bk_host_id": 824,
                "bk_set_id": 76522,
            },
        ],
    },
    "code": "0",
}

HOST_EVENT = {
    "bk_event_type": "update",
    "bk_resource": "host",
    "bk_detail": {
        "bk_manage_type": None,
        "import_from": "1",
        "inner_switch_port": "",
        "bk_svc_id_arr": None,
        "bk_host_name": "stage-bk-3",
        "last_time": "2021-03-30T17:48:55.556+08:00",
        "bk_relations": None,
        "bk_comment": "",
        "bk_logic_zone": None,
        "net_struct_name": "",
        "bk_biz_id": None,
        "bk_asset_id": "",
        "clb_vip": None,
        "bk_outer_equip_id": None,
        "bk_svr_type_id": None,
        "bk_host_outerip": "0.0.0.0",
        "idc_unit_name": "",
        "svr_device_id": "",
        "idc_unit_id": None,
        "bk_svr_device_cls_name": None,
        "bk_outer_switch_ip": None,
        "module_name": "",
        "rack": "",
        "bk_mem": 31843,
        "bk_isp_name": None,
        "domain": "",
        "bk_os_name": "linux centos",
        "srv_out_band_manage_type": "",
        "bk_str_version": None,
        "create_time": "2020-02-14T13:07:07.332+08:00",
        "hard_memo": "",
        "bk_idc_area_id": None,
        "sub_zone_id": None,
        "sub_zone": "",
        "bk_inner_net_idc": None,
        "bk_bs_info": None,
        "svr_type_name": "",
        "bk_os_bit": "64-bit",
        "idc_id": None,
        "bk_logic_zone_id": None,
        "bk_cloud_inst_id": None,
        "svr_first_time": None,
        "bk_cloud_id": 0,
        "svr_id": None,
        "bk_cpu_mhz": None,
        "idc_city_name": None,
        "bk_inner_switch_ip": None,
        "bk_mac": "abcd",
        "srv_important_level": None,
        "dept_name": "",
        "svr_input_time": None,
        "bk_supplier_account": "tencent",
        "bk_cpu": 8,
        "net_struct_id": "",
        "bk_province_name": None,
        "svr_device_type_id": None,
        "bk_os_version": "7.2",
        "group_name": "",
        "bk_disk": 294,
        "svr_device_type_name": None,
        "outer_network_segment": "",
        "operator": "admin",
        "bk_host_innerip": "0.0.0.0",
        "raid_name": "",
        "dbrole": None,
        "bk_outer_mac": "",
        "rack_id": None,
        "接入iFix": None,
        "raid_id": None,
        "logic_domain": "",
        "bk_sla": None,
        "bk_cloud_vendor": None,
        "bk_cloud_host_status": None,
        "bk_os_type": "1",
        "logic_domain_id": None,
        "svr_device_class": "",
        "svr_out_band_type": "",
        "net_device_id": "",
        "bk_bak_operator": "admin",
        "bk_cpu_module": "Intel(R) Xeon(R) Gold 6133 CPU @ 2.50GHz",
        "bk_host_id": 2000000007,
        "bk_idc_area": None,
        "bk_product": None,
        "bk_ip_oper_name": None,
        "srv_status": "",
        "bk_position_name": None,
        "bk_is_virtual": None,
        "bk_service_arr": None,
        "classify_level_name": None,
        "outer_switch_port": "",
        "idc_name": None,
        "bk_sn": "",
        "inner_network_segment": "",
        "is_special": "",
        "bk_inner_equip_id": None,
        "idc_city_id": None,
        "bk_zone_name": None,
        "bk_state": None,
        "svr_type_id": None,
        "bk_state_name": None,
    },
    "bk_cursor": "aaaa",
}

HOST_RELATION_EVENT = {
    "bk_event_type": "update",
    "bk_resource": "host",
    "bk_detail": {"bk_host_id": 2000000007},
    "bk_cursor": "aaaa",
}


def get_biz_hosts_side_effect(params, raise_exception=True, retry_times=10):
    if params["page"]["start"] == 0:
        return DataResponse(BIZ_HOSTS)
    else:
        return DataResponse({**BIZ_HOSTS, **{"data": {"count": 0, "info": []}}})


def get_biz_relations_side_effect(params, raise_exception=True, retry_times=10):
    if params["page"]["start"] == 0:
        return DataResponse(BIZ_HOST_RELATIONS)
    else:
        return DataResponse({**BIZ_HOST_RELATIONS, **{"data": {"count": 0, "data": []}}})


def dataflow_model_side_effect(params, raise_exception=True, retry_times=10):
    return DataResponse({"result": True, "data": {"flow_id": 4528376}, "code": "0"})


def raw_data_lists_side_effect(params, raise_exception=True, retry_times=10):
    return DataResponse(
        {
            "errors": None,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "maintainer": "admin",
                    "sensitivity": "private",
                    "topic_name": "bkpub_cmdb_biz591",
                    "updated_at": "2021-05-08 15:16:32",
                    "raw_data_alias": "CMDB 业务信息",
                    "topic": "bkpub_cmdb_biz591",
                    "bk_biz_name": "DEMO 业务",
                    "id": 525059,
                    "description": "CMDB 业务信息",
                    "data_scenario": "custom",
                    "storage_partitions": 1,
                    "created_by": "admin",
                    "updated_by": "admin",
                    "permission": "",
                    "raw_data_name": "bkpub_cmdb_biz",
                    "storage_channel_id": 11,
                    "active": 1,
                    "bk_biz_id": 591,
                    "data_source": "",
                    "created_at": "2021-05-08 15:16:32",
                    "data_encoding": "UTF-8",
                    "bk_app_code": "data",
                    "data_category": None,
                }
            ],
            "result": True,
        }
    )


class TestHostInfo(BaseTestCase):
    @mock.patch("collection.cmdb.host_info.cmdb_api.find_host_topo_relation")
    @mock.patch("collection.cmdb.host_info.cmdb_api.list_biz_hosts")
    @mock.patch("collection.cmdb.base.get_bk_biz_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_report(
        self,
        patch_producer,
        patch_get_bk_biz_ids,
        patch_list_biz_hosts,
        patch_find_host_topo_relation,
    ):
        patch_get_bk_biz_ids.return_value = [2005000002, 100605]
        patch_list_biz_hosts.side_effect = get_biz_hosts_side_effect
        patch_find_host_topo_relation.side_effect = get_biz_relations_side_effect

        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        CMDBHostInstCollector(config).batch_report()

        assert patch_list_biz_hosts.call_count == 4
        assert patch_find_host_topo_relation.call_count == 4
        assert producer_mock.produce_message.call_count == 2

    @mock.patch("collection.cmdb.host_info.cmdb_api.get_host_base_info")
    @mock.patch("collection.cmdb.host_info.cmdb_api.find_host_biz_relation")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_handle_event(self, patch_producer, patch_find_host_biz_relation, patch_get_host_base_info):
        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock
        patch_find_host_biz_relation.return_value = DataResponse({"result": True, "data": BIZ_ONE_HOST_RELATIONS})
        patch_get_host_base_info.return_value = DataResponse(
            {
                "result": True,
                "data": [
                    {
                        "bk_property_name": key,
                        "bk_property_id": key,
                        "bk_property_value": value,
                    }
                    for key, value in BIZ_ONE_HOST_INFO.items()
                ],
            }
        )

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        collector = CMDBHostInstCollector(config)

        collector.handle_host_event(HOST_EVENT)
        message = json.loads(producer_mock.produce_message.call_args[0][0])
        assert message["event_content"]["bk_host_id"] == 2000000007
        assert message["event_content"]["bk_host_name"] == "stage-bk-3"
        assert type(message["event_content"]["bk_relations"]) == str

        collector.handle_relation_event(HOST_RELATION_EVENT)
        message = json.loads(producer_mock.produce_message.call_args[0][0])
        assert message["event_content"]["bk_host_id"] == 2000000007
        assert message["event_content"]["bk_host_name"] == "host01"
        assert type(message["event_content"]["bk_relations"]) == str

    @mock.patch("collection.cmdb.host_info.cmdb_api.list_biz_hosts")
    @mock.patch("collection.cmdb.datacheck.base.dataquery_api.query_new")
    @mock.patch("collection.cmdb.base.get_bk_biz_ids")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_data_check(self, patch_producer, patch_get_bk_biz_ids, patch_query, patch_list_biz_hosts):
        patch_get_bk_biz_ids.return_value = [2005000002, 100605]
        patch_list_biz_hosts.side_effect = get_biz_hosts_side_effect

        def query_side_effect(params, raise_exception):
            if "2005000002" in params["sql"]:
                return DataResponse({"result": True, "data": {"list": [{"bk_host_id": 2000000007}]}})
            else:
                return DataResponse({"result": True, "data": {"list": [{"bk_host_id": 123455}]}})

        patch_query.side_effect = query_side_effect
        collect_cmdb_host_config = {"bk_biz_id": 1, "raw_data_name": "xxxx"}
        checker = CMDBHostInfoChecker(collect_cmdb_host_config)
        result = checker.check_all_biz()
        assert result["same_count"] == 1
        assert result["diff_count"] == 1

    @mock.patch("api.dataflow_api.flows.start")
    @mock.patch("api.datamanage_api.generate_datamodel_instance")
    def test_process_node_register(self, patch_datamanager_api, patch_flow_api):
        patch_datamanager_api.side_effect = dataflow_model_side_effect
        patch_flow_api.side_effect = dataflow_model_side_effect
        process_cmdb_host_info()
        assert patch_flow_api.call_count == 1
