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

import mock
from rest_framework.test import APITestCase

from dataflow.flow.api_models import ResultTable
from dataflow.flow.controller.node_controller import NodeController
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.nodes.base_node.node_validate import NodeValidate
from dataflow.flow.models import FlowNodeInfo
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class TestNodeController(APITestCase):
    def setUp(self, node_id=12345, flow_id=12345):
        FlowNodeInfo.objects.create(
            node_id=node_id,
            flow_id=flow_id,
            node_name="测试",
            node_config='{"bk_biz_id": 591, "window_type": "none", "counter": null, "count_freq": null, '
            '"name": "f2457_s_03", "window_time": null, "from_result_table_ids": ["591_f2457_m_01"], '
            '"table_name": "f2457_s_03", "sql": "select ip, report_time, gseindex, path,'
            ' \n    log\nfrom 591_f2457_m_01", "session_gap": null, "output_name": "f2457_s_03", '
            '"waiting_time": null}',
            node_type="realtime",
            status="no-start",
            latest_version="V201811221519585600",
            created_by="admin",
            created_at="2018-10-30 20:57:50",
            updated_by="admin",
            updated_at="2018-10-30 20:57:50",
            description="测试使用",
        )

    def test_create_storages_node(self):
        pass

    def test_update_storages_node(self):
        pass

    def test_update_hdfs_physical_storage(self):
        result_table_id = "591_test"
        update_status = "user"
        form_data = {"expires": -1, "cluster": "hdfs"}
        StorekitHelper.get_physical_table = mock.Mock(return_value={"expires": -1, "cluster_name": "hdfs"})
        StorekitHelper.update_physical_table = mock.Mock(return_value={})
        NodeController.update_hdfs_physical_storage(result_table_id, update_status, form_data)
        update_status = "system"
        NodeController.update_hdfs_physical_storage(result_table_id, update_status, form_data)

    def test_create_or_update_hdfs_channel(self):
        pass

    def test_create_kafka_channel_storage(self):
        pass

    def test_create_tdw_channel_storage(self):
        pass

    def test_is_rt_meta_exist(self):
        result_table_id = "591_test"
        ResultTableHelper.get_result_table = mock.Mock(return_value=True)
        result = NodeController.is_rt_meta_exist(result_table_id)
        self.assertTrue(result, True)
        ResultTableHelper.get_result_table = mock.Mock(return_value=False)
        self.assertTrue(result, False)

    def test_delete_rt_system_meta_storage(self):
        result_table_id = "591_test"
        system_cluster_type = "hdfs"
        NodeController.is_rt_meta_exist = mock.Mock(return_value=False)
        NodeController.delete_rt_system_meta_storage(result_table_id, system_cluster_type)
        NodeController.is_rt_meta_exist = mock.Mock(return_value=True)
        o_rt = ResultTable(result_table_id)
        o_rt.has_storage_by_generate_type = mock.Mock(return_value=True)
        NodeController.delete_rt_system_meta_storage(result_table_id, system_cluster_type)

    def test_update_meta_outputs(self):
        pass

    def test_update_meta_hdfs_outputs(self):
        pass

    def test_update_meta_kafka_outputs(self):
        pass

    def test_update_meta_tdw_outputs(self):
        pass

    def test_create_or_update_dp_meta(self):
        parent_node = NODE_FACTORY.get_node_handler(node_id=12345)
        current_node = NODE_FACTORY.get_node_handler(node_id=12345)
        parent_node_rt_id = "591_test"
        NodeInstanceLink.get_link_path_channel = mock.Mock(return_value="hdfs")
        NodeController.update_meta_hdfs_outputs = mock.Mock(return_value=True)
        NodeController.update_meta_kafka_outputs = mock.Mock(return_value=True)
        NodeController.update_meta_tdw_outputs = mock.Mock(return_value=True)
        NodeController.create_or_update_dp_meta(parent_node, current_node, parent_node_rt_id)
        NodeInstanceLink.get_link_path_channel = mock.Mock(return_value="kafka")
        NodeController.create_or_update_dp_meta(parent_node, current_node, parent_node_rt_id)
        NodeInstanceLink.get_link_path_channel = mock.Mock(return_value="tdw")
        NodeController.create_or_update_dp_meta(parent_node, current_node, parent_node_rt_id)

    def test_create_upstream_meta_info(self):
        pass

    def test_delete_upstream_meta_info(self):
        pass

    def test_update_upstream_meta_info(self):
        pass

    def test_create(self):
        node_type = "realtime"
        operator = "admin"
        flow_id = 12345
        from_links = [
            {"source": {"node_id": 4, "id": "ch_1570", "arrow": "Right"}, "target": {"id": "ch_1536", "arrow": "Left"}}
        ]
        config_params = {
            "bk_biz_id": 591,
            "window_type": "none",
            "counter": None,
            "count_freq": None,
            "name": "f2457_s_03",
            "window_time": None,
            "from_result_table_ids": ["591_f2457_m_01"],
            "table_name": "f2457_s_03",
            "sql": "select ip, report_time, gseindex, path, \n    log\nfrom 591_f2457_m_01",
            "session_gap": None,
            "output_name": "f2457_s_03",
            "waiting_time": None,
        }
        frontend_info = None
        NodeValidate.tdw_auth_validate = mock.Mock(return_value=True)
        NodeValidate.from_links_validate = mock.Mock(return_value=True)
        create_node = NODE_FACTORY.get_node_handler_by_type(node_type)
        NODE_FACTORY.get_node_handler_by_type = mock.Mock(return_value=create_node)
        create_node.clean_config = mock.Mock(
            return_value={
                "bk_biz_id": 591,
                "window_type": "none",
                "counter": None,
                "count_freq": None,
                "name": "f2457_s_03",
                "window_time": None,
                "from_result_table_ids": ["591_f2457_m_01"],
                "table_name": "f2457_s_03",
                "sql": "select ip, report_time, gseindex, path, \n    log\nfrom 591_f2457_m_01",
                "session_gap": None,
                "output_name": "f2457_s_03",
                "waiting_time": None,
            }
        )
        NodeValidate.node_count_validate = mock.Mock(return_value=True)
        NodeValidate.node_auth_check = mock.Mock(return_value=True)
        NodeController.create_upstream_meta_info = mock.Mock(return_value=True)
        create_node.add_node_info = mock.Mock(return_value=config_params)
        create_node.add_after = mock.Mock(
            return_value={"result_table_ids": ["591_test"], "heads": ["591_test"], "tails": ["591_test"]}
        )
        create_node.update_node_metadata_relation = mock.Mock(return_value=True)
        NodeController.create(node_type, operator, flow_id, from_links, config_params, frontend_info)

    def test_partial_update(self):
        pass

    def test_update(self):
        # 更新节点
        operator = "admin"
        flow_id = 12345
        node_id = 12345
        from_links = [
            {"source": {"node_id": 4, "id": "ch_1570", "arrow": "Right"}, "target": {"id": "ch_1536", "arrow": "Left"}}
        ]
        request_data = {
            "bk_biz_id": 591,
            "window_type": "none",
            "counter": None,
            "count_freq": None,
            "name": "f2457_s_03",
            "window_time": None,
            "from_result_table_ids": ["591_f2457_m_01"],
            "table_name": "f2457_s_03",
            "sql": "select ip, report_time, gseindex, path, \n    log\nfrom 591_f2457_m_01",
            "session_gap": None,
            "output_name": "f2457_s_03",
            "waiting_time": None,
        }
        frontend_info = None
        is_self_param = False
        is_self_link = False
        update_node = NODE_FACTORY.get_node_handler(node_id=node_id)
        update_node.clean_config = mock.Mock(return_value=request_data)
        NodeValidate.node_auth_check = mock.Mock(return_value=True)
        NodeValidate.from_links_validate = mock.Mock(return_value=True)
        update_node.node_info.create_or_update_link = mock.Mock(return_value=True)
        update_node.update_node_info = mock.Mock(return_value={})
        NodeController.update_upstream_meta_info = mock.Mock()
        update_node.update_after = mock.Mock(
            return_value={"result_table_ids": ["591_test"], "heads": ["591_test"], "tails": ["591_test"]}
        )
        update_node.update_node_metadata_relation = mock.Mock(return_value=True)
        NodeController.update(
            operator, flow_id, node_id, from_links, request_data, frontend_info, is_self_param, is_self_link
        )

    def test_delete(self):
        # 删除节点
        operator = "admin"
        node_id = 12345
        del_node = NODE_FACTORY.get_node_handler(node_id=node_id)
        NODE_FACTORY.get_node_handler = mock.Mock(return_value=del_node)
        FlowHandler.validate_related_source_nodes = mock.Mock(return_value=True)
        del_node.get_to_nodes_handler = mock.Mock(return_value=[])
        NodeController.delete_upstream_meta_info = mock.Mock()
        del_node.remove_before = mock.Mock(return_value=True)
        del_node.remove_node_info = mock.Mock(return_value=True)
        NodeController.delete(operator, node_id)

    def test_get_node_info_dict(self):
        # 获取节点
        node_id = 12345
        node = NODE_FACTORY.get_node_handler(node_id=node_id)
        node.to_dict = mock.Mock(return_value={})

    def test_add_nodes(self):
        pass

    def test_insert_flow_node(self):
        pass

    def test_del_flow_nodes(self):
        pass

    def test_list_monitor_data(self):
        pass
