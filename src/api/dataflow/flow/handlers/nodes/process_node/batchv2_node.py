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

from dataflow.flow.handlers import forms
from dataflow.flow.handlers.form_utils import get_all_fields_from_form
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.process_node.base_batch_node import BaseBatchNode
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper


class BatchV2Node(BaseBatchNode):
    default_storage_type = "hdfs"
    node_type = NodeTypes.BATCHV2
    node_form = forms.UnifiedProcessingNodeForm
    config_attrs = get_all_fields_from_form(node_form)

    def add_after(self, username, from_node_ids, form_data):
        form_data["api_version"] = "v2"
        # valid_start_time(None, form_data, True)
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        self.check_upstream_default_expires(form_data, upstream_nodes_info)
        # 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        # rt_params['processing_id'] = self.build_processing_id(form_data['bk_biz_id'], form_data['table_name'])
        response_data = BatchHelper.create_processingv2(**rt_params)
        self.processing_id = response_data["processing_id"]
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        upstream_system_storage_rts = self.check_upstream_default_expires(form_data, upstream_nodes_info)
        # 校验当前节点的数据窗口长度是否小于下游 HDFS 存储节点的过期时间
        self.check_downstream_expires(form_data)
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        response_data = BatchHelper.update_processingv2(**rt_params)
        # 校验是否需要更新上游离线节点过期时间
        self.update_upstream_default_expires(form_data, upstream_system_storage_rts, username)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(BatchV2Node, self).remove_before(username)
        # 删除数据修正，如果有
        self.del_data_correct()
        self.delete_dp_with_rollback(BatchHelper.delete_processingv2)

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(BatchV2Node, self).build_before(from_node_ids, form_data, is_create)
        # 数据修正逻辑，更新节点
        if not is_create:
            form_data = self.create_or_update_data_correct(form_data)
        # from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        # from_batch_nodes = NodeUtils.filter_batch(from_nodes)
        # validate_batch_count_freq_and_delay(
        #     CountFreq(form_data['dedicated_config']['schedule_config']['count_freq'],
        #               form_data['dedicated_config']['schedule_config']['schedule_period']),
        #     from_nodes)
        return form_data

    def build_rt_params(self, form_data, from_node_ids, username):
        """
        生成 RT 参数
        """
        rt_dict = form_data
        # 增加通用参数
        rt_dict["project_id"] = self.project_id
        rt_dict["tags"] = self.geog_area_codes
        rt_dict["api_version"] = "v2"
        rt_dict["bk_username"] = username
        rt_dict["dedicated_config"]["batch_type"] = "batch_sql_v2"

        # 窗口参数中，增加是否为静态表的参数
        static_rt_ids = []
        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        for _from_node in from_nodes:
            # 取出父表
            _table_ids = NodeUtils.get_from_result_table_ids(
                self.node_type,
                self.get_config(False),
                specified_from_node_id=_from_node.node_id,
            )
            if _from_node.node_type in NodeTypes.BATCH_KV_SOURCE_CATEGORY:
                static_rt_ids.extend(_table_ids)
                continue
        modified_window_info = []
        for one_window_info in rt_dict["window_info"]:
            if one_window_info["result_table_id"] in static_rt_ids:
                one_window_info["is_static"] = True
            modified_window_info.append(one_window_info)
        rt_dict["window_info"] = modified_window_info

        # 增加数据修正逻辑, 替换sql
        is_open_correct = None
        correct_config_id = None
        if "is_open_correct" in form_data["dedicated_config"]:
            is_open_correct = form_data["dedicated_config"]["is_open_correct"]
        if "correct_config_id" in form_data["dedicated_config"]:
            correct_config_id = form_data["dedicated_config"]["correct_config_id"]
        if is_open_correct and correct_config_id:
            correct_data = DatamanageHelper.get_data_correct({"correct_config_id": correct_config_id})
            sql = correct_data["correct_sql"]
            form_data["dedicated_config"]["sql"] = sql
        return rt_dict
