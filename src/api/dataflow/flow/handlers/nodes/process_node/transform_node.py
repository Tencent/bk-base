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

from django.utils.translation import ugettext as _

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import ValidError
from dataflow.flow.handlers.nodes.base_node.base_processing_node import ProcessingNode
from dataflow.flow.handlers.nodes.base_node.node_handler import call_func_with_rollback
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.databus.databus_helper import DatabusHelper


class TransformNode(ProcessingNode):
    default_storage_type = "hdfs"
    channel_storage_type = "kafka"

    node_type = None
    node_form = None
    config_attrs = [
        "bk_biz_id",
        "name",
        "table_name",
        "output_name",
        "from_result_table_ids",
        "description",
        "config",
    ]

    def add_after(self, username, from_node_ids, form_data):
        # 1. 校验上游schema是否合法
        self.valid_transform_schema(form_data["from_result_table_ids"])
        # 2. 创建固化节点(接口调用同时创建processing)
        rt_params = self.build_rt_params(form_data)
        rt_params["processing_id"] = self.build_processing_id(form_data["bk_biz_id"], form_data["table_name"])
        response_data = DatabusHelper.create_datanode(**rt_params)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        # 校验上游schema是否合法
        self.valid_transform_schema(form_data["from_result_table_ids"])
        # 更新固化节点(接口调用同时更新processing)
        rt_params = self.build_rt_params(form_data)
        rt_params.update({"processing_id": self.processing_id})

        result_table_ids = []
        for output in rt_params["outputs"]:
            result_table_ids.append("{}_{}".format(output["bk_biz_id"], output["table_name"]))
        refresh_result_ret = self.refresh_result_table_storages(result_table_ids)
        rt_params["delete_result_tables"] = refresh_result_ret["result_table_ids_to_delete"]
        rollback_func_info_list = refresh_result_ret["rollback_func_info_list"]
        downstream_link_to_delete = refresh_result_ret["downstream_link_to_delete"]
        # 3. 更新 processing
        response_data = call_func_with_rollback(DatabusHelper.update_datanode, rt_params, rollback_func_info_list)
        # 移除与之相关的下游节点连线
        for to_node_id in downstream_link_to_delete:
            self.flow.delete_line(self.node_id, to_node_id)

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(TransformNode, self).remove_before(username)
        self.delete_multi_dp_with_rollback(
            DatabusHelper.delete_datanode,
            result_table_ids=self.result_table_ids,
            recycle_system_storage=False,
        )

    def valid_transform_schema(self, from_result_table_ids):
        """
        校验来自父节点RT列表是否具有一致 SCHEMA
        """
        if len(from_result_table_ids) < 2:
            return True

        o_rts = [ResultTable(result_table_id=rt_id) for rt_id in from_result_table_ids]

        # 将各个RT的所有字段列表进行排序
        fields_arr = [sorted(_o_rt.fields, key=lambda _f: _f["field_name"]) for _o_rt in o_rts]

        base_len = len(fields_arr[0])
        for fields in fields_arr:
            if len(fields) != base_len:
                raise ValidError(_("父表字段列表长度不一致"))

        # rt1 = [{field1}, {field2}], rt2 = [{field3}, {field4}]
        # zip 后，使用 [({field1}, {field3}), ({field2}, {field4})] 按照位置进行比较
        cols_fields = list(zip(*fields_arr))
        for cols in cols_fields:
            _base_name = cols[0]["field_name"]
            _base_type = cols[0]["field_type"]
            for _f in cols:
                if _f["field_name"] != _base_name or _f["field_type"] != _base_type:
                    raise ValidError(_("父表间存在一个不一致的字段（{}）".format(_f["field_name"])))

    def build_rt_params(self, form_data):
        """
        生成 RT 参数
        """
        rt_dict = self.clean_config(form_data, False)
        # 保持格式一致，默认为空dict
        config = json.dumps({})
        outputs = [{"bk_biz_id": rt_dict["bk_biz_id"], "table_name": rt_dict["table_name"]}]
        if self.node_type == NodeTypes.SPLIT_KAFKA:
            config = json.dumps({"split_logic": rt_dict["config"]})
            outputs = list(
                map(
                    lambda _output: {
                        "bk_biz_id": _output["bk_biz_id"],
                        "table_name": rt_dict["table_name"],
                    },
                    rt_dict["config"],
                )
            )
        rt_params = {
            "project_id": self.flow.project_id,
            "bk_biz_id": rt_dict["bk_biz_id"],
            "node_type": self.node_type,
            "result_table_name": rt_dict["table_name"],
            "result_table_name_alias": rt_dict["output_name"],
            "source_result_table_ids": ",".join(form_data["from_result_table_ids"]),
            "description": rt_dict["description"],
            "config": config,
            "outputs": outputs,
        }
        return rt_params

    @property
    def processing_type(self):
        return "transform"
