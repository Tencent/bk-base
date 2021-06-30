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

from dataflow.flow.handlers import forms
from dataflow.flow.handlers.nodes.base_node.base_storage_node import StorageNode
from dataflow.flow.node_types import NodeTypes


class IgniteStorageNode(StorageNode):
    node_type = NodeTypes.IGNITE_STORAGE
    node_form = forms.IgniteStorageForm
    config_attrs = [
        "name",
        "bk_biz_id",
        "cluster",
        "expires",
        "expires_unit",
        "result_table_id",
        "from_nodes",
        "storage_type",
        "indexed_fields",
        "storage_keys",
        "max_records",
    ]
    storage_type = "ignite"

    def build_rt_storage_config(self, result_table_id, form_data, username):
        # 单位为空补 d
        if not form_data["expires_unit"]:
            form_data["expires_unit"] = "d"
        return {
            "result_table_id": result_table_id,
            "cluster_name": form_data["cluster"],
            "cluster_type": self.storage_type,
            "expires": str(form_data["expires"]) + str(form_data["expires_unit"]),
            "storage_config": json.dumps(
                {
                    "indexed_fields": form_data["indexed_fields"],
                    "storage_keys": form_data["storage_keys"],
                    "scenario_type": form_data["storage_type"],
                    "max_records": form_data["max_records"],
                }
            ),
        }
