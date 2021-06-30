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

from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import ValidError
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.stream.stream_helper import StreamHelper


def validate_node_infos(nodes, is_create):
    """
    解析nodes信息，确保其符合对应节点类型的表单参数要求
    @param nodes: {list}
        各元素必须字段：node_type, from_nodes
    @param is_create:
    @return: 返回验证后的节点参数结果
    """
    for node_info in nodes:
        # 校验表单
        node_type = node_info["node_type"]
        name = node_info["name"]
        from_nodes = NodeUtils.get_input_nodes_config(node_type, node_info)
        # from_nodes = node_info.get('from_nodes', [])
        if node_type in NodeTypes.SOURCE_CATEGORY:
            result_table_id = node_info.get("result_table_id", None)
            if not result_table_id:
                raise ValidError(_("数据源节点类型必须包含参数result_table_id"))
            node_info["from_result_table_ids"] = [result_table_id]
        elif node_type in NodeTypes.STORAGE_CATEGORY:
            if len(from_nodes) != 1 or len(from_nodes[0]["from_result_table_ids"]) != 1:
                raise ValidError(_("存储节点类型必须包含参数from_nodes，且仅支持一个上游节点和一个相应上游结果表"))
            result_table_id = from_nodes[0]["from_result_table_ids"][0]
            node_info["result_table_id"] = result_table_id
            node_info["from_result_table_ids"] = [result_table_id]
        else:
            from_result_table_ids = []
            for rt_ids in from_nodes:
                from_result_table_ids.extend(rt_ids["from_result_table_ids"])
            node_info["from_result_table_ids"] = from_result_table_ids
            node_info = NodeUtils.set_input_nodes_config(node_type, node_info, from_nodes)
        node_obj = NODE_FACTORY.get_node_handler_by_type(node_type)
        try:
            NodeUtils.validate_config(node_info, node_obj.node_form, is_create)
        except Exception as e:
            raise Exception(
                _("%(node_type)s类型节点(%(name)s)创建失败: %(message)s")
                % {"node_type": node_type, "name": name, "message": "{}".format(e)}
            )
    return nodes


def param_verify(params):
    """
    节点参数校验
    @param params 校验参数
    @return: 返回参数校验结果
    """
    if params["scheduling_type"] == "batch":
        return BatchHelper.check_batch_param(params)
    elif params["scheduling_type"] == "stream":
        return StreamHelper.check_stream_param(params)
