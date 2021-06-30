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

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.node_handler import NodeHandler
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class StorageNode(NodeHandler):
    storage_type = ""

    @property
    def result_table_id(self):
        result_table_ids = self.result_table_ids
        if not result_table_ids:
            raise Errors.NodeError(_("获取节点关联RT失败"))
        return self.result_table_ids[0]

    @property
    def processing_type(self):
        """
        存储节点的processing_type等于父节点的processing_type
        @return:
        """
        parent_nodes = self.get_from_nodes_handler()
        if len(parent_nodes) != 1:
            raise Errors.NodeError(_("存储节点(%s)仅支持一个父计算节点" % self.node_id))
        result_table_ids = parent_nodes[0].result_table_ids
        if len(result_table_ids) != 1:
            raise Errors.NodeError(
                _("存储节点(%(node_id)s)仅支持一个结果表作为数据源，当前个数为%(length)s")
                % {"node_id": self.node_id, "length": len(result_table_ids)}
            )
        rt = ResultTable(result_table_ids[0])
        if rt.is_exist():
            return rt.processing_type
        else:
            raise Errors.NodeError(_("获取存储节点(%s)结果表信息失败." % self.node_id))

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(StorageNode, self).build_before(from_node_ids, form_data, is_create)
        parent_nodes = []
        for node_id in from_node_ids:
            node_handler = NODE_FACTORY.get_node_handler(node_id=node_id)
            parent_nodes.append(node_handler)
        NodeUtils.check_expires(self.node_type, parent_nodes, form_data)
        return form_data

    def add_after(self, username, from_node_ids, form_data):
        # 需要用户输入一个result_table_id
        from_result_table_id = form_data["result_table_id"]
        form_data.update({"result_table_id": from_result_table_id})
        # 所有节点 add_after 统一返回格式
        rts_heads_tails = {
            "result_table_ids": [form_data["result_table_id"]],
            "heads": None,
            "tails": None,
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        prev_from_result_table_id = prev["result_table_ids"][0]
        # 需要用户输入一个result_table_id
        from_result_table_id = form_data["result_table_id"]
        # 如果存储节点更换了父节点，提示不允许
        if prev_from_result_table_id != from_result_table_id:
            raise Errors.NodeError(_("存储节点不支持更换结果表，请删除重新保存"))
        rts_heads_tails = {
            "result_table_ids": [from_result_table_id],
            "heads": None,
            "tails": None,
        }
        return rts_heads_tails

    def remove_before(self, username):
        """
        存储节点删除前的清理工作

        1. 调用StorekitAPI清除存储配置
        2. 停掉相关的分发进程，在部署过程是不会停掉分发进程，只能在删除的时候停掉
        3. 如果该存储节点为该rt最后一个相关的存储节点，则清理rt
        """
        super(StorageNode, self).remove_before(username)
        # 如果该存储节点为该rt最后一个相关的存储节点，则清理rt。针对用户先删计算，再删存储的情况
        if not self.get_from_nodes_handler():
            for one_result_table_id in self.result_table_ids:
                if not DataProcessingHelper.check_data_processing(one_result_table_id):
                    related_storage_node = NodeUtils.get_rt_related_storage_node(self.flow_id, [one_result_table_id])
                    if len(related_storage_node) == 1 and self.node_id in related_storage_node:
                        ResultTableHelper.delete_result_table(one_result_table_id)

    def update_node_metadata_relation(self, bk_biz_id, result_table_ids, heads=None, tails=None):
        """
        更新节点与rt、processing等实体的关联关系
        @param bk_biz_id:
        @param result_table_ids:
        @param heads: 只有计算节点有 heads，tails 参数
        @param tails:
        """
        self.node_info.update_as_storage(bk_biz_id, result_table_ids[0])

    def build_rt_storage_config(self, result_table_id, form_data, username):
        """
        各种存储节点的配置信息不尽相同
        @param result_table_id:
        @param form_data:
        @param username:
        @return:
        """
        raise NotImplementedError("节点类型（%s）未定义storage_config构建方法." % self.node_type)
