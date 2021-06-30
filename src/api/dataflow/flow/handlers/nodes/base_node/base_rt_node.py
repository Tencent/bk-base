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

from dataflow.flow.exceptions import NodeValidError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_processing_node import ProcessingNode
from dataflow.shared.databus.databus_helper import DatabusHelper


class RTNode(ProcessingNode):
    # 一个计算节点可能有默认存储类型，值等于待创建存储的cluster_type
    default_storage_type = None

    # 计算节点接存储时，总线分发时依赖的存储类型，目前stream和batch都用kafka
    channel_storage_type = "kafka"

    def build_rt_params(self, form_data, from_node_ids, username):
        return None

    @classmethod
    def build_rt_channel_storage_config(cls, result_table_id):
        """
        创建 channel 存储，目前仅有kafka
        @param result_table_id:
        @return:
        TODO 待删除，统一移到 node_controller
        """
        response_data = DatabusHelper.get_inner_kafka_channel_info(result_table_id)
        cluster_type = "kafka"
        return {
            "result_table_id": result_table_id,
            "cluster_name": response_data["cluster_name"],
            "cluster_type": cluster_type,
            # 为实时节点kafka存储添加过期时间，讨论结果：先用默认值3d
            "expires": "3d",
            "storage_config": "{}",
            "storage_channel_id": response_data["id"],
            "priority": response_data["priority"],
            "generate_type": "system",
        }

    def build_hdfs_storage_config(self, result_table_id, expires):
        """
        对于 离线，离线指标，MLSQL 节点，更新上游 HDFS 过期时间用到
        """
        return {
            "result_table_id": result_table_id,
            "cluster_name": NodeUtils.get_storage_cluster_name(
                self.project_id, self.default_storage_type, result_table_id
            ),
            "cluster_type": self.default_storage_type,
            "expires": str(expires) + ("d" if expires != -1 else ""),
            "storage_config": "{}",
            "generate_type": "system",
        }

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(RTNode, self).build_before(from_node_ids, form_data, is_create)
        # 节点更新
        if not is_create:
            # 不允许出现循环 rt
            self.check_circle_rt_id(form_data)
        return form_data

    def check_circle_rt_id(self, form_data):
        """
        不允许有 rt 环
        用户更新数据源后，再更新节点，可能出现数据环
        """
        result_table_id = self.result_table_ids[0]
        from_result_table_ids = NodeUtils.get_from_result_table_ids(self.node_type, form_data)
        for rt_id in from_result_table_ids:
            if rt_id == result_table_id:
                raise NodeValidError(_("输入和输出结果表不允许重复"))
