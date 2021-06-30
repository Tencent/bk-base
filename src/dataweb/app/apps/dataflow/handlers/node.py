# -*- coding:utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.utils.translation import ugettext as _

from apps.api import DataFlowApi
from apps.dataflow.handlers.result_table import ResultTable
from apps.exceptions import ApiResultError
from apps.utils import APIModel


class BaseNode(APIModel):

    M_STATUS_DISPLAY = {"no-start": _("未启动"), "running": _("运行中")}

    @classmethod
    def init_by_data(cls, data):
        o = cls(flow_id=data["flow_id"], node_id=data["node_id"])
        o._data = data
        return o

    def __init__(self, flow_id, node_id):
        node_id = int(node_id)
        super(BaseNode, self).__init__(flow_id, node_id)
        self.node_id = node_id
        self.flow_id = flow_id

    def is_exist(self):
        try:
            self._data = self._get_data()
            return True
        except ApiResultError:
            return False

    def get_frontend_data(self):
        """
        在返回前端数据时，提前封装好需要数据
        1. 添加 biz_name 字段

        @param {Boolean} add_biz_name 是否补充 biz_name 字段
        @param {Boolean} add_output_description 是否补充 output 字段的描述（结果表描述）
        """
        fdata = self.data.copy()
        fdata["status_display"] = fdata.get("status_display") or self.M_STATUS_DISPLAY.get(
            fdata["status"], fdata["status"]
        )
        return fdata

    def _get_data(self):
        api_params = {"flow_id": self.flow_id, "node_id": self.node_id}
        return DataFlowApi.flow_flows_nodes.retrieve(api_params)


class CleanNode(BaseNode):
    pass


class RealtimeNode(BaseNode):
    pass


class OfflineNode(BaseNode):
    pass


class StorageNode(BaseNode):
    def get_frontend_data(self, *args, **kwargs):
        fdata = super(StorageNode, self).get_frontend_data()
        if "cluster" in fdata["node_config"]:
            fdata["node_config"]["cluster_name"] = _("公共集群")
        return fdata


class MysqlNode(StorageNode):
    pass


class HdfsNode(StorageNode):
    pass


class ESNode(StorageNode):
    pass


class RTSourceNode(BaseNode):
    def get_frontend_data(self, *args, **kwargs):
        fdata = super(RTSourceNode, self).get_frontend_data()
        o_rt = ResultTable(result_table_id=fdata["result_table_id"])
        if o_rt.is_exist():
            fdata["node_config"]["table_name"] = o_rt.table_name
        else:
            fdata["node_config"]["table_name"] = _("结果数据表已被删除")

        return fdata


class AlgorithmModelNode(BaseNode):
    pass


class NodeFactory(object):
    M_NODE = {
        "clean": CleanNode,
        "realtime": RealtimeNode,
        "mysql_storage": MysqlNode,
        "tspider_storage": StorageNode,
        "hdfs_storage": HdfsNode,
        "offline": OfflineNode,
        "elastic_storage": ESNode,
        "rtsource": RTSourceNode,
        "algorithm_model": AlgorithmModelNode,
    }

    SOURCE_TYPES = ["rawsource", "rtsource"]
    CACL_TYPES = ["clean", "realtime", "offline"]
    STORAGE_TYPES = ["mysql_storage", "elastic_storage", "hdfs_storage"]

    @classmethod
    def get_node(cls, node):
        """
        @param node {dict} 节点配置信息，从 PIZZA 端获取到的信息，示例：
            {
                "node_id": 40,
                "status": "no-start",
                "name": null,
                "config": {
                    "raw_data_id": 111
                },
                "version": "V201709182121062852",
                "type": "rawsource",
                "frontend_info": {
                    "y":2,
                    "x":1
                }
            }
        @return {BaseNode} 节点对象
        """
        return cls.M_NODE.get(node["node_type"], BaseNode).init_by_data(node)
