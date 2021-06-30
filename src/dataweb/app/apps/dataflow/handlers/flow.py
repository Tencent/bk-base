# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import copy
import json

from apps.api import DataFlowApi, MetaApi
from apps.dataflow.handlers.business import Business
from apps.dataflow.handlers.node import NodeFactory
from apps.dataflow.handlers.project import Project
from apps.exceptions import ApiResultError
from apps.utils import APIModel


class Flow(APIModel):
    def __init__(self, flow_id):
        super(Flow, self).__init__()
        self.flow_id = flow_id
        self._nodes = None
        self._links = None

    def load_graph(self, version=None):
        graph_info = DataFlowApi.flow_flows_graph.retrieve({"flow_id": self.flow_id, "version": version})
        self._nodes = graph_info["nodes"]
        self._links = graph_info["links"]
        return graph_info

    def save_frontend_graph(self, data):
        """
        保存图形中的前端信息

        @param {Dict} data 前端图信息
        @paramExample
            {
                locations: [
                    {
                        id: 'ch111',
                        node_id: 111,
                        x: 11,
                        y: 222,
                        type: 'rawsource',
                        config: '',
                        ...
                    },
                    {
                        id: 'ch222',
                        x: 11,
                        y: 222,
                        type: 'clean',
                        config: '',
                        ...
                    }
                ],
                lines: [
                    {
                        'source': {'arrow': 'Right', 'id': 'ch111'},
                        'target': {'arrow': 'Left', 'id': 'ch222'}
                    }
                ]

            }
        """
        # 已保存节点配置 + 未保存节点配置
        saved_nodes = []
        m_saved_node_id = {}

        no_saved_nodes = []
        for _location in data["locations"]:
            if "node_id" in _location:
                saved_nodes.append(
                    {"node_id": _location["node_id"], "frontend_info": {"x": _location["x"], "y": _location["y"]}}
                )
                m_saved_node_id[_location["id"]] = _location["node_id"]
            else:
                no_saved_nodes.append(_location)

        # 标记连线节点对应的后台ID
        for _l in data["lines"]:
            _source_id = _l["source"]["id"]
            _target_id = _l["target"]["id"]
            if _source_id in m_saved_node_id:
                _l["source"]["node_id"] = m_saved_node_id[_source_id]
                _l["source"]["id"] = self._build_id(m_saved_node_id[_source_id])

            if _target_id in m_saved_node_id:
                _l["target"]["node_id"] = m_saved_node_id[_target_id]
                _l["target"]["id"] = self._build_id(m_saved_node_id[_target_id])

        no_saved_lines = []
        for _l in data["lines"]:
            if "node_id" not in _l["source"] or "node_id" not in _l["target"]:
                no_saved_lines.append(_l)

        update_nodes_params = {"flow_id": self.flow_id, "action": "update_nodes", "data": no_saved_nodes}
        DataFlowApi.flow_flows_graph.update(update_nodes_params)

        save_draft_params = {
            "flow_id": self.flow_id,
            "action": "save_draft",
            "data": json.dumps({"locations": no_saved_nodes, "lines": no_saved_lines}),
        }
        DataFlowApi.flow_flows_graph.update(save_draft_params)

    def clean_nodes(self, nodes):
        """
        给节点添加中文名等额外信息
        :param nodes:
        :return:
        """
        if not nodes:
            return []

        c_nodes = []
        # 结果表对应中文名
        result_tables = MetaApi.result_tables.list({"project_id": self.project_id})
        result_tables_name = {_rt["result_table_id"]: _rt["description"] for _rt in result_tables}
        # 业务id对应业务名称
        m_biz_name = Business.get_name_dict()
        for _n in nodes:
            fdata = NodeFactory.get_node(_n).get_frontend_data()
            if "bk_biz_id" in fdata["node_config"]:
                _biz_id = fdata["node_config"]["bk_biz_id"]
                fdata["node_config"]["bk_biz_name"] = m_biz_name.get(_biz_id, _biz_id)
                fdata["output_description"] = result_tables_name.get(fdata["result_table_id"], fdata["node_name"])
            c_nodes.append(fdata)
        return c_nodes

    def is_exist(self):
        try:
            data = DataFlowApi.flow_flows.retrieve({"flow_id": self.flow_id})
            self._data = data
            return True
        except ApiResultError:
            return False

    def _get_data(self):
        return DataFlowApi.flow_flows.retrieve({"flow_id": self.flow_id})

    @classmethod
    def list_project_flows(cls, projects, add_extra_info=None):
        """
        @todo 切换至前端获取后删除此方法及相关引用
        @apiParam {List} projects 项目列表
        @apiParam {Dict} [add_extra_info] 是否添加节点数量等 # 添加扩展字段
        @apiParamExample {json} 获取用户的flow列表
            {
                "user": "admin",
                "add_extra_info": {
                    "add_node_count_info": False,
                    "add_process_status_info": True,
                    "add_exception_info": True,
                }
            }
        """
        if not projects:
            return []

        l_project_ids = [_project["project_id"] for _project in projects]

        # 请求 Flow 列表基本参数
        api_param = {}

        if add_extra_info:
            api_param.update(add_extra_info)

        # 如果拉取项目太多，可以直接拉取全量
        if len(projects) < 50:
            api_param["project_id"] = l_project_ids

        flows = DataFlowApi.flow_flows.list(api_param)

        # Flow 按项目分类
        d_flows = {project_id: [] for project_id in l_project_ids}
        for _flow in flows:
            if _flow["project_id"] in d_flows:
                d_flows[_flow["project_id"]].append(_flow)

        for project in projects:
            project["flows"] = d_flows[project["project_id"]]

        Project.wrap_flow_count(projects)

        return projects

    @property
    def nodes(self):
        if self._nodes is None:
            self.load_graph()
        return self._nodes

    @property
    def links(self):
        if self._links is None:
            self.load_graph()
        return self._links

    @property
    def status(self):
        return self.data["status"]

    @property
    def project_id(self):
        return self.data["project_id"]

    @staticmethod
    def _build_id(build_id):
        return "ch_%s" % build_id

    @staticmethod
    def _build_frontend_node(node):
        _default = {"x": 100, "y": 100}
        if node["frontend_info"]:
            _frontend_info = copy.deepcopy(node["frontend_info"])
        else:
            _frontend_info = copy.deepcopy(_default)

        _frontend_info.update(node)
        _frontend_info["id"] = Flow._build_id(node["node_id"])
        _frontend_info["result_table_id"] = node["node_config"].get("result_table_id", None)
        return _frontend_info

    @staticmethod
    def _build_frontend_link(link):
        _default = {"source": {"arrow": "Right"}, "target": {"arrow": "Left"}}
        if link["frontend_info"]:
            _frontend_info = link["frontend_info"]
        else:
            _frontend_info = _default

        _frontend_info["source"]["id"] = Flow._build_id(link["from_node_id"])
        _frontend_info["target"]["id"] = Flow._build_id(link["to_node_id"])

        return _frontend_info
