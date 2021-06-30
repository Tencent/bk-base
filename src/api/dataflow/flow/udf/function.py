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

from common.base_utils import model_to_dict
from common.local import get_request_username
from django.db import transaction

from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowInfo, FlowNodeInfo, FlowNodeProcessing, FlowUdfLog
from dataflow.flow.tasks import start_udf_debugger
from dataflow.shared.meta.project.project_helper import ProjectHelper
from dataflow.shared.permission import update_scope_role
from dataflow.udf.debug.debug_driver import auto_generate_data
from dataflow.udf.functions.function_driver import FunctionHandler as InnerFunctionHandler
from dataflow.udf.functions.function_driver import (
    check_example_sql,
    create_function,
    generate_code_frame,
    get_dev_function_info,
    get_function_info,
    get_function_related_info,
    get_support_python_packages,
    list_function_info,
    list_function_log,
    lock_function,
    release_function,
    unlock_function,
    update_function,
)


class FunctionHandler(object):
    def __init__(self, function_name):
        self.function_name = function_name

        # 缓存信息
        self._related_info = None
        self._related_flows = None

    @classmethod
    def create_function(cls, params):
        func_name = params["func_name"]
        res = create_function(params)
        update_scope_role("function.manager", func_name, [get_request_username()])
        return res

    @classmethod
    def list_release_function(cls):
        return list_function_info()

    @property
    def related_info(self):
        """
        {
          'flow_id': [
             {
                  'node_id': 1,
                  'status': 'normal' | 'warning',
                  'version': 'v3'
              }
          ]
        }
        @return:
        """
        if self._related_info is None:
            related_processing_info = get_function_related_info(self.function_name)
            objects = FlowNodeProcessing.objects.filter(processing_id__in=list(related_processing_info.keys()))
            _related_info = {}
            for obj in objects:
                flow_id = obj.flow_id
                node_id = obj.node_id
                processing_id = obj.processing_id
                flow_info = _related_info.setdefault(flow_id, [])
                node_info = related_processing_info[processing_id]
                node_info.update({"node_id": node_id})
                flow_info.append(node_info)
            self._related_info = _related_info
        return self._related_info

    def related_flows(self, project_ids=None):
        """
        默认返回函数所有的flow
        @param project_ids:
        @return:
        """
        if self._related_flows is None:
            self._related_flows = []
            flow_ids = list(self.related_info.keys())
            if project_ids is None:
                flow_objects = FlowInfo.objects.filter(flow_id__in=flow_ids)
            else:
                flow_objects = FlowInfo.objects.filter(flow_id__in=flow_ids, project_id__in=project_ids)
            for flow_object in flow_objects:
                self._related_flows.append(model_to_dict(flow_object))
        return self._related_flows

    def get_related_projects(self):
        """
        @return:
        [
            {
                'project_id': 1,
                'project_name': 'xxx'
            }
        ]
        """
        projects_info = []
        projects = set()
        for related_flow in self.related_flows():
            projects.add(related_flow["project_id"])
        if projects:
            projects_info = ProjectHelper.get_multi_project(list(projects))
        return projects_info

    def get_related_flows(self, project_ids=None):
        res = {}
        flows_info = self.related_flows(project_ids)
        for flow_info in flows_info:
            _flows = res.setdefault(flow_info["project_id"], [])
            _flows.append(flow_info)
        return res

    def _display_node_status(self, db_node_status, logical_node_status):
        """
        # 节点状态显示优先级
        # failure -> warning -> running, starting, stopping, no-start
        @param db_node_status: 节点在 db 中标识的状态
        @param logical_node_status: 节点通过逻辑判断的状态
        @return:
        """
        if db_node_status in [FlowInfo.STATUS.FAILURE] or logical_node_status in [FlowInfo.STATUS.FAILURE]:
            return FlowInfo.STATUS.FAILURE
        if logical_node_status == "warning":
            return "warning"
        else:
            return db_node_status

    def get_related_nodes(self, flow_ids=None):
        """
        @return:
        {
            'flow_id': [
                {
                    'node_id': 1,
                    'node_name': 'xx',
                    'status': 'xx',
                    'version': 'v3'
                }
            ]
        }
        """
        node_ids = []
        udf_info = {}
        for flow_id, nodes_info in list(self.related_info.items()):
            if flow_ids is not None and flow_id not in flow_ids:
                continue
            for node_info in nodes_info:
                node_ids.append(node_info["node_id"])
                node_status = node_info["status"]
                if NODE_FACTORY.get_node_handler(node_info["node_id"]).is_modify_version():
                    node_status = "warning"
                udf_info[node_info["node_id"]] = {
                    "status": node_status,
                    "version": node_info["version"],
                }
        objects = FlowNodeInfo.objects.filter(node_id__in=node_ids)
        res = {}
        for object in objects:
            node_status = object.status
            _nodes = res.setdefault(object.flow_id, [])
            _nodes.append(
                {
                    "node_id": object.node_id,
                    "node_name": object.node_name,
                    "status": self._display_node_status(node_status, udf_info[object.node_id]["status"]),
                    "version": udf_info[object.node_id]["version"],
                }
            )
        return res

    def get_release_function(self, add_release_log_info=False):
        """
        @param add_release_log_info:
        @return:
            {
                'func_name': 'xx',
                ...
                'release_log': [
                    {
                        'version': 'xxx',
                        'release_log': 'xxx'
                    }
                ],
                'related_processing': {
                    'xxx': {
                        'status': 'warning' | 'normal'
                    }
                }
            }
        """
        function_info = get_function_info(self.function_name)
        if add_release_log_info:
            function_info["release_log"] = list_function_log(self.function_name)
        return function_info

    def update_function(self, params):
        return update_function(self.function_name, params)

    def generate_code_frame(self, params):
        return generate_code_frame(self.function_name, params)

    def get_dev_function_info(self):
        return get_dev_function_info(self.function_name)

    def lock(self):
        lock_function(self.function_name)

    def unlock(self):
        unlock_function(self.function_name)

    @transaction.atomic()
    def relock(self):
        self.unlock()
        self.lock()

    def start_debugger(self, function_name, calculation_types, debug_data, result_table_id, sql):
        context = {
            "calculation_types": calculation_types,
            "debug_data": debug_data,
            "result_table_id": result_table_id,
            "sql": sql,
        }
        debug_id = start_udf_debugger(function_name, get_request_username(), context)
        return debug_id

    def get_latest_debug_context(self):
        history_logs = FlowUdfLog.objects.filter(func_name=self.function_name, action="start_debug").order_by("-id")
        if history_logs:
            return history_logs[0].get_context()
        else:
            return {}

    @staticmethod
    def get_debug_data(result_table_id):
        return auto_generate_data(result_table_id)

    def release(self, release_log):
        return release_function(self.function_name, release_log)

    @staticmethod
    def get_support_python_packages():
        return get_support_python_packages()

    def check_example_sql(self, sql):
        check_example_sql(self.function_name, sql)

    def remove_function(self):
        InnerFunctionHandler(self.function_name).delete_function()
