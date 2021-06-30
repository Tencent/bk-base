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

from common.auth import perm_check
from common.base_utils import model_to_dict
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.flow import exceptions as Errors
from dataflow.flow.models import FlowUdfLog
from dataflow.flow.serializer import serializers
from dataflow.flow.udf.function import FunctionHandler
from dataflow.shared.permission import require_username


class FunctionViewSet(APIViewSet):
    lookup_field = "function_name"
    lookup_value_regex = "[a-z][a-z0-9_]*"

    @params_valid(serializer=serializers.CreateFunctionSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/flow/udfs/ 创建函数
        @apiName create_function
        @apiGroup Flow
        @apiDescription 创建函数
        @apiParamExample {json}
            {
                "func_name": 'test',
                "func_alias": 'test',
                "func_language": 'java',
                "func_udf_type": 'udf',
                "input_type": ['test'],
                "return_type": ['test'],
                "explain": 'test',
                "example": 'test',
                "example_return_value": 'test',
                "code_config": {"dependencies": [], "code": "public ..."}
            }
        @apiSuccessExample {json} 成功返回
            {}
        """
        return Response(FunctionHandler.create_function(params))

    @params_valid(serializer=serializers.UpdateFunctionSerializer)
    @perm_check("function.develop")
    def update(self, request, function_name, params):
        """
        @api {put} /dataflow/flow/udfs/:function_name/ 更新函数
        @apiName update_function
        @apiGroup Flow
        @apiDescription 更新函数
        @apiParamExample {json}
            {
                "func_alias": 'test',
                "func_language": 'java',
                "func_udf_type": 'udf',
                "input_type": ['test'],
                "return_type": ['test'],
                "explain": 'test',
                "example": 'test',
                "example_return_value": 'test',
                "code_config": {"dependencies": [], "code": "public ..."}
            }
        @apiSuccessExample {json} 成功返回
            {}
        """
        o_function = FunctionHandler(function_name)
        return Response(o_function.update_function(params))

    @params_valid(serializer=serializers.RetrieveFunctionSerializer)
    def retrieve(self, request, function_name, params):
        """
        @api {get} /dataflow/flow/udfs/:function_name/ 获取指定自定义函数
        @apiName get_function
        @apiGroup Flow
        @apiDescription 获取指定自定义函数
        @apiParamExample {json}
            {
                'add_project_info': false,       # 是否返回项目信息
                'add_flow_info': false,          # 是否返回任务信息
                'add_node_info': false,          # 是否返回节点信息
                'add_release_log_info': false    # 是否返回节点信息
            }
        @apiSuccessExample {json} 成功返回
            {
                "func_name": "abc",
                "func_alias": "阿比西",
                "func_language": "java",
                'related_project': [
                    {
                        'project_id': 1,
                        'project_name': 'xxx'
                    }
                ],
                'related_flow': [
                    {
                        'flow_id': 1,
                        'flow_name': 'xxx',
                        'status': 'running'
                    }
                ],
                'related_node': [
                    {
                        'node_id': 1,
                        'node_name': 'xxx',
                        'status': 'running'
                    }
                ],
                'release_log': [
                    {
                        'version': 'xxx',
                        'release_log': 'xxx'
                    }
                ]
            }
        """
        add_project_info = params["add_project_info"]
        add_flow_info = params["add_flow_info"]
        add_node_info = params["add_node_info"]
        add_release_log_info = params["add_release_log_info"]
        o_function = FunctionHandler(function_name)
        release_function_info = o_function.get_release_function(add_release_log_info=add_release_log_info)
        if add_project_info:
            release_function_info["related_project"] = o_function.get_related_projects()
        if add_flow_info:
            related_flows = o_function.get_related_flows()
            res = []
            for project_id, values in list(related_flows.items()):
                res.extend(values)
            release_function_info["related_flow"] = res
        if add_node_info:
            related_nodes = o_function.get_related_nodes()
            res = []
            for project_id, values in list(related_nodes.items()):
                res.extend(values)
            release_function_info["related_node"] = res
        return Response(release_function_info)

    @perm_check("function.develop")
    def destroy(self, request, function_name):
        """
        @api {delete} /dataflow/flow/udfs/:function_name/ 删除函数
        @apiName remove_function
        @apiGroup Flow
        @apiDescription 删除指定自定义函数
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                None
        """
        o_function = FunctionHandler(function_name)
        related_info = o_function.related_info
        if related_info:
            message = _("当前函数正被使用，不允许删除")
            raise Errors.UdfFuncRemovedNotAllowedError(message)
        o_function.remove_function()
        return Response()

    @require_username
    def list(self, request):
        """
        @api {get} /dataflow/flow/udfs/      获取自定义函数列表
        @apiName list_function
        @apiGroup Flow
        @apiDescription 获取自定义函数
        @apiParamExample {json}
            {}
        @apiSuccessExample {json} 成功返回
            [{
                "func_name": "abc",
                "func_alias": "阿比西",
                "managers": "xx,xx",
                "developers": "xx,xx",
                "version": "",
                "updated_at": "2018-01-01 11:00:00"
            }]
        """
        return Response(FunctionHandler.list_release_function())

    @detail_route(methods=["get"], url_path="code_frame")
    @params_valid(serializer=serializers.CodeFrameSerializer)
    def code_frame(self, request, function_name, params):
        """
        @api {get} /dataflow/flow/udfs/:function_name/code_frame/ 获取udf代码框架
        @apiName code_frame
        @apiGroup Flow
        @apiDescription 获取udf代码框架
        @apiParamExample {json}
            {
                "func_language": "java",
                "func_udf_type": "udf",
                "input_type": ["string", "string"],
                "return_type": ["string"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "code": "the code",
                }
        """
        o_function = FunctionHandler(function_name)
        data = o_function.generate_code_frame(params)
        return Response({"code": data})

    @detail_route(methods=["post"], url_path="edit")
    @params_valid(serializer=serializers.EditFunctionSerializer)
    @perm_check("function.develop")
    def edit(self, request, function_name, params):
        """
        @api {post} /dataflow/flow/udfs/:function_name/edit/ 编辑指定自定义函数
        @apiName edit_function
        @apiGroup Flow
        @apiDescription 点击编辑时
                    若 ignore_locked 为 true, 表示强制进入编辑函数并上锁
                    若为 false, 若函数被锁住，返回函数信息，前端提示用户已被锁住，是否还要进入，若是，界面再发请求进入编辑界面
        @apiParamExample {json}
            {
                'ignore_locked': false
            }
        @apiSuccessExample {json} 成功返回
            {
                "func_name": "abc",
                "func_alias": "阿比西",
                "func_language": "java",
                "func_udf_type": "udf",
                "input_type": ["string", "string"],
                "return_type": ["string"],
                "explain": "abc函数的用法",
                "example": "select abc(field_a) as cc from table",
                "example_return_value": "return_value"
                "code_config": {"dependencies":[], "code":"public ..."},
                "locked": 0,
                "locked_at": "xxx",
                "locked_by": "xxx"
            }
        """
        o_function = FunctionHandler(function_name)
        function_info = o_function.get_dev_function_info()
        if function_info:
            if not params["ignore_locked"]:
                ret = {
                    "locked": function_info["locked"],
                    "locked_at": function_info["locked_at"],
                    "locked_by": function_info["locked_by"],
                }
                if function_info["locked"]:
                    # 此时函数已被锁，只返回必要信息
                    return Response(ret)
                else:
                    # 自动重上锁
                    o_function.relock()
                    function_info["locked_at"] = get_request_username()
                    function_info["locked_by"] = get_request_username()
                    return Response(function_info)
            else:
                o_function.relock()
                return Response(o_function.get_dev_function_info())
        else:
            raise Errors.UdfFuncNotFoundError()

    @detail_route(methods=["post"], url_path="unlock")
    def unlock(self, request, function_name):
        """
        @api {post} /dataflow/flow/udfs/:function_name/unlock/ 解锁自定义函数
        @apiName unlock_function
        @apiGroup Flow
        @apiDescription 解锁自定义函数
        @apiParamExample {json}
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        o_function = FunctionHandler(function_name)
        o_function.unlock()
        return Response()

    @list_route(methods=["get"], url_path="get_debug_data")
    @params_valid(serializer=serializers.FunctionDebugDataSerializer)
    def get_debug_data(self, request, params):
        """
        @api {get} /dataflow/flow/udfs/get_debug_data/ 获取udf调试初始数据
        @apiName get_debug_data
        @apiGroup Flow
        @apiDescription 获取udf调试初始数据
        @apiParamExample {json}
            {
                'result_table_id': 'xxx'
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    'schema': [
                        {
                            'field_name': 'a',
                            'field_type': 'string'
                        },
                        {
                            'field_name': 'b',
                            'field_type': 'string'
                        }
                    ],
                    'value': [
                        ['x', 'xx']
                    ]
                }
        """
        return Response(FunctionHandler.get_debug_data(params["result_table_id"]))

    @detail_route(methods=["post"], url_path="release")
    @params_valid(serializer=serializers.DeployFunctionSerializer)
    def release(self, request, function_name, params):
        """
        @api {post} /dataflow/flow/udfs/:function_name/release/ 发布函数
        @apiName release_function
        @apiGroup Flow
        @apiDescription 发布函数
        @apiParamExample {json}
            {
                'release_log': 'xxx'
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        release_log = params["release_log"]
        return Response(FunctionHandler(function_name).release(release_log))

    @list_route(methods=["get"], url_path="support_python_packages")
    def support_python_packages(self, request):
        """
        @api {get} /dataflow/flow/udfs/support_python_packages/ 获取支持python包
        @apiName support_python_packages
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                [
                    {
                        "name": "pandas",
                        "version": "0.25.0",
                        "description": "分析结构化数据的工具集",
                        "is_cpython": 0
                    }
                ]
        """
        return Response(FunctionHandler.get_support_python_packages())

    @detail_route(methods=["get"], url_path="get_related_info")
    @params_valid(serializer=serializers.FunctionRelatedInfoSerializer)
    def get_related_info(self, request, function_name, params):
        """
        @api {get} /dataflow/flow/udfs/:function_name/get_related_info/
        @apiName get_related_info
        @apiGroup Flow
        @apiParam {string{project或flow}} object_type 对象类型
        @apiParam {string{小于32字符}} [object_id] 对象列表，对于project, 返回项目flow列表，对于flow， 返回节点列表
        @apiParamExample {json} 参数样例:
            {
               'object_type': 'project' | 'flow'
               'object_id': [1, 2]
            }
        @apiSuccessExample {json} 成功返回
            {
                1: {
                    [
                        {
                            'id': 1,
                            'name': 'xxx',
                            'status': 'running',
                            'version': 'v3'
                        }
                    ]
                }
            }
        """
        o_function = FunctionHandler(function_name)
        object_type = params["object_type"]
        object_ids = params["object_id"] or None
        if object_type == "project":
            res = o_function.get_related_flows(object_ids)
        else:
            res = o_function.get_related_nodes(object_ids)
        return Response(res)

    @detail_route(methods=["get"], url_path="check_example_sql")
    @params_valid(serializer=serializers.CheckExampleSqlSerializer)
    def check_example_sql(self, request, function_name, params):
        """
        @api {get} /dataflow/flow/udfs/:function_name/check_example_sql/ 样例SQL语法检测
        @apiName check_example_sql
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "sql": "select ****"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        o_function = FunctionHandler(function_name)
        o_function.check_example_sql(params["sql"])
        return Response()


class FunctionDebuggerViewSet(APIViewSet):
    lookup_field = "debugger_id"
    lookup_value_regex = r"\d+"

    def retrieve(self, request, function_name, debugger_id):
        """
        @api {get} /dataflow/flow/udfs/:function_name/debuggers/:debugger_id/ 调试函数
        @apiName retrieve
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "id": 1,
                    "func_name": "xx",
                    "action": "start_debug",
                    "start_time": "",
                    "end_time": "",
                    "status": "no-start",
                    "logs_zh": "xxx",
                    "logs_en": "",
                    "context": "{}",
                    "created_by": "xx",
                    "created_at": "xx",
                    "description": ""
                }
        """
        debug_log = FlowUdfLog.objects.get(id=debugger_id, func_name=function_name)
        return Response(model_to_dict(debug_log))

    @list_route(methods=["post"], url_path="start")
    @require_username
    @params_valid(serializer=serializers.StartFunctionDebuggerSerializer)
    def start_debugger(self, request, function_name, params):
        """
        @api {post} /dataflow/flow/udfs/:function_name/debuggers/start/ 调试函数
        @apiName start_debugger_function
        @apiGroup Flow
        @apiDescription 调试函数
        @apiParamExample {json}
            {
                'calculation_types': ['batch', 'stream'],
                'debug_data': {
                    'schema': [
                        {
                            'field_name': 'a',
                            'field_type': 'string'
                        },
                        {
                            'field_name': 'b',
                            'field_type': 'string'
                        }
                    ],
                    'value': [
                        ['x', 'xx']
                    ]
                },
                'result_table_id': 'xxx',
                'sql': 'xxx'
            }
        @apiSuccessExample {json} 成功返回
            {
                'task_id': 1111
            }
        """
        o_function = FunctionHandler(function_name)
        calculation_types = params["calculation_types"]
        debug_data = params["debug_data"]
        result_table_id = params["result_table_id"]
        sql = params["sql"]
        task_id = o_function.start_debugger(function_name, calculation_types, debug_data, result_table_id, sql)
        return Response({"task_id": task_id})

    @detail_route(methods=["get"], url_path="latest")
    def query_debug_latest(self, request, function_name, debugger_id):
        """
        @api {get} /dataflow/flow/udfs/:function_name/debuggers/:debugger_id/latest/ 获取调试信息
        @apiName query_debug_latest
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'status': 'failure',
                'logs': [
                    {
                        'level': 'ERROR',
                        'time': '2011-01-01 12:00:00',
                        'message': '启动实时调试任务',
                        'detail': 'class not found exception',
                        'stage': '4/5',
                        'status': 'failure' | 'running' | 'success'
                    },
                    {
                        'level': 'ERROR',
                        'time': '2011-01-01 12:00:00',
                        'message': '启动离线调试任务',
                        'detail': 'class not found exception',
                        'stage': '4/5',
                        'status': 'failure' | 'running' | 'success'
                    }
                ],
            }
        """
        debugger = FlowUdfLog.objects.get(id=debugger_id, func_name=function_name)
        _logs = debugger.get_logs()
        logs = []
        display_id = None
        # 根据 display_id 取出最后一批
        for _log in _logs[::-1]:
            if not display_id:
                display_id = _log["display_id"]
            else:
                if _log["display_id"] != display_id:
                    break
            logs.append(_log)

        return Response({"status": debugger.status, "logs": logs[::-1]})

    @list_route(methods=["get"], url_path="get_latest_config")
    @perm_check("function.develop", url_key="function_name")
    def get_latest_config(self, request, function_name):
        """
        @api {get} /dataflow/flow/udfs/:function_name/debuggers/get_latest_config/ 获取函数最近调试信息
        @apiName get_latest_config
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'calculation_types': ['batch', 'stream'],
                'debug_data': {
                    'schema': [
                        {
                            'field_name': 'a',
                            'field_type': 'string'
                        },
                        {
                            'field_name': 'b',
                            'field_type': 'string'
                        }
                    ],
                    'value': [
                        ['x', 'xx']
                    ]
                },
                'result_table_id': 'xxx',
                'sql': 'xxx'
            }
        """
        o_function = FunctionHandler(function_name)
        latest_config = o_function.get_latest_debug_context()
        return Response(latest_config)
