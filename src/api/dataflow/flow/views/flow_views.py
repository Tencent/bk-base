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
import os
from functools import reduce

from common.auth.objects import is_sys_scopes
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet, APIViewSet

# 本地调试取消权限认证
from django.conf import settings
from django.forms import model_to_dict
from django.http import HttpResponse
from django.utils.translation import ugettext as _
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters, serializers
from rest_framework.response import Response

from dataflow.flow.app import FlowConfig
from dataflow.flow.check import flow_check
from dataflow.flow.controller.node_controller import NodeController
from dataflow.flow.exceptions import (
    FileUploadedError,
    FlowError,
    FlowManageError,
    FlowPermissionNotAllowedError,
    NodeValidError,
    ValidError,
)
from dataflow.flow.handlers.flow import FlowHandler, get_multi_wrap
from dataflow.flow.handlers.flow_utils import validate_node_infos
from dataflow.flow.handlers.generic import DataPageNumberPagination, FlowModelSerializer
from dataflow.flow.handlers.monitor import FlowMonitorHandlerSet
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.nodes.process_node.model_node import ModelNode
from dataflow.flow.handlers.signals import flow_version_wrap
from dataflow.flow.models import FlowExecuteLog, FlowInfo, FlowNodeRelation
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer.serializers import (
    ApplyCustomCalculateJobConfirmSerializer,
    CreateFlowSerializer,
    CreateFlowWithNodesSerializer,
    CreateTDWSourceSerializer,
    FileUploadSerializer,
    FlowStartActionSerializer,
    InitFlowWithNodesSerializer,
    MultiFlowSerializer,
    RemoveTDWSourceSerializer,
    UpdateFlowSerializer,
    UpdateGraphSerializer,
    UpdateJobSerializer,
    UpdateTDWSourceSerializer,
)
from dataflow.flow.settings import FLOW_MAX_NODE_COUNT
from dataflow.flow.tasklog.flow_log_util import FlowLogUtil
from dataflow.flow.tasks import restart_flow, start_flow, stop_flow
from dataflow.flow.utils.filtersets import get_filterset
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.permission import perm_check_user, require_username

RUN_MODE = settings.RUN_MODE
if RUN_MODE in ["LOCAL"]:
    from dataflow.shared.permission import LocalUserPerm as UserPerm
    from dataflow.shared.permission import local_perm_check as perm_check
    from dataflow.shared.permission import local_perm_check_param as perm_check_param
else:
    from common.auth import perm_check
    from common.auth.perms import UserPerm

    from dataflow.shared.permission import perm_check_param


def index(request):
    return HttpResponse("Hello DataFlow.Flow!")


class HealthCheckView(APIViewSet):
    def healthz(self, request):
        """
        @api {get}  /dataflow/flow/healthz Flow健康状态
        @apiName healthz
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    rabbitmq: {
                        status: false,
                        message: "(-1, 'EOF')"
                    },
                    celery.flow: {
                        status: false,
                        message: "[Errno 111] Connection refused"
                    }
                }
        """
        rtn = flow_check()
        return Response(rtn)


class TestViewSet(APIViewSet):
    def test_exception(self, request):
        raise FlowError(_("自动抛出异常，用于测试"), "11101", "xxx_sssas1111")


class FlowViewSet(APIModelViewSet):
    class FlowSerializer(FlowModelSerializer):
        created_at = serializers.DateTimeField(label=_("创建时间"), read_only=True, format="%Y-%m-%d %H:%M:%S")
        updated_at = serializers.DateTimeField(label=_("更新时间"), read_only=True, format="%Y-%m-%d %H:%M:%S")

        def validate(self, data):

            # 项目底下校验 name 是否重复
            created_by = get_request_username()
            if created_by:
                data["created_by"] = created_by
            name = data.get("flow_name", None)
            if name is None:
                return data

            if not self.instance:
                qs = FlowInfo.objects.filter(project_id=data["project_id"], flow_name=name)
            else:
                qs = FlowInfo.objects.filter(project_id=self.instance.project_id, flow_name=name).exclude(
                    pk=self.instance.pk
                )

            if qs.exists():
                raise serializers.ValidationError(_("项目底下存在同名任务"))

            return data

        class Meta:
            model = FlowInfo

    queryset = FlowInfo.objects.all().order_by("-flow_id")
    serializer_class = FlowSerializer
    filter_backends = (
        filters.SearchFilter,
        DjangoFilterBackend,
        filters.OrderingFilter,
    )
    # 修改DjangoFilterBackend，默认取model的所有字段
    filter_class = get_filterset(FlowInfo, exclude_fields=["bk_app_code"])
    # 由于 DjangoFilterBackend 修改过滤字段的方式，但是我们重新定义了filter_class，所以filter_fields不会生效
    # filter_fields = ('status',)
    search_fields = ("=flow_id", "flow_name", "created_by")
    pagination_class = DataPageNumberPagination
    lookup_value_regex = r"\d+"
    ordering = (
        "-updated_at",
        "flow_id",
        "status",
        "flow_name",
        "created_by",
        "created_at",
    )
    # 由于采用ModelViewSet所以必须要用model里有的字段
    lookup_field = "flow_id"

    @list_route(methods=["post"], url_path="apply_confirm_custom_calculates")
    @params_valid(serializer=ApplyCustomCalculateJobConfirmSerializer)
    def apply_confirm(self, request, params):
        """
        @api {post} /dataflow/flow/flows/apply_confirm_custom_calculates 补算任务审批结果调用
        @apiName apply_confirm_custom_calculate
        @apiGroup Flow
        @apiParamExample {json} 参数示例
            {
                'id': 'flow_id#custom_calculate_id',
                'confirm_type': 'cancelled' | 'approved' | 'rejected',   # 撤销，同意，驳回
                'message': 'xx',
                'operator': 'xx'
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {}
        """
        id = params["id"]
        flow_id = int(id.split("#")[0])
        custom_calculate_id = int(id.split("#")[1])
        confirm_type = params["confirm_type"]
        message = params["message"] if "message" in params else ""
        operator = params["operator"]
        res = FlowHandler(flow_id).apply_confirm_custom_calculate_job(
            custom_calculate_id, operator, message, confirm_type
        )
        return Response(res)

    @params_valid(serializer=CreateFlowSerializer)
    @perm_check_param("project.manage_flow", "project_id")
    @perm_check("flow.create", detail=False)
    def create(self, request, params):
        """
        @api {post}  /dataflow/flow/flows 创建Flow
        @apiName create flow
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "project_id": 1,
                "flow_name": 'xxx',

                # 可选 tdw 参数
                "tdw_conf": {                           # 任务信息
                    "task_info": {
                        "spark": {
                            "spark_version": "2.3",
                        }
                    },
                    "gaia_id": 111,                     # 计算集群ID
                    "tdwAppGroup": "g_ieg_xxx",         # 应用组ID
                    "sourceServer": "127.0.0.1",        # 源服务器
                }
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "flow_id": 1,
                    "created_at": "2018-10-13 21:57:14",
                    "updated_at": "2018-10-23 17:41:17",
                    "active": true,
                    "flow_name": "flow01",
                    "project_id": 1,
                    "status": "no-start",
                    "is_locked": 0,
                    "latest_version": "V2018102317411792358",
                    "bk_app_code": "bk_data",
                    "created_by": null,
                    "locked_by": null,
                    "locked_at": null,
                    "updated_by": "admin",
                    "description": ""
                }
        """
        if "tdw_conf" in params:
            request.data["tdw_conf"] = json.dumps(request.data["tdw_conf"])
        with auto_meta_sync(using="default"):
            response = super(FlowViewSet, self).create(request)
        FlowHandler(response.data["flow_id"]).create_alert_config()
        return response

    @params_valid(serializer=UpdateFlowSerializer)
    @perm_check("flow.update", detail=True, url_key="flow_id")
    def partial_update(self, request, flow_id, params):
        """
        @api {patch}  /dataflow/flow/flows/:fid/ 更新Flow
        @apiName update flow
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "project_id": 1,
                "flow_name": 'xxx',

                # 可选 tdw 参数
                "tdw_conf": {
                    "task_info": {                      # 任务信息
                        "spark": {
                            "spark_version": "2.3",
                        }
                    },
                    "gaia_id": 111,                     # 计算集群ID
                    "tdwAppGroup": "g_ieg_xxx",         # 应用组ID
                    "sourceServer": "127.0.0.1",        # 源服务器
                }
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "flow_id": 1,
                    "created_at": "2018-10-13 21:57:14",
                    "updated_at": "2018-10-23 17:41:17",
                    "active": true,
                    "flow_name": "flow01",
                    "project_id": 1,
                    "status": "no-start",
                    "is_locked": 0,
                    "latest_version": "V2018102317411792358",
                    "bk_app_code": "bk_data",
                    "created_by": null,
                    "locked_by": null,
                    "locked_at": null,
                    "updated_by": "admin",
                    "description": ""
                }
        """
        if "tdw_conf" in request.data:
            tdw_conf = request.data["tdw_conf"]
            FlowHandler(flow_id).check_tdw_conf(tdw_conf)
            request.data["tdw_conf"] = json.dumps(tdw_conf)
        with auto_meta_sync(using="default"):
            response = super(FlowViewSet, self).partial_update(request)
        return response

    @get_multi_wrap()
    def list(self, request):
        """
        @api {get}  /dataflow/flow/flows 获取Flow列表信息
        @apiName list flow info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        "flow_id": 1,
                        "created_at": "2018-10-13 21:57:14",
                        "updated_at": "2018-10-23 17:41:17",
                        "active": true,
                        "flow_name": "flow01",
                        "project_id": 1,
                        "status": "no-start",
                        "is_locked": 0,
                        "latest_version": "V2018102317411792358",
                        "bk_app_code": "bk_data",
                        "created_by": null,
                        "locked_by": null,
                        "locked_at": null,
                        "updated_by": "admin",
                        "description": ""
                    },
                    ...
                ]
        """
        # 若为基于结果表过滤，鉴权后直接返回任务列表
        result_table_id = request.query_params.get("result_table_id")
        if result_table_id:
            # 获取产生这个 RT 的 flow
            flows = FlowHandler.list_rela_by_rtid(
                [result_table_id],
                node_category=NodeTypes.PROCESSING_CATEGORY,
            )
            flow_ids = []
            project_ids = []
            for _flow in flows:
                flow_ids.append(str(_flow["flow_id"]))
                project_ids.append(str(_flow["project_id"]))
            self.request.GET._mutable = True
            origin_flow_id__in = request.query_params.get("flow_id__in", "")
            if origin_flow_id__in:
                flow_ids = flow_ids + origin_flow_id__in.split(",")
            # 若获取不到，返回空
            request.query_params["flow_id__in"] = ",".join(flow_ids) or "''"
            request.query_params["project_id__in"] = ",".join(project_ids) or None
            # 以 project_id__in 过滤条件为准
            request.query_params["project_id"] = None
            self.request.GET._mutable = False
        scopes = UserPerm(get_request_username()).list_scopes("project.retrieve")
        if is_sys_scopes(scopes):
            response = super(FlowViewSet, self).list(request)
        else:
            # 获取指定项目列表进行鉴权
            project_id = request.query_params.get("project_id")
            project_id__in = project_id if project_id is not None else request.query_params.get("project_id__in")
            auth_project_ids = [_p["project_id"] for _p in scopes]
            if not auth_project_ids:
                return Response([])
            if project_id__in is None:
                # 获取所有有权限的项目列表下的所有 flow 列表
                self.request.GET._mutable = True
                request.query_params["project_id__in"] = str(
                    reduce(lambda x, y: "{},{}".format(str(x), str(y)), auth_project_ids)
                )
                self.request.GET._mutable = False
                response = super(FlowViewSet, self).list(request)
            else:
                # 判断所有指定项目是否对于当前用户是否都有权限
                project_ids = project_id__in.split(",")
                for query_project_id in project_ids:
                    if not query_project_id:
                        raise ValueError(_("传入空的项目ID."))
                    if query_project_id not in auth_project_ids and int(query_project_id) not in auth_project_ids:
                        raise FlowPermissionNotAllowedError(_("您没有权限使用当前项目(ID=%s)") % query_project_id)
                response = super(FlowViewSet, self).list(request)
        add_extra_info = {
            "add_node_count_info": bool(int(request.query_params.get("add_node_count_info", 0))),
            "add_process_status_info": bool(int(request.query_params.get("add_process_status_info", 0))),
            "add_exception_info": bool(int(request.query_params.get("add_exception_info", 0))),
            "show_display": bool(int(request.query_params.get("show_display", 0))),
        }

        # 注意处理分页结果
        if "results" in response.data:
            FlowHandler.wrap_extra_info(response.data["results"], add_extra_info, many=True)
        else:
            FlowHandler.wrap_extra_info(response.data, add_extra_info, many=True)

        return response

    def retrieve(self, request, flow_id=None):
        """
        @api {get}  /dataflow/flow/flows/:fid 获取DataFlow信息
        @apiName get flow info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "flow_id": 1,
                    "created_at": "2018-10-13 21:57:14",
                    "updated_at": "2018-10-23 17:41:17",
                    "active": true,
                    "flow_name": "flow01",
                    "project_id": 1,
                    "status": "no-start",
                    "is_locked": 0,
                    "latest_version": "V2018102317411792358",
                    "bk_app_code": "bk_data",
                    "created_by": null,
                    "locked_by": null,
                    "locked_at": null,
                    "updated_by": "admin",
                    "description": ""
                }
        """
        add_exception_info = int(request.query_params.get("add_exception_info", 0))

        add_extra_info = {
            "show_display": bool(int(request.query_params.get("show_display", 0))),
            "add_custom_calculate_info": True,
        }

        response = super(FlowViewSet, self).retrieve(request, pk=flow_id)

        if add_exception_info:
            FlowHandler.wrap_exception(response.data, many=False)

        FlowHandler.wrap_extra_info(response.data, add_extra_info, many=False)

        return response

    # 二级资源graph统一入口，不拆分是因为graph没有id的概念
    @detail_route(methods=["get", "put"], url_path="graph")
    def graph(self, request, flow_id=None):
        """
        graph信息获取更新统一入口
        """
        if request.method == "GET":
            return self.get_graph(request, flow_id)
        elif request.method == "PUT":
            # 必须静态指定flow_id
            return self.update_graph(request, flow_id=flow_id)

    @perm_check("flow.retrieve")
    def get_graph(self, request, flow_id=None):
        """
        @api {get}  /dataflow/flow/flows/:fid/graph 获取DataFlow图信息
        @apiName get_graph
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "nodes": [],
                    "version": "",
                    "links": []
                }
        """
        flow = FlowHandler(flow_id=flow_id)
        return Response(flow.get_graph())

    @perm_check("flow.update")
    @flow_version_wrap(FlowConfig.handlers)
    @params_valid(serializer=UpdateGraphSerializer)
    def update_graph(self, request, flow_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/graph 更新DataFlow图信息
        @apiName update_graph
        @apiDescription 更新图信息，支持多个操作
            批量节点更新（update_nodes）
            批量删除连线（delete_lines）
            批量新增连线（create_lines）
            保存草稿信息 (save_draft)

        @apiGroup Flow
        @apiParamExample {json} 批量更新节点配置
            {
                action: 'update_nodes'
                data: [
                    {node_id: 111, frontend_info: {}}
                ]

            }
        @apiParamExample {json} 批量删除连线
            {
                action: 'delete_lines',
                data: [
                    {from_node_id: 111, to_node_id: 222}
                ]
            }
        @apiParamExample {json} 批量新增连线
            {
                action: 'create_lines',
                data: [
                    {from_node_id: 111, to_node_id: 222, frontend_info: {}}
                ]
            }
        @apiParamExample {json} 保存草稿功能
            {
                action: 'save_draft',
                data: ''
            }
        """
        action = params["action"]
        data = params["data"]
        username = get_request_username()
        flow_handler = FlowHandler(flow_id=flow_id)
        return Response(getattr(flow_handler, action)(username, data))

    @detail_route(methods=["get"], url_path="get_graph_extra_info")
    @perm_check("flow.retrieve")
    def get_graph_extra_info(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/get_graph_extra_info 返回画布额外信息
        @apiName get_graph_extra_info
        @apiGroup Flow
        @apiParamExample {json}
            {}
        @apiSuccessExample {json} 成功返回
             {
                'node_id1': {
                    'status': 'failure' | 'no-start' | 'running' ...
                    'message': 'xxx'
                }
             }
        """
        node_status = {}
        node_status.update(FlowHandler(flow_id).list_source_node_status())
        return Response(node_status)

    @detail_route(methods=["post"], url_path="create")
    @perm_check("flow.update")
    def init_flow(self, request, flow_id):
        """
        @api {post} /dataflow/flow/flows/:fid/create 传入节点信息在一个 flow 中重新初始化一个完整的flow（清空旧flow）
        @apiName init_flow
        @apiGroup Flow
        @apiDescription 清空当前 flow，创建一个新的 flow, 具体节点参数可参考【Flow - 新增节点】config配置或用户文档
        @apiParamExample {json}
            {
                'nodes': [
                    {
                        "bk_biz_id": 1,                         # 业务号
                        "id": 1,                                # 当前节点自定义id
                        "from_nodes": [                         # 上游节点信息
                            {
                                'id': 3,
                                'from_result_table_ids': ['1_xxxx']
                            }
                        ],
                        "node_type": "stream",                  # 节点类型
                        "table_name": "xxx",                    # 数据输出
                        "output_name": "xxx",                   # 输出中文名
                        "window_type": "none",                  # 窗口类型
                        "window_time": 0,                       # 窗口长度
                        "waiting_time": 0,                      # 延迟时间
                        "count_freq": 30,                       # 统计频率
                        "sql": "select * from etl_van_test5",   # sql
                        "frontend_info": {"x": 92, "y": 293}    # 前端信息(当前仅有坐标信息)，可选值
                        ...
                    },
                    {},...
                ]
            }
        @apiSuccessExample {json} 成功返回
            {
                "flow_id": 1,
                "node_ids": [2,3]                               # 成功创建的节点列表
            }
        """
        try:
            self.params_valid(serializer=InitFlowWithNodesSerializer)
        except Exception as e:
            logger.exception(e)
            raise ValidError(_("导入配置的文件参数格式错误:{}".format(e)))
        username = get_request_username()
        nodes = request.data["nodes"]
        # 先统一校验
        nodes = validate_node_infos(nodes, True)
        flow = None
        try:
            if FLOW_MAX_NODE_COUNT is not None and len(nodes) > FLOW_MAX_NODE_COUNT:
                raise FlowError(_("当前任务节点数量超出限制，最大允许数量为%s") % FLOW_MAX_NODE_COUNT)
            flow = FlowHandler(flow_id)
            if flow.status in FlowInfo.PROCESS_STATUS:
                raise FlowError(_("任务正在运行中，不可导入dataflow"))
            NodeController.del_flow_nodes(flow, is_delete_flow=False)
            res = NodeController.add_nodes(flow, username, nodes)
            res["flow_id"] = flow_id
        except FlowManageError as e:
            NodeController.del_flow_nodes(flow, is_delete_flow=False)
            raise e
        except Exception as e:
            raise FlowManageError(_("创建任务期间出现非预期异常({})，请联系管理员".format(e)))
        return Response(res)

    @detail_route(methods=["get"], url_path="export")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    def export_flow(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/export 按照 create_flow 的格式导出 flow
        @apiName export_flow
        @apiGroup Flow
        @apiDescription 按照 create_flow 的参数格式导出 flow
        @apiSuccessExample {json} 成功返回
            {
                "project_id": 1,
                "flow_name": "xxx",
                "nodes": [
                     {
                        'name': '实时数据源',
                        'id': 1,
                        'from_nodes': [],
                        'bk_biz_id': 591,
                        'node_type': 'stream_source',
                        'result_table_id': '591_abc'
                    },
                    {
                        'name': '实时节点',
                        'id': 2,
                        'from_nodes': [
                            {
                                'id': 1,
                                'from_result_table_ids': ['591_abc']
                            }
                        ],
                        'bk_biz_id': 591,
                        'node_type': 'realtime',
                        'table_name': 'xxx',
                        'sql': 'select ip from 591_abc',
                        "output_name": "xxx",
                        "window_type": "none"
                    }
                ]
            }
        """
        res = FlowHandler(flow_id).export()
        return Response(res)

    @perm_check("flow.retrieve")
    @detail_route(methods=["get"], url_path="versions")
    def list_versions(self, request, flow_id=None):
        """
        @api {get}  /dataflow/flow/flows/:fid/versions 获取DataFlow历史版本
        @apiName list_versions
        @apiGroup Flow
        @apiSuccessExample {list} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        "created_at": "2018-10-16 20:36:55",
                        "version": "V2018101620365271064",
                        "flow_id": 1
                    },
                    {
                        "created_at": "2018-10-16 20:39:43",
                        "version": "V2018101620394316035",
                        "flow_id": 1
                    }
                ]
        """
        flow = FlowHandler(flow_id=flow_id)
        return Response(flow.list_versions())

    @perm_check("flow.retrieve")
    def get_version_graph(self, request, flow_id, version_id):
        """
        @api {get}  /dataflow/flow/flows/:fid/versions/:vid/graph 获取DataFlow某个历史版本[目前忽略vid]
        @apiName get_version_graph
        @apiGroup Flow
        @apiSuccessExample {list} 成功返回
            HTTP/1.1 200 OK
                {
                    "nodes": [
                        {
                            "status": "no-start",
                            "node_config": {
                                "bk_biz_id": 2,
                                "result_table_id": "2_xxxx_clean"
                            },
                            "node_id": 1,
                            "alert_status": "info",
                            "frontend_info": {
                                "y": 293,
                                "x": 92
                            },
                            "node_type": "etl_source",
                            "node_name": "etl_name",
                            "version": null,
                            "result_table_ids": [
                                "2_xxxx_clean"
                            ],
                            "flow_id": 1,
                            "has_modify": false
                        },
                        ...
                    ],
                    "versions": "V2018102321494724324",
                    "links": [
                        {
                            "to_node_id": 3,
                            "from_node_id": 1,
                            "frontend_info": {
                                "source": {
                                    "node_id": 1,
                                    "id": "ch_1478",
                                    "arrow": "Right"
                                },
                                "target": {
                                    "id": "ch_1509",
                                    "arrow": "Left"
                                }
                            }
                        },
                        ...
                    ]

                }
        """
        flow = FlowHandler(flow_id=flow_id)
        return Response(flow.get_graph(version_id))

    @perm_check("flow.delete")
    @flow_version_wrap(FlowConfig.handlers)
    def destroy(self, request, flow_id):
        """
        @api {delete} /dataflow/flow/flows/:flow_id 删除DataFlow
        @apiName remove_flow
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                None
        """
        flow = FlowHandler(flow_id=flow_id)
        NodeController.del_flow_nodes(flow, is_delete_flow=True)
        return Response()

    @list_route(methods=["delete"], url_path="multi_destroy")
    @params_valid(serializer=MultiFlowSerializer)
    @perm_check_param("flow.delete", "flow_ids", is_array=True)
    def multi_destroy(self, request, params):
        """
        @api {delete} /dataflow/flow/flows/multi_destroy 批量删除DataFlow
        @apiName multi_remove_flow
        @apiGroup Flow
        @apiParam {list} flow_ids
        @apiParamExample {json} 参数样例
            {
                "flow_ids": [1, 2, 3]
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "success_flows": [],
                    "fail_flows": []
                }
        """
        flow_ids = params["flow_ids"]
        m_success_flow = {}
        m_fail_flow = {}
        for flow_id in set(flow_ids):
            try:
                flow = FlowHandler(flow_id=int(flow_id))
                NodeController.del_flow_nodes(flow, is_delete_flow=True)
                m_success_flow[flow_id] = "ok"
            except FlowError as e:
                m_fail_flow[flow_id] = e.message
            except Exception as e:
                m_fail_flow[flow_id] = "{}".format(e)
        return Response({"success_flows": m_success_flow, "fail_flows": m_fail_flow})

    @list_route(methods=["get"], url_path="latest_flows")
    @perm_check_param("project.retrieve", "project_id")
    def list_latest_flows(self, request):
        """
        @api {get} /dataflow/flow/flows/latest_flows?project_id=xxx 获取最近操作的flow
        @apiName list_latest_flows
        @apiGroup Flow
        @apiParam {int} project_id

        @apiParamExample {json} 参数样例:
            {
                "project_id": 1
            }

        @apiSuccessExample {json} 成功返回:
            [
                {
                    "flow_id": 1,
                    "is_deleted": false,
                    "name": "test flow",
                    "biz_id": 1,
                    "project_id": 1,
                    "graph_info": null,
                    "created_by": "user00",
                    "created_at": "2017-08-24T17:29:20.326616",
                    "git_tag": "master",
                    "workers": "1",
                    "updated_by": null,
                    "updated_at": null,
                    "status": "no-start",
                    "is_locked": 0,
                    "version": null,
                    "app_code": "data"
                }
            ]
        """
        project_id = request.query_params["project_id"]
        latest_flows = FlowInfo.objects.filter(project_id=project_id).order_by("-updated_at")[:10]
        serializer = self.FlowSerializer(latest_flows, many=True)
        FlowHandler.wrap_exception(serializer.data, many=True)
        return Response(serializer.data)

    @detail_route(methods=["get"], url_path="list_rt_by_flowid")
    @perm_check("flow.retrieve")
    def list_rt_by_flowid(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/list_rt_by_flowid/ 获取flow中对应的离线/实时任务列表
        @apiName list_rt_by_flowid
        @apiGroup Flow
        @apiDescription 获取指定flow对应的rt列表（按照stream/batch分类）
        @apiParam {string} flow_id 指定flow_id
        @apiSuccessExample {json} rt列表返回结果（分类型聚合）
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_dict": {
                    "batch": {
                        "591_iptest_expand3_offline": {
                            "schedule_period": "hour",
                            "component_type": "spark",
                            "deploy_mode": "yarn",
                            "result_tables": [
                                {
                                'result_table_id': '591_iptest_expand3_offline',
                                'result_table_name_alias': '591_iptest_expand3_offline'
                                }
                            ]
                        }
                    },
                    "stream": {
                        "13035_7ecd590e3473496f887ae37b3ff36f9a": {
                            "schedule_period": "realtime",
                            "component_type": "flink",
                            "deploy_mode": "k8s",
                            "result_tables": [
                                {
                                'result_table_id': '591_iptest_expand',
                                'result_table_name_alias': '591_iptest_expand'
                                },
                                {
                                'result_table_id': '591_iptest_expand2',
                                'result_table_name_alias': '591_iptest_expand2'
                                },
                                {
                                'result_table_id': '591_iptest_expand3',
                                'result_table_name_alias': '591_iptest_expand3'
                                }
                            ]
                        }
                    }
                },
                "flow_id": "3164",
                "geog_area_code": "inland"
            },
            "result": true
        }
        """
        ret = FlowLogUtil.list_rt_by_flowid(flow_id)
        # get flow geog_code
        flow_handler = FlowHandler(flow_id)
        geog_area_code = flow_handler.geog_area_codes[0]
        return Response(
            data={
                "flow_id": flow_id,
                "geog_area_code": geog_area_code,
                "result_table_dict": ret,
            }
        )

    @list_route(methods=["get"], url_path="list_rela_by_rtid")
    @require_username
    def list_rela_by_rtid(self, request):
        """
        @api {get} /dataflow/flow/flows/list_rela_by_rtid?result_table_id=xxx 通过rtid获取DataFlow列表
        @apiName list_rela_by_rtid
        @apiGroup Flow
        @apiParam {string} result_table_id

        @apiParamExample {json} 参数样例(用&符号支持传多个)
            {
                "result_table_id": '1_xxxxx'
            }
        @apiSuccessExample {list} 成功返回
            [
                {
                "flow_id": 174,
                "created_at": "2018-11-15 11:12:36",
                "updated_at": "2018-11-19 20:49:36",
                "active": true,
                "flow_name": "flow01",
                "project_id": 162,
                "status": "running",
                "is_locked": 0,
                "latest_version": "V2018111618513180137",
                "bk_app_code": "xxx",
                "created_by": "xxx",
                "locked_by": "xxx",
                "locked_at": "2018-11-15 11:12:35",
                "updated_by": "xxx",
                "description": "",
                "has_exception": false,
                "project_name": "project1"
                }
            ]
        """
        result_table_ids = request.query_params.getlist("result_table_id")
        perm_check_user("result_table.retrieve", result_table_ids)
        flows_info = FlowHandler.list_rela_by_rtid(result_table_ids)
        FlowHandler.wrap_exception(flows_info, many=True)
        add_extra_info = {"show_display": bool(int(request.query_params.get("show_display", 0)))}
        FlowHandler.wrap_extra_info(flows_info, add_extra_info, many=True)
        return Response(flows_info)

    @list_route(methods=["get"], url_path="list_flow_monitor_info")
    def list_flow_monitor_info(self, request):
        """
        @api {get} /dataflow/flow/flows/list_flow_monitor_info?result_table_id=xxx&result_table_id=xxx
                    通过rtid获取包含监控信息DataFlow列表
        @apiName list_flow_monitor_info
        @apiGroup Flow
        @apiParam {string[]} result_table_id
        @apiSuccessExample {list} 成功返回
            {
                '591_xxx': [
                    {'flow_id': 1, 'is_source': true, 'node_id': 1, 'module': 'xx', 'component': 'xx',
                        'node_type': 'xxx'},
                    {'flow_id': 2, 'is_source': false, 'node_id': 1, 'module': 'xx', 'component': 'xx',
                        'node_type': 'xxx'},
                ]
            }
        """
        result_table_ids = request.query_params.getlist("result_table_id", None)
        if result_table_ids is None:
            raise ValidError(_("请求参数必须包含result_table_id."))
        flow_monitor_info = FlowHandler.list_flow_monitor_info(result_table_ids)
        return Response(flow_monitor_info)

    @list_route(methods=["get"], url_path="projects/count")
    @perm_check_param("project.retrieve", "project_id", is_array=True)
    def list_rela_count_by_project_id(self, request):
        """
        @api {get} /dataflow/flow/flows/projects/count?project_id=1&project_id=2 通过project_id获取flow相关数量，支持批量
        @apiName list_rela_count_by_project_id
        @apiGroup Flow
        @apiSuccessExample {list} 成功返回
            [
                {'project_id': 111, 'count': 11, 'running_count': 10, 'no_start_count': 1},
                {'project_id': 2222, 'count': 22, 'running_count': 11, 'no_start_count': 11}
            ]
        """
        query_dict = request.query_params
        ids = [int(_id) for _id in set(query_dict.getlist("project_id"))]
        data = FlowHandler.list_rela_count_by_project_id(ids)
        return Response(data)

    @detail_route(methods=["get"], url_path="cluster_groups")
    @perm_check("flow.execute")
    def list_cluster_groups(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/cluster_groups 获取可选的集群组列表
        @apiName list_optional_cluster_groups
        @apiGroup Flow
        @apiSuccessExample {list} 成功返回
            [
                {'cluster_group_id': 'xxx', 'cluster_group_name': 'xx', 'cluster_group_alias': '默认集群组',
                    'scope': 'public'},
                {'cluster_group_id': 'xxx', 'cluster_group_name': 'xx', 'cluster_group_alias': '默认集群组',
                    'scope': 'public'},
            ]
        """
        optional_cluster_groups = FlowHandler(flow_id).list_project_cluster_group()
        return Response(optional_cluster_groups)

    @list_route(methods=["post"], url_path="multi_start")
    @params_valid(serializer=MultiFlowSerializer)
    @perm_check_param("flow.execute", "flow_ids", is_array=True)
    def multi_start(self, request, params):
        """
        @api {post} /dataflow/flow/flows/multi_start 批量启动DataFlow
        @apiName multi_start
        @apiGroup Flow
        @apiParam {list} flow_ids
        @apiParamExample {json} 参数样例:
            {
                "flow_ids": [1, 2, 3]
            }
        @apiSuccessExample {json} 成功返回
            {
                'success_flows': {
                    1: 111,   # 启动成功返回task_id
                    2: 222
                },
                'fail_flows': {
                    3: "空任务不可启动"  # 启动失败返回错误信息
                }
            }
        """
        operator = get_request_username()
        flow_ids = params["flow_ids"]
        is_latest = params["is_latest"] if "is_latest" in params else False

        m_success_flow = {}
        m_fail_flow = {}
        for flow_id in flow_ids:
            try:
                task_id = start_flow(flow_id, operator, is_latest=is_latest)
                m_success_flow[flow_id] = task_id
            except FlowError as e:
                m_fail_flow[flow_id] = e.message

        return Response({"success_flows": m_success_flow, "fail_flows": m_fail_flow})

    @detail_route(methods=["post"], url_path="start")
    @perm_check("flow.execute")
    @params_valid(serializer=FlowStartActionSerializer)
    @require_username
    def start(self, request, flow_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/start 启动DataFlow
        @apiName start_flow
        @apiGroup Flow
        @apiParam [string] consuming_mode 从最新/继续/最早位置消费，对应有'from_tail', 'continue',
                           'from_head'，默认为continue
        @apiParam [string] cluster_group 集群组
        @apiParamExample {json} 参数样例:
            {
                'consuming_mode': 'continue',
                'cluster_group': 'default'
            }
        @apiSuccessExample {json} 成功返回
            {
                'task_id': 1111
            }
        """
        operator = get_request_username()
        consuming_mode = params["consuming_mode"] if "consuming_mode" in params else "continue"
        cluster_group = params["cluster_group"] if "cluster_group" in params else None
        task_id = start_flow(
            flow_id,
            operator,
            consuming_mode=consuming_mode,
            cluster_group=cluster_group,
        )
        return Response({"task_id": task_id})

    @list_route(methods=["post"], url_path="multi_stop")
    @params_valid(serializer=MultiFlowSerializer)
    @perm_check_param("flow.execute", "flow_ids", is_array=True)
    @require_username
    def multi_stop(self, request, params):
        """
        @api {post} /dataflow/flow/flows/multi_stop 批量停止DataFlow
        @apiName multi_stop
        @apiGroup Flow
        @apiParam {list} flow_ids
        @apiParamExample {json} 参数样例:
            {
                "flow_ids": [1, 2, 3]
            }
        @apiSuccessExample {json} 成功返回
            {
                'success_flows': {
                    1: 111,   # 启动成功返回 task_id
                    2: 222
                },
                'fail_flows': {
                    3: "空任务不可启动"  # 启动失败返回错误信息
                }
            }
        """
        operator = get_request_username()
        flow_ids = params["flow_ids"]
        m_success_flow = {}
        m_fail_flow = {}
        for flow_id in flow_ids:
            try:
                task_id = stop_flow(flow_id, operator)
                m_success_flow[flow_id] = task_id
            except FlowError as e:
                m_fail_flow[flow_id] = e.message

        return Response({"success_flows": m_success_flow, "fail_flows": m_fail_flow})

    @detail_route(methods=["post"], url_path="stop")
    @perm_check("flow.execute")
    @require_username
    def stop(self, request, flow_id=None):
        """
        @api {post} /dataflow/flow/flows/:fid/stop 停止DataFlow
        @apiName stop_flow
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'task_id': 1111
            }
        """
        operator = get_request_username()
        task_id = stop_flow(flow_id, operator)
        return Response({"task_id": task_id})

    @detail_route(methods=["post"], url_path="restart")
    @perm_check("flow.execute")
    @params_valid(serializer=FlowStartActionSerializer)
    @require_username
    def restart(self, request, flow_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/restart 重启DataFlow
        @apiName restart_flow
        @apiGroup Flow
        @apiParam [string] consuming_mode 从最新/继续/最早位置消费，对应有'from_tail', 'continue',
                           'from_head'，默认为continue
        @apiParam [string] cluster_group 集群组
        @apiParamExample {json} 参数样例:
            {
                'consuming_mode': 'continue',
                'cluster_group': 'default'
            }
        @apiSuccessExample {json} 成功返回
            {
                'task_id': 1111
            }
        """
        operator = get_request_username()
        consuming_mode = params["consuming_mode"] if "consuming_mode" in params else "continue"
        cluster_group = params["cluster_group"] if "cluster_group" in params else None
        task_id = restart_flow(
            flow_id,
            operator,
            consuming_mode=consuming_mode,
            cluster_group=cluster_group,
        )
        return Response({"task_id": task_id})

    @list_route(methods=["post"], url_path="multi_restart")
    @params_valid(serializer=MultiFlowSerializer)
    @perm_check_param("flow.execute", "flow_ids", is_array=True)
    @require_username
    def multi_restart(self, request, params):
        """
        @api {post} /dataflow/flow/flows/multi_restart 批量重启DataFlow
        @apiName multi_restart
        @apiGroup Flow
        @apiParam {list} flow_ids
        @apiParamExample {json} 参数样例:
            {
                "flow_ids": [1, 2, 3]
            }
        @apiSuccessExample {json} 成功返回
            {
                'success_flows': {
                    1: 111,   # 启动成功返回 task_id
                    2: 222
                },
                'fail_flows': {
                    3: "空任务不可启动"  # 启动失败返回错误信息
                }
            }
        """
        operator = get_request_username()
        flow_ids = params["flow_ids"]
        m_success_flow = {}
        m_fail_flow = {}
        for flow_id in flow_ids:
            try:
                task_id = restart_flow(flow_id, operator)
                m_success_flow[flow_id] = task_id
            except FlowError as e:
                m_fail_flow[flow_id] = e.message

        return Response({"success_flows": m_success_flow, "fail_flows": m_fail_flow})

    @detail_route(methods=["get"], url_path="latest_deploy_data")
    @perm_check("flow.retrieve")
    def get_latest_deploy_info(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/latest_deploy_data 获取flow最近部署信息
        @apiName get_latest_deploy_info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'status': 'failure',
                'logs': [
                    {'message': '\\u9501\\u5b9a DataFlow', 'level': 'INFO', 'time': '2017-10-08 20:43:44'},
                    {'message': '--- \\u5f00\\u59cb\\u542f\\u52a8 DataFlow ---',
                    'level': 'INFO', 'time': '2017-10-08 20:43:44'},
                    {'message': '\\u542f\\u52a8\\uff081_xiaozetestclean15\\uff09\\u8282\\u70b9', 'level': 'INFO',
                                'time': '2017-10-08 20:43:44'},
                    {'message': '\\u542f\\u52a8\\u6e05\\u6d17\\u4efb\\u52a1', 'level': 'INFO',
                                'time': '2017-10-08 20:43:44'},
                    {'message': '\\u66f4\\u65b0\\u8282\\u70b9\\u72b6\\u6001\\uff0cstatus=running', 'level': 'INFO',
                                'time': '2017-10-08 20:43:44'},
                    {'message': '\\u542f\\u52a8\\uff081_xiaozerealtime2\\uff09\\u8282\\u70b9', 'level': 'INFO',
                                'time': '2017-10-08 20:43:44'},
                    {'message': '\\u542f\\u52a8\\u5b9e\\u65f6\\u4efb\\u52a1', 'level': 'INFO',
                                'time': '2017-10-08 20:43:44'},
                    ...
                ],
                'logs_en': '',
                'logs_zh': '',
                'flow_id': 1,
                'created_by': 'user00',
                'action': 'stop',
                'id': 57,
                'context': None,
                'start_time': '2018-11-27T21:19:32.928522',
                'end_time': '2018-11-27T21:19:32.928522',
                'version': None,
                'description': '',
                'created_by': '',
                'progress': 60.1
            }
        """
        tasks = FlowExecuteLog.objects.filter(flow_id=flow_id, status__in=FlowExecuteLog.DEPLOY_STATUS)
        if tasks.exists():
            tasks = tasks.order_by("-id")
            latest_o_task = tasks[0]

            latest_task = model_to_dict(latest_o_task)
            latest_task["logs"] = latest_o_task.get_logs()
            latest_task["progress"] = latest_o_task.get_progress()

            # 补算任务不需要对任务进行超时终止
            if (
                latest_o_task.action not in [FlowExecuteLog.ACTION_TYPES.CUSTOM_CALCULATE]
                and latest_o_task.confirm_timeout()
            ):
                # 确认是否存在任务超时，超时所需的补救措施，解锁任务
                o_flow = FlowHandler(flow_id=flow_id)
                o_flow.unlock()
                o_flow.sync_status()

            return Response(latest_task)

        return Response(None)

    class FlowTaskSerializer(serializers.ModelSerializer):
        """
        可为model查询的每个object增加字段，如增加logs字段
        source为调用的方法
        """

        logs = serializers.ListField(source="get_logs", read_only=True)
        progress = serializers.CharField(source="get_progress", read_only=True)

        class Meta:
            model = FlowExecuteLog

    @detail_route(methods=["get"], url_path="deploy_data")
    @perm_check("flow.retrieve")
    def list_deploy_info(self, request, flow_id):
        """
        @api {post} /dataflow/flow/flows/:fid/deploy_data 获取flow所有部署信息
        @apiName list_deploy_info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            [
                {
                    'status': 'failure',
                    'logs': [
                        {'message': '\\u9501\\u5b9a DataFlow', 'level': 'INFO', 'time': '2017-10-08 20:43:44'},
                        {'message': '--- \\u5f00\\u59cb\\u542f\\u52a8 DataFlow ---',
                        'level': 'INFO', 'time': '2017-10-08 20:43:44'},
                        {'message': '\\u542f\\u52a8\\uff081_xiaozetestclean15\\uff09\\u8282\\u70b9', 'level': 'INFO',
                                    'time': '2017-10-08 20:43:44'},
                        {'message': '\\u542f\\u52a8\\u6e05\\u6d17\\u4efb\\u52a1', 'level': 'INFO',
                                    'time': '2017-10-08 20:43:44'},
                        {'message': '\\u66f4\\u65b0\\u8282\\u70b9\\u72b6\\u6001\\uff0cstatus=running', 'level': 'INFO',
                                    'time': '2017-10-08 20:43:44'},
                        {'message': '\\u542f\\u52a8\\uff081_xiaozerealtime2\\uff09\\u8282\\u70b9', 'level': 'INFO',
                                    'time': '2017-10-08 20:43:44'},
                        {'message': '\\u542f\\u52a8\\u5b9e\\u65f6\\u4efb\\u52a1', 'level': 'INFO',
                                    'time': '2017-10-08 20:43:44'},
                        ...
                    ],
                    'logs_en': '',
                    'logs_zh': '',
                    'flow_id': 1,
                    'created_by': 'user00',
                    'action': 'stop',
                    'id': 57,
                    'context': None,
                    'start_time': '2018-11-27T21:19:32.928522',
                    'end_time': '2018-11-27T21:19:32.928522',
                    'version': None,
                    'description': '',
                    'created_by': ''
                }
            ]
        """
        queryset = FlowExecuteLog.objects.filter(flow_id=flow_id, status__in=FlowExecuteLog.DEPLOY_STATUS).order_by(
            "-id"
        )

        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.FlowTaskSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.FlowTaskSerializer(queryset, many=True)
        return Response(serializer.data)

    @detail_route(methods=["get"], url_path="monitor_data")
    @perm_check("flow.retrieve")
    def list_monitor_data(self, request, flow_id):
        """
        @api {get} /dataflow/flow/flows/:fid/monitor_data 根据fid获取监控打点信息
        @apiName list_flow_monitor_data
        @apiGroup Flow
        @apiParam [string] monitor_type 监控指标类型，用逗号分割
        @apiParam [json] detail_time_filter 时间过滤条件
        @apiParamExample {json} 获取flow告警信息
            {
                monitor_type: 'dmonitor_alerts'
                detail_time_filter: "[{
                    "monitor_type": "dmonitor_alerts",
                    "interval":"1d"
                }]"
            }
        @apiParamExample {json} 获取flow运行信息
            {
                monitor_type: 'data_loss_io_total'
                detail_time_filter: "[{
                    "monitor_type": "data_loss_io_total",
                    "interval":"1d"
                }]"
            }
        @apiParamExample {json} 获取flow rt运行状态信息
            {
               monitor_type: 'rt_status'
                detail_time_filter: "[{
                    "monitor_type": "rt_status",
                    "interval":"1d"
                }]"
            }
        @apiSuccessExample {json} 成功返回
            {
                "dmonitor_alerts": [
                    {
                        "message_zh-cn": "最近10分钟数据量平均值85.7条/分，比7天前同比下降91.53%, 7天前平均1012.5条/分",
                        "project_id": "195",
                        "full_message_zh-cn": "节点(591_node1)最近10分钟数据量平均值85.7条/分，比7天前同比下降91.53%,
                                                7天前平均1012.5条/分",
                        "level": "info",
                        "full_message_en": " average data count per minute during recent 10 minutes is 85.7142857143.
                                            decresed 91.53% compared with 7 days ago.",
                        "flow_id": "222",
                        "time": 1544174262,
                        "component": null,
                        "module": null,
                        "node_type": "realtime",
                        "message_en": " average data count per minute during recent 10 minutes is 85.7142857143.
                                        decresed 91.53% compared with 7 days ago.",
                        "alert_code": "data_trend",
                        "biz_id": "0",
                        "recover_time": 1545047648,
                        "alert_status": "recovered",
                        "result_table_id": "591_node1",
                        "node_id": "0",
                        "msg": "xxxx"
                    }
                ],
                "data_loss_io_total": {
                    "100": {
                        "input": 446,    // 目前一个节点只有一个rt，以此展示
                        "output": 446,   // 目前一个节点只有一个rt，以此展示
                        "node_type": "etl_source",
                        "data": {
                            "591_new_van": {
                                "output": 446u
                            }
                        }
                    },
                    "101": {
                        "input": 446,
                        "output": 446,
                        "node_type": "realtime",
                        "data": {
                            "591_stream_test_abc": {
                                "input": 446",
                                "output": 446"
                            }
                        }
                    }
                }
            }
        """
        flow = FlowHandler(flow_id=flow_id)
        if not flow.nodes:
            raise Exception(_("空任务不能监控"))
        monitor = FlowMonitorHandlerSet(flow)
        return Response(monitor.list_monitor_data())

    # 算法相关接口
    @list_route(methods=["get"], url_path="get_models")
    def get_models(self, request):
        """
        @api {get} /dataflow/flow/flows/get_models 获取模型列表
        @apiName list_models
        @apiGroup Flow
        @apiSuccessExample {list} 成功返回
            [
                {
                    'model_id': 'xxx',
                    'model_version_id': 'xxx',
                    'model_name': 'xx',
                    'description': 'xx',
                    'version_description': 'xxx'
                },
                {
                    'model_id': 'xxx',
                    'model_version_id': 'xxx',
                    'model_name': 'xx',
                    'description': 'xx',
                    'version_description': 'xxx'
                }
            ]
        """
        project_id = request.query_params.get("project_id", None)
        if project_id is None:
            raise ValidError(_("请传入项目ID，project_id 为必填项."))
        models = ModelNode.get_models([project_id])
        return Response(models)

    @detail_route(methods=["post"], url_path="create_tdw_source")
    @params_valid(serializer=CreateTDWSourceSerializer)
    @perm_check("flow.update", detail=True, url_key="flow_id")
    def create_tdw_source(self, request, flow_id, params):
        """
        @api {post}  /dataflow/flow/flows/:fid/create_tdw_source 创建tdw存量表对应的RT
        @apiName create_tdw_result_table
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
                "cluster_id": "xx",         # 集群ID
                "db_name": "xxx",           # 数据库名称
                "tdw_table_name": "xxx",    # 存量表名
                "output_name": "xxx",       # 输出中文名
                "description": "xxx",
                "associated_lz_id": "xxx"   # 可选
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "result_table_id": xxxx
                }
        """
        res_data = FlowHandler(flow_id).create_tdw_source(params)
        return Response(res_data)

    @detail_route(methods=["patch"], url_path="update_tdw_source")
    @params_valid(serializer=UpdateTDWSourceSerializer)
    @perm_check("flow.update", detail=True, url_key="flow_id")
    def update_tdw_source(self, request, flow_id, params):
        """
        @api {patch}  /dataflow/flow/flows/:fid/update_tdw_source 更新 tdw 存量表对应的RT
        @apiName update_tdw_result_table
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
                "output_name": "xxx",       # 输出中文名
                "description": "xxx",
                "associated_lz_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "result_table_id": xxxx
                }
        """
        res_data = FlowHandler(flow_id).update_tdw_source(params)
        return Response(res_data)

    @detail_route(methods=["post"], url_path="remove_tdw_source")
    @params_valid(serializer=RemoveTDWSourceSerializer)
    @perm_check("flow.delete", detail=True, url_key="flow_id")
    def remove_tdw_source(self, request, flow_id, params):
        """
        @api {post}  /dataflow/flow/flows/:fid/remove_tdw_source 删除tdw存量表对应的RT
        @apiName remove_tdw_source
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {}
        """
        FlowHandler(flow_id).remove_tdw_source(params)
        return Response({})

    @list_route(methods=["put"], url_path="update_job")
    @params_valid(serializer=UpdateJobSerializer)
    def update_job(self, request, params):
        """
        @api {put} /dataflow/flow/flows/update_job flow更新离线RT对应的job
        @apiName update_job
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "result_table_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            {}
        """
        username = get_request_username()
        result_table_id = params["result_table_id"]
        result_table = FlowNodeRelation.objects.filter(
            result_table_id=result_table_id,
            node_type__in=[
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
            ],
        )
        if result_table.exists():
            if len(result_table) != 1:
                raise NodeValidError("The rt(%s) has multi batch nodes" % result_table_id)
            NODE_FACTORY.get_node_handler(result_table[0].node_id).update_batch_job(username)
        else:
            raise NodeValidError("The rt(%s) does not exist" % result_table_id)
        return Response()

    @list_route(methods=["put"], url_path="update_model_app_job")
    @params_valid(serializer=UpdateJobSerializer)
    def update_model_app_job(self, request, params):
        """
        @api {put} /dataflow/flow/flows/update_model_app_job flow更新模型应用RT对应的job
        @apiName update_model_app_job
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {
                "result_table_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            {}
        """
        username = get_request_username()
        result_table_id = params["result_table_id"]
        result_table = FlowNodeRelation.objects.filter(result_table_id=result_table_id, node_type=NodeTypes.MODEL_APP)
        if result_table.exists():
            if len(result_table) != 1:
                raise NodeValidError("The rt(%s) has multi model_app nodes" % result_table_id)
            NODE_FACTORY.get_node_handler(result_table[0].node_id).update_model_app_job(username)
        else:
            raise NodeValidError("The rt(%s) does not exist" % result_table_id)
        return Response()

    @detail_route(methods=["post"], url_path="upload")
    @perm_check("flow.update")
    @params_valid(serializer=FileUploadSerializer)
    def upload(self, request, flow_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/upload 上传压缩包
        @apiName upload
        @apiGroup Flow
        @apiParamExample {multipart} 上传文件
            {
                'file': open('/root/xxx.jar', 'rb'),
                'type': 'tdw'
            }
        @apiSuccessExample {json} 成功返回
            {
                'id': 1111,      # jar包ID
                'name': '',
                'created_by': '',
                'created_at': '2019-05-08 16:35:44'
            }
        """
        uploaded_file = request.FILES["file"]
        description = params["type"]
        suffix = os.path.splitext(uploaded_file.name)[-1][1:]
        if not suffix:
            raise FileUploadedError(_("当前上传文件应包含后缀."))
        file_info = FlowHandler(flow_id).upload_file(
            get_request_username(), flow_id, uploaded_file, suffix, description
        )
        return Response(
            {
                "id": file_info["id"],
                "name": file_info["name"],
                "created_by": file_info["created_by"],
                "created_at": file_info["created_at"],
            }
        )

    @list_route(methods=["post"], url_path="create")
    @params_valid(serializer=CreateFlowWithNodesSerializer)
    @perm_check_param("project.manage_flow", "project_id")
    @perm_check("flow.create", detail=False)
    def create_flow(self, request, params):
        """
        @api {post} /dataflow/flow/flows/create 传入各信息直接初始化一个完整的flow
        @apiName create_flow
        @apiGroup Flow
        @apiDescription 具体节点参数可参考【Flow - 新增节点】config配置或用户文档
        @apiParam {list} nodes
        @apiParamExample {json}
            {
                'bk_username': 'xxx',
                'bk_app_code'： 'xxx',
                'project_id': 'xxx',
                'flow_name': 'xxx',
                'nodes': [
                    {
                        "bk_biz_id": 1,                         # 业务号
                        "id": 1,                                # 当前节点自定义id
                        "from_nodes": [                         # 上游节点信息
                            {
                                'id': 3,
                                'from_result_table_ids': ['1_xxxx']
                            }
                        ],
                        "node_type": "stream",                  # 节点类型
                        "table_name": "xxx",                    # 数据输出
                        "output_name": "xxx",                   # 输出中文名
                        "window_type": "none",                  # 窗口类型
                        "window_time": 0,                       # 窗口长度
                        "waiting_time": 0,                      # 延迟时间
                        "count_freq": 30,                       # 统计频率
                        "sql": "select * from etl_van_test5",   # sql
                        "frontend_info": {"x": 92, "y": 293}    # 前端信息(当前仅有坐标信息)，可选值
                        ...
                    },
                    {},...
                ]
            }
        @apiSuccessExample {json} 成功返回
            {
                "flow_id": 1,
                "node_ids": [2,3]                               # 成功创建的节点列表
            }
        """
        username = get_request_username()
        nodes = request.data["nodes"]
        # 为避免在校验器中根据不同节点类型定义太多required为false的字段，这里将参数验证移至此处
        # 1. 先统一校验
        nodes = validate_node_infos(nodes, True)
        # 2. 再创建空flow，并增加节点
        response = super(FlowViewSet, self).create(request)
        try:
            flow = FlowHandler(response.data["flow_id"])
            if FLOW_MAX_NODE_COUNT is not None and len(nodes) > FLOW_MAX_NODE_COUNT:
                raise FlowError(_("当前任务节点数量超出限制，最大允许数量为%s") % FLOW_MAX_NODE_COUNT)
            res = NodeController.add_nodes(flow, username, nodes)
            res["flow_id"] = flow.flow_id
        except FlowManageError as e:
            flow = FlowHandler(response.data["flow_id"])
            # 该自定义异常信息包含创建节点失败信息、回滚失败信息等所有失败信息，理论上不会有其它异常(否则直接抛出异常)
            # 若为空flow，直接删除，否则属于回滚失败类型
            NodeController.del_flow_nodes(flow, is_delete_flow=True)
            raise e
        except Exception as e:
            raise FlowManageError(_("创建任务期间出现非预期异常({})，请联系管理员".format(e)))
        return Response(res)
