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
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.flow.deploy.deploy_driver import deploy
from dataflow.flow.exceptions import FlowCustomCalculateNotAllowed
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.models import FlowDebugLog, FlowExecuteLog
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer.serializers import (
    ApplyCustomCalculateJobSerializer,
    DeploySerializer,
    QueryDebugDetailSerializer,
)
from dataflow.flow.tasks import (
    check_custom_calculate,
    start_custom_calculate,
    start_debugger,
    stop_custom_calculate,
    terminate_debugger,
)
from dataflow.flow.tasks.debug_flow import FlowDebuggerHandler
from dataflow.flow.utils.ftp_server import FtpServer
from dataflow.pizza_settings import HDFS_UPLOADED_FILE_TMP_DIR
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.permission import require_username
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class DeployView(APIViewSet):
    def hdfs_uploaded_file_clean(self, request):
        """
        @api {post}  /dataflow/flow/hdfs_uploaded_file_clean 清理上传的临时文件
        @apiName hdfs_uploaded_file_clean
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {}
        """
        geog_area_codes = TagHelper.list_tag_geog()
        hdfs_default_clusters_groups = []
        for geog_area_code in geog_area_codes:
            hdfs_default_cluster = StorekitHelper.get_default_storage("hdfs", geog_area_code, allow_empty=True)
            if hdfs_default_cluster:
                hdfs_default_clusters_groups.append(hdfs_default_cluster["cluster_group"])
        res_data = {}
        for hdfs_default_clusters_group in hdfs_default_clusters_groups:
            ftp_server = FtpServer(hdfs_default_clusters_group)
            clean_expired_res_data = ftp_server.clean_expired_file(HDFS_UPLOADED_FILE_TMP_DIR)
            clean_unused_res_data = ftp_server.clean_unused_uploaded_file()
            res_data[hdfs_default_clusters_group] = {
                "expired": clean_expired_res_data,
                "unused": clean_unused_res_data,
            }
        return Response(res_data)

    def check_custom_calculate(self, request):
        logger.info("开始检查补算任务")
        check_custom_calculate()
        return Response()

    @params_valid(serializer=DeploySerializer)
    def deploy(self, request, params):
        """
        @api {get} /dataflow/flow/deploy 部署发布
        @apiName deploy
        @apiGroup batch
        @apiVersion 1.0.0
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "1500200",
                }
        """
        geog_area_code = params["geog_area_code"]
        deploy(geog_area_code)
        return Response()


class DebuggerViewSet(APIViewSet):
    lookup_field = "debugger_id"
    lookup_value_regex = r"\d+"

    # 根据实际的debugger_id查询调试信息，该API暂时未使用
    @detail_route(methods=["get"], url_path="debug_detail")
    @params_valid(serializer=QueryDebugDetailSerializer)
    @perm_check("flow.retrieve", url_key="flow_id")
    def query_debug_detail(self, request, flow_id, debugger_id, params):
        """
        @api {get} /dataflow/flow/flows/:fid/debuggers/:debugger_id/debug_detail 查询某个节点的调试详情
        @apiName query_debugger_detail
        @apiGroup Flow
        @apiParam {int} node_id 节点 ID
        @apiParamExample {json}
            {
                'node_id': 1
            }
        @apiSuccessExample {json} 成功返回
            {
                "status": "failure",
                "status_display": "调试成功" | "调试失败" | "调试中",
                "logs": []
            }
        """
        node_id = params["node_id"]
        FlowHandler(flow_id).check_node_id_in_flow(node_id)
        debug_detail = FlowDebuggerHandler(debugger_id).query_debug_detail(node_id=node_id)
        return Response(debug_detail)

    @perm_check("flow.retrieve", url_key="flow_id")
    def retrieve(self, request, flow_id, debugger_id):
        """
        @api {get} /dataflow/flow/flows/:fid/debuggers/:debugger_id 获取调试信息
        @apiName query_debugger
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                "nodes_info": [],
                "status": "failure",
                "status_display": "调试失败",
                "is_done": true,
                "logs": []
            }
        """
        debugger = FlowDebugLog.objects.get(id=debugger_id)
        nodes_info = debugger.get_nodes_info()
        l_nodes_info = []
        # 正在调试及其状态会在当前信息列表中
        for _node_id, _info in list(nodes_info.items()):
            l_nodes_info.append(
                {
                    "node_id": int(_node_id),
                    "status": _info["status"],
                    "status_display": FlowDebugLog.M_STATUS_CHOICES[_info["status"]],
                }
            )

        # 对于暂不支持调试的节点，统一返回固定标识
        for _n in FlowHandler(flow_id=flow_id).nodes:
            # 除非节点为接了下游节点的 HDFS 存储，否则若节点配置为不支持调试，即不支持调试
            if _n.node_type in [NodeTypes.HDFS_STORAGE] and len(_n.get_to_nodes_handler()) > 0:
                continue
            if not _n.is_support_debug:
                l_nodes_info.append(
                    {
                        "node_id": _n.node_id,
                        "status": "disabled",
                        "status_display": _("该类型节点暂不支持调试"),
                    }
                )

        return Response(
            {
                "nodes_info": l_nodes_info,
                "logs": debugger.get_logs(),
                "status": debugger.status,
                "status_display": debugger.status_display,
                "is_done": debugger.is_done,
            }
        )

    @list_route(methods=["post"], url_path="start")
    @perm_check("flow.execute", url_key="flow_id")
    @require_username
    def start_debugger(self, request, flow_id):
        """
        @api {post} /dataflow/flow/flows/:fid/debuggers/start 开始调试flow
        @apiName start_debugger
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                '1111'
            }
        """
        operator = get_request_username()
        debugger_id = start_debugger(flow_id, operator)
        return Response(debugger_id)

    @detail_route(methods=["post"], url_path="stop")
    @perm_check("flow.retrieve", url_key="flow_id")
    @require_username
    def stop_debugger(self, request, flow_id, debugger_id):
        """
        @api {post} /dataflow/flow/flows/:fid/debuggers/:debugger_id/stop 停止调试flow
        @apiName stop_debugger
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'task_id': 1111
            }
        """
        operator = get_request_username()
        terminate_debugger(debugger_id, operator)
        return Response(None)


class CustomCalcalculateViewSet(APIViewSet):
    lookup_field = "custom_calculate_id"
    lookup_value_regex = "[a-z_0-9]+"

    @list_route(methods=["post"], url_path="apply")
    @params_valid(serializer=ApplyCustomCalculateJobSerializer)
    @perm_check("flow.execute", detail=True, url_key="flow_id")
    @require_username
    def apply(self, request, flow_id, params):
        """
        @api {post}  /dataflow/flow/flows/:fid/custom_calculates/apply 申请
        @apiName apply_custom_calculate
        @apiGroup Flow
        @apiParam {list} node_ids
        @apiParam {bool} with_child
        @apiParam {string} start_time
        @apiParam {string} end_time
        @apiParamExample {json} 参数样例:
            {
                "node_ids": [1, 2, 3],
                "start_time": "2018-12-12 11:00",
                "end_time": "2018-12-12 11:00",
                "with_child": true
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "status": "applying"
                    "node_info": {
                        1: {"status": "applying"}
                    }
                }
        """
        o_flow = FlowHandler(flow_id)
        # 若当前无补算任务或补算任务已结束，创建补算任务，待审批完成后回调
        if o_flow.custom_calculate_id:
            task_info = o_flow.custom_calculate_task
            if task_info and task_info.status not in FlowExecuteLog.FINAL_STATUS:
                raise FlowCustomCalculateNotAllowed(_("当前任务存在补算任务，不允许重复申请"))
            elif not task_info:
                logger.warning("当前任务存在补算ID(%s)，但流水表中不存在该记录(可能被清除)" % o_flow.custom_calculate_id)
        try:
            o_flow.lock("补算流程中")
            node_ids = params["node_ids"]
            start_time = params["start_time"]
            end_time = params["end_time"]
            with_child = params["with_child"]
            res = o_flow.apply_custom_calculate_job(node_ids, start_time, end_time, with_child)
            return Response(res)
        except Exception as e:
            logger.exception(e)
            o_flow.unlock()
            raise e

    @list_route(methods=["post"], url_path="cancel")
    @perm_check("flow.execute", detail=True, url_key="flow_id")
    @require_username
    def cancel(self, request, flow_id):
        """
        @api {post}  /dataflow/flow/flows/:fid/custom_calculates/cancel 撤销补算任务的申请
        @apiName cancel_custom_calculate
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {}
        """
        FlowHandler(flow_id).cancel_custom_calculate_job()
        return Response({})

    @perm_check("flow.retrieve", url_key="flow_id")
    def retrieve(self, request, flow_id, recalculate_id):
        """
        @api {get} /dataflow/flow/flows/:fid/custom_calculates/:recalculate_id 获取当前flow重算任务状态
        @apiName query_custom_calculate
        @apiGroup Flow
        @apiParamExample {json} 成功返回
            {
                "status": "running"
            }
        """
        flow = FlowHandler(flow_id)
        return Response(flow.get_recalculate_basic_info())

    @list_route(methods=["post"], url_path="start")
    @perm_check("flow.execute", url_key="flow_id")
    @require_username
    def start(self, request, flow_id):
        """
        @api {post} /dataflow/flow/flows/:fid/custom_calculates/start 提交补算任务
        @apiName start_recalculate
        @apiGroup Flow
        @apiParamExample
            {}
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "task_id": 1,
                    "status": "running"
                    "node_info": {
                        1: {"status": "running"}
                    }
                }
        """
        operator = get_request_username()
        res = start_custom_calculate(flow_id, operator)
        return Response(res)

    @list_route(methods=["post"], url_path="stop")
    @perm_check("flow.execute", url_key="flow_id")
    def stop(self, request, flow_id):
        """
        @api {post} /dataflow/flow/flows/:fid/custom_calculates/stop 停止补算任务
        @apiName stop_custom_calculate
        @apiGroup Flow
        @apiParamExample {json} 成功返回
            {}
        """
        operator = get_request_username()
        stop_custom_calculate(flow_id, operator)
        return Response({})


class TaskViewSet(APIViewSet):
    lookup_field = "task_id"
    lookup_value_regex = r"\d+"

    @perm_check("flow.retrieve", url_key="flow_id")
    def retrieve(self, request, flow_id, task_id):
        """
        @api {get} /dataflow/flow/flows/:fid/tasks/:task_id 获取 celery 任务信息
        @apiName get_task_info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            {
                'id': 1,
                'flow_id': 1,
                'action': 'start',
                'status': 'success',
                'logs_zh': 'xxx',
                'logs_en': 'xxx',
                'start_time': 'xxx',
                'end_time': 'xxx',
                'context': 'xxx',
                'version': 'xxx',
                'created_by': 'xxx',
                'created_at': 'xxx',
                'description': 'xxx'
            }
        """
        tasks = FlowExecuteLog.objects.filter(id=task_id)
        if tasks.exists():
            task_info = model_to_dict(tasks[0])
            task_info["logs"] = tasks[0].get_logs()

            return Response(task_info)

        return Response(None)
