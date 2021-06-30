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
from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.flow.exceptions import FlowError
from dataflow.flow.handlers.add_partition import AddPartitionHandler
from dataflow.flow.handlers.flow_utils import param_verify

# 迁移相关
from dataflow.flow.migration.migration import add_execute_log, add_flow, add_flow_by_yaml
from dataflow.flow.migration.serializer import MigrationCreateFlowSerializer, MigrationDataflowExecuteLogSerializer
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer.serializers import AddPartitionSerializer, OpFlowSerializer, ParamVerifySerializer
from dataflow.flow.tasks import op_with_flow_task_by_id
from dataflow.pizza_settings import EXPAND_PARTITION_DOWNSTREAM_NODE_TYPES
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.permission import require_username
from dataflow.shared.send_message import send_message


class FlowMigrationTask(APIViewSet):
    @list_route(methods=["post"], url_path="create")
    @params_valid(serializer=MigrationCreateFlowSerializer)
    @perm_check("flow.create", detail=False)
    def create_flow(self, request, params):
        """
        @api {post} /dataflow/flow/flow_migration/create/
        @apiName create_flow
        @apiGroup Flow
        @apiDescription 把完整的点和线信息插入一个flow
        @apiParam {list} nodes 外部的参数用于插入表FlowInfo, 用于插入点FlowNodeInfo
        @apiParam {list} links 外部的参数用于插入表FlowInfo, 用于插入线FlowLinkInfo
        @apiParamExample {json}
            {
                'flow_id': '',
                'flow_name': '',
                'project_id': '',
                'status': '',
                'is_locked': '',
                'latest_version': '',
                'bk_app_code': '',
                'active': '',
                'created_by': '',
                'created_at': '',
                'locked_by': '',
                'locked_at': '',
                'updated_by': '',
                'updated_at': '',
                'description': '',
                'nodes': [
                    {
                        'id': 1,
                        'flow_id': '',
                        'node_name': '',
                        'node_config': '',
                        'node_type': '',
                        'frontend_info': '',
                        'status': '',
                        'latest_version': '',
                        'running_version': '',
                        'created_by': '',
                        'created_at': '',
                        'updated_by': '',
                        'updated_at': '',
                        'description': ''
                    }
                ],
                'links': [
                    {
                        'flow_id': '',
                        'from_node_id': '',
                        'to_node_id': '',
                        'frontend_info': {
                            'source': {
                                'node_id': '',
                                'id': 'ch_' + '',
                                'arrow': 'Right'
                            },
                            'target': {
                                'id': 'ch_' + '',
                                'arrow': 'Left'
                            }
                        }
                    }
                ]
            }
        @apiSuccessExample {json} 成功返回
            {
                  'result': 'success'
            }
        """
        with auto_meta_sync(using="default"):
            add_flow(params)
        return Response()

    @list_route(methods=["post"], url_path="execute_log")
    @params_valid(serializer=MigrationDataflowExecuteLogSerializer)
    @perm_check("flow.update", detail=False)
    def add_execute_log(self, request, params):
        """
        @api {post} /dataflow/flow/flow_migration/execute_log/
        @apiName add_execute_log
        @apiGroup Flow
        @apiDescription 添加dataflow_execute_log表
        @apiParam {list} logs 添加 dataflow_execute_log 表所需的字段
        @apiParamExample {json}
        {
            'flow_id': flow_id,
            'logs': [{
                'id': '',
                'action': '',
                'status': '',
                'start_time': '',
                'end_time': '',
                'created_at': '',
                'created_by': '',
                'description': '',
                'version': '',
                'context': '',
                'logs_zh': '',
                'logs_en': ''
            }]
        }
        @apiSuccessExample {json} 成功返回
            {
                  'result': 'success'
            }
        """
        add_execute_log(params)
        return Response()

    @list_route(methods=["post"], url_path="create_by_yaml")
    @params_valid(serializer=MigrationCreateFlowSerializer)
    @perm_check("flow.create", detail=False)
    def create_flow_by_yaml(self, request, params):
        """
        @api {post} /dataflow/flow/flow_migration/create_by_yaml/
        @apiName create_flow
        @apiGroup Flow
        @apiDescription 把完整的点和线信息插入一个flow, 信息来自旧官网yaml配置
        @apiParam {list} nodes 外部的参数用于插入表FlowInfo, 用于插入点FlowNodeInfo
        @apiParam {list} links 外部的参数用于插入表FlowInfo, 用于插入线FlowLinkInfo
        @apiParamExample {json}
            {
                'flow_id': '',
                'flow_name': '',
                'project_id': '',
                'status': '',
                'is_locked': '',
                'latest_version': '',
                'bk_app_code': '',
                'active': '',
                'created_by': '',
                'created_at': '',
                'locked_by': '',
                'locked_at': '',
                'updated_by': '',
                'updated_at': '',
                'description': '',
                'nodes': [
                    {
                        'id': 1,
                        'flow_id': '',
                        'node_name': '',
                        'node_config': '',
                        'node_type': '',
                        'frontend_info': '',
                        'status': '',
                        'latest_version': '',
                        'running_version': '',
                        'created_by': '',
                        'created_at': '',
                        'updated_by': '',
                        'updated_at': '',
                        'description': ''
                    }
                ],
                'links': [
                    {
                        'flow_id': '',
                        'from_node_id': '',
                        'to_node_id': '',
                        'frontend_info': {
                            'source': {
                                'node_id': '',
                                'id': 'ch_' + '',
                                'arrow': 'Right'
                            },
                            'target': {
                                'id': 'ch_' + '',
                                'arrow': 'Left'
                            }
                        }
                    }
                ]
            }
        @apiSuccessExample {json} 成功返回
            {
                  'result': 'success'
            }
        """
        with auto_meta_sync(using="default"):
            flow_id = add_flow_by_yaml(params)
        return Response({"flow_id": flow_id})

    @list_route(methods=["post"], url_path="op_flow_task")
    @params_valid(serializer=OpFlowSerializer)
    @require_username
    def op_flow_task(self, request, params):
        """
        @api {post} /dataflow/flow/flow_migration/op_flow_task/
        @apiName op_flow_task
        @apiGroup Flow
        @apiDescription 对指定的flow下的计算节点进行操作（启动/停止）
        @apiParam {int} flow_id
        @apiParam {string} action 动作（start/stop）
        @apiParam {string} op_type 操作类型（实时、离线、算法节点等）
        @apiParamExample {json} 请求参数示例
        {
            'flow_id': 'result_table_id',
            'action': 'start/stop' ,
            'op_type': 'offline' # 可不填该参数,默认为realtime
        }
        @apiSuccessExample {json} 返回结果
        {
            'errors':null,
            'message':'ok',
            'code':'1500200',
            'data':true,
            'result':true
        }
        """
        operator = get_request_username()
        flow_id = params["flow_id"]
        action = params["action"]
        # 默认操作实时节点
        if "op_type" in params:
            op_type = params["op_type"]
        else:
            op_type = NodeTypes.STREAM
        success = op_with_flow_task_by_id(flow_id, action, op_type, operator)
        if success:
            result = "成功"
        else:
            result = "失败!"
        title = "《IEG数据平台》操作计算节点通知"
        content = _(
            "通知类型: {message_type}<br>"
            "任务ID: {flow_id}<br>"
            "计算节点类型: {op_type}<br>"
            "操作动作: {action}<br>"
            "操作结果: {result}<br>"
        ).format(
            message_type="启动/关停计算节点",
            flow_id=flow_id,
            op_type=op_type,
            action=action,
            result=result,
        )
        try:
            send_message([operator], title, content, body_format="Html")
        except FlowError as flow_exception:
            logger.exception(flow_exception)
            logger.warning("发送<<启动/关停实时计算节点>>邮件失败")
        return Response(success)


class ManageAddPartition(APIViewSet):
    @list_route(methods=["post"], url_path="check")
    @params_valid(serializer=AddPartitionSerializer)
    @require_username
    def check_add_partition(self, request, params):
        """
        @api {post} /dataflow/flow/manage/add_partition/check/
        @apiName add_partition
        @apiGroup Flow
        @apiDescription 扩充指定rt相关数据分区操作,会重启直接下游的分发节点及相关节点类型（downstream_node_types）的任务。
            1.如果是清洗表，则同一个raw_data下的多个清洗表都会扩到相同分区；2.如果rt是清洗表，则会同时扩对应raw_data的分区数。
        @apiParam {string} result_table_id 操作的rt id
        @apiParam {int} partitions 分区扩充的目标数量
        @apiParam {string} [downstream_node_types=realtime,model,process_model] 操作下游的节点类型，以逗号分隔，
            默认realtime,model,process_model
        @apiParam {bool} [strict_mode=true] 是否严格模式，源和目标分区数相同时，strict为true时不操作退出，
            为false时可以重启节点，默认true
        @apiParam {string} bk_username 发起操作的用户名
        @apiParamExample {json} 请求参数示例
        {
            'result_table_id': 'result_table_id',
            'partitions': 2,
            'downstream_node_types': 'realtime,model,process_model',
            'strict_mode': true
            'bk_username': 'xxx'
        }
        @apiSuccessExample {json} 扩充分区返回结果
        {
            'errors':null,
            'message':'ok',
            'code':'1500200',
            'data':null,
            'result':true
        }
        """
        # from common.local import set_local_param
        # set_local_param('bk_username', SYSTEM_ADMINISTRATORS[0])
        operator = get_request_username()

        result_table_id = params["result_table_id"]
        partitions = params["partitions"]
        downstream_node_types = None
        strict_mode = True
        if params.get("downstream_node_types"):
            downstream_node_types = params["downstream_node_types"]
        if not downstream_node_types:
            downstream_node_types = EXPAND_PARTITION_DOWNSTREAM_NODE_TYPES
        if "strict_mode" in list(params.keys()):
            strict_mode = params["strict_mode"]

        logger.info(
            "check_add_partition params: result_table_id(%s), partitions(%s), downstream_node_types(%s), "
            "strict_mode(%s)" % (result_table_id, partitions, downstream_node_types, strict_mode)
        )
        add_partition_handler = AddPartitionHandler(
            operator, result_table_id, partitions, downstream_node_types, strict_mode
        )
        add_partition_handler.add_partition()
        return Response()


class ParamVerifyUtil(APIViewSet):
    @list_route(methods=["post"], url_path="check")
    @params_valid(serializer=ParamVerifySerializer)
    def param_verify(self, request, params):
        """
        @api {post} /dataflow/flow/param_verify/check/ 节点参数校验
        @apiName param_verify
        @apiGroup Flow
        @apiParam {string} scheduling_type 调度类型，batch/stream
        @apiParam {json} scheduling_content 调度参数，窗口配置等
        @apiParam {json} from_nodes 上游的节点调度配置
        @apiParam {json} to_nodes 下游的节点调度配置
        @apiParamExample {json} 请求参数示例
        {
            'scheduling_type': 'batch',
            'scheduling_content': 'xxxxxx',
            'from_nodes': '[{'scheduling_type':'batch', 'scheduling_content': {}}]',
            'to_nodes': '[{'scheduling_type':'batch', 'scheduling_content': {}}]'
        }
        @apiSuccessExample {json} 节点参数校验返回结果
        {
            'errors':null,
            'message':'ok',
            'code':'1500200',
            'data':null,
            'result':true
        }
        """
        verify_result = param_verify(params)
        return Response(verify_result)
