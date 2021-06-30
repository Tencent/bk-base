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
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.flow import exceptions as Errors
from dataflow.flow.handlers.flow import FlowHandler, valid_flow_wrap
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowInfo
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer import serializers


class DataMakeupViewSet(APIViewSet):
    lookup_field = "data_makeup_id"

    @valid_flow_wrap("")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    @params_valid(serializer=serializers.CreateDataMakeupSerializer)
    def create(self, request, flow_id, node_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/nodes/:nid/data_makeup 提交数据补齐
        @apiName create_data_makeup
        @apiGroup Flow
        @apiDescription 提交数据补齐
        @apiParamExample {json}
            {
                "target_schedule_time": "2018-12-12 10:00:00",
                "source_schedule_time": "2018-12-12 11:00:00",
                "with_child": true,
                "with_storage": true
            }
        @apiSuccessExample {json} 成功返回
            {}
        """
        node = NODE_FACTORY.get_node_handler(node_id=node_id)
        if node.node_type not in [
            NodeTypes.BATCH,
            NodeTypes.BATCHV2,
            NodeTypes.DATA_MODEL_BATCH_INDICATOR,
        ]:
            raise Errors.FlowDataMakeupNotAllowed(_("非离线节点不支持数据补齐"))
        if node.status != FlowInfo.STATUS.RUNNING:
            raise Errors.FlowDataMakeupNotAllowed(_("所选节点%(node_name)s不处于运行状态，不支持补齐") % {"node_name": node.name})
        flow = FlowHandler(flow_id)
        head_dps = []
        if params["with_child"]:
            downstream_nodes = node.get_downstream_nodes(
                [
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                    NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                ]
            )
            running_downstream_nodes = [_n for _n in downstream_nodes if _n.status == FlowInfo.STATUS.RUNNING]
            for _node in running_downstream_nodes:
                if _node.processing_id != node.processing_id:
                    head_dps.append(_node.processing_id)
        source_schedule_time = serializers.CreateDataMakeupSerializer.get_format_time(params["source_schedule_time"])
        target_schedule_time = serializers.CreateDataMakeupSerializer.get_format_time(params["target_schedule_time"])
        flow.create_data_makeup(
            node.processing_id,
            ",".join(head_dps),
            source_schedule_time,
            target_schedule_time,
            params["with_storage"],
        )
        return Response({})

    @list_route(methods=["get"], url_path="status_list")
    @valid_flow_wrap("")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    @params_valid(serializer=serializers.BatchStatusListSerializer)
    def status_list(self, request, flow_id, node_id, params):
        """
        @api {get} /dataflow/flow/flows/:fid/nodes/:nid/data_makeup/status_list 查询执行记录
        @apiName status_list
        @apiGroup Flow
        @apiDescription 查询执行记录
        @apiParamExample {json}
            {
                "start_time": "2018-12-12 11:00:00",
                "end_time": "2018-12-12 11:00:00",
            }
        @apiSuccessExample {json} 成功返回
            [{
                "schedule_time": "xxx",
                "status": "running",
                "status_str": "成功",
                "created_at": "1544583600000",
                "updated_at": "1544583600000",
                "start_time": "2018-12-12 11:00:00",
                "end_time": "2018-12-12 11:00:00",
                "is_allowed": true
            }]
        """
        node = NODE_FACTORY.get_node_handler(node_id=node_id)
        if node.node_type not in NodeTypes.BATCH_CATEGORY:
            raise Errors.NodeError(_("非离线节点不支持当前操作"))
        res = node.get_status_list(params["start_time"], params["end_time"])
        return Response(res)

    @list_route(methods=["get"], url_path="check_execution")
    @valid_flow_wrap("")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    @params_valid(serializer=serializers.BatchCheckExecution)
    def check_execution(self, request, flow_id, node_id, params):
        """
        @api {get} /dataflow/flow/flows/:fid/nodes/:nid/data_makeup/check_execution 查看补齐结果
        @apiName check_execution
        @apiGroup Flow
        @apiDescription 查看补齐结果
        @apiParamExample {json}
            {
                "schedule_time": "2018-12-12 11:00:00"
            }
        @apiSuccessExample {json} 成功返回
            {
                "is_allowed": True
            }
        """
        node = NODE_FACTORY.get_node_handler(node_id=node_id)
        if node.node_type not in NodeTypes.BATCH_CATEGORY:
            raise Errors.NodeError(_("非离线节点不支持当前操作"))
        schedule_time = serializers.BatchCheckExecution.get_format_time(params["schedule_time"])
        res = node.check_execution(schedule_time)
        return Response(res)
