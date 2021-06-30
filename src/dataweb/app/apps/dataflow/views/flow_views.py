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
import json

from django.utils.translation import ugettext as _
from rest_framework import serializers
from rest_framework.response import Response

from apps import exceptions
from apps.api import DataFlowApi
from apps.common.views import detail_route
from apps.dataflow.handlers.flow import Flow
from apps.dataflow.permissions import FlowPermissions
from apps.generic import APIViewSet
from apps.utils.time_handler import timestamp_to_timeformat


class FlowViewSet(APIViewSet):
    serializer_class = serializers.Serializer
    lookup_value_regex = r"\d+"
    lookup_field = "flow_id"
    permission_classes = (FlowPermissions,)

    class ListSerializer(serializers.Serializer):
        search = serializers.CharField(required=False)
        add_node_count_info = serializers.IntegerField(required=False)
        add_exception_info = serializers.IntegerField(required=False)
        add_process_status_info = serializers.IntegerField(required=False)

    @detail_route(methods=["get"], url_path="check")
    def check(self, request, flow_id=None):
        return Response(True)

    @detail_route(methods=["get"], url_path="get_latest_deploy_info")
    def get_latest_deploy_info(self, request, flow_id=None):
        """
        @api {post} /flows/:flow_id/get_latest_deploy_info/
            获取 DataFlow 最新部署详情
        @apiName get_latest_deploy_info
        @apiGroup DataFlow
        @apiSuccessExample {json} 成功返回
            {
                'status': 'failure',
                'logs': [
                    {'message': '\\u9501\\u5b9a DataFlow', 'level': 'INFO',
                     'time': '2017-10-08 20:43:44'},
                    {'message': '--- \\u5f00\\u59cb\\u542f\\u52a8 DataFlow ---', 'level': 'INFO',
                     'time': '2017-10-08 20:43:44'},
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
                'flow_id': 1,
                'created_by': 'user00',
                'op_type':
                'start',
                'id': 57,
                'nodes_status': [
                    {
                        'node_id': 111,
                        'status': 'no-start' # 仅 no-start，running 两种状态
                        'alert_status': 'danger' # info, warning, danger 三种告警状态
                    }
                ]
            }
        """
        api_params = {"flow_id": flow_id}
        data = DataFlowApi.flow_flows.latest_deploy_data(api_params)
        if data is None:
            return Response(None)

        o_flow = Flow(flow_id=flow_id)

        data["nodes_status"] = [
            {"node_id": _n["node_id"], "status": _n["status"], "alert_status": _n["alert_status"]}
            for _n in o_flow.nodes
        ]
        data["flow_status"] = o_flow.status
        return Response(data)

    @detail_route(methods=["get"], url_path="data_monitor")
    def data_monitor(self, request, flow_id=None):
        """
        @api {get} /flows/:flow_id/data_monitor/ flow监控打点
        @apiName data_monitor
        @apiGroup DataFlow
        @apiSuccessExample {json} 成功返回
        {
            "1126": {
                "output": null
            },
            "1127": {
                "input": 58,
                "output": 58
            },
            "-1": {},
            "1153": {
                "start_time": "2018-03-13 20:28:37",
                "interval": 3600
            },
            "1152": {
                "input": 59
            }
        }
        """
        # 计算节点、存储节点 （data_loss_io_total获取） 离线节点 （rt_status）
        monitor_types = ["data_loss_io_total", "rt_status"]
        api_param = {
            "flow_id": flow_id,
            "monitor_type": ",".join(monitor_types),
            # detail_time_filter 可选参数，以下为默认值
            "detail_time_filter": json.dumps(
                [
                    {"monitor_type": "data_loss_io_total", "interval": "15m"},
                ]
            ),
        }
        data = DataFlowApi.flow_flows.monitor_data(api_param)
        integration = {}
        for monitor_type, value in list(data.items()):
            integration.update(data[monitor_type])
        return Response(integration)

    @detail_route(methods=["get"], url_path="list_alert")
    def list_alert(self, request, flow_id=None):
        """
        @api {get} /flows/:flow_id/list_alert/ 获取告警信息
        @apiName list_alert
        @apiGroup DataFlow
        @apiSuccessExample {json} 成功返回
            [
                {
                    "msg": "离线任务在调度周期[2017-10-28 15:00:00 - 2017-10-28 16:00:00]内执行失败",
                    "start_time": "2017-10-28 16:00:00",
                    "node_id": 2092,
                    "type": "offline",
                    "name": "3_xzOfflinet4"
                }
            ]
        """

        monitor_type = "dmonitor_alerts"
        api_param = {
            "flow_id": flow_id,
            "monitor_type": monitor_type,
        }
        alerts = DataFlowApi.flow_flows.monitor_data(api_param)[monitor_type]
        # 时间戳转化
        for alert in alerts:
            alert.update({"time": timestamp_to_timeformat(alert["time"])})
        return Response(alerts)

    @detail_route(methods=["post"], url_path="update_graph")
    def update_graph(self, request, flow_id=None):
        """
        @api {post} /flows/:flow_id/update_graph 更新 DataFlow 图信息
        @apiName update_graph
        @@apiDescription 更新图信息，支持多个操作
            批量节点更新（update_nodes）
            批量删除连线（delete_lines）
            批量新增连线（create_lines）
            批量更新图信息（save_frontend_graph）

        @apiGroup DataFlow
        @apiParamExample {json} 批量更新节点配置
            {
                action: 'update_nodes'
                data: [
                    {
                        node_id: 111,
                        frontend_info:{x: 11, y: 111}
                    }
                ]

            }
        @apiParamExample {json} 批量删除连线
            {
                action: 'delete_lines',
                data: [
                    {
                        from_node_id: 111,
                        to_node_id: 222
                    }
                ]
            }
        @apiParamExample {json} 批量新增连线
            {
                action: 'create_lines',
                data: [
                    {
                        from_node_id: 111,
                        to_node_id: 222,
                        frontend_info: {
                            source: {
                                'arrow': 'Left',
                                ...
                            },
                            target: {
                                'arrow': 'Right',
                                ...
                            }
                        }
                    }
                ]
            }
        @apiParamExample {json} 批量更新图信息
            {
                action: 'save_frontend_graph',
                data: {
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
            }
        """
        action = request.data["action"]
        data = request.data["data"]

        valid_actions = ["update_nodes", "delete_lines", "create_lines", "save_frontend_graph"]
        if action not in valid_actions:
            raise exceptions.FormError(_("非法操作类型，目前仅支持（%s）") % " | ".join(valid_actions))

        if action == "save_frontend_graph":
            return Response(Flow(flow_id=flow_id).save_frontend_graph(data))
        else:
            api_param = {"action": action, "data": data, "flow_id": flow_id}
            return Response(DataFlowApi.flow_flows_graph.update(api_param))

    @detail_route(methods=["get"], url_path="list_monitor_data")
    def list_monitor_data(self, request, flow_id=None):
        """
        @api {get} /flows/:flow_id/list_monitor_data 获取监控打点信息
        @apiName flow_list_monitor_data
        @apiGroup DataFlow

        @apiParam {String} monitor_type 监控类别（dmonitor_alerts-告警信息，data_loss_io_total-运行信息）
        @apiParam {Json} detail_time_filter 时间过滤条件
        @apiParamExample {json} 获取flow告警信息
            {
                monitor_type: 'dmonitor_alerts'
                detail_time_filter: "[{
                    "monitor_type": "dmonitor_alerts",
                    "interval":"1d"
                }]"
            }
        @apiParamExample 获取flow运行信息
            {
                monitor_type: 'data_loss_io_total'
                detail_time_filter: "[{
                    "monitor_type": "data_loss_io_total",
                    "interval":"1d"
                }]"
            }
        """
        monitor_type = request.query_params.get("monitor_type", "")
        detail_time_filter = request.query_params.get("detail_time_filter")
        api_param = {"flow_id": flow_id, "monitor_type": monitor_type, "detail_time_filter": detail_time_filter}
        data = DataFlowApi.flow_flows.monitor_data(api_param)
        return Response(data)
