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


from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import DATAFLOW_APIGATEWAY_ROOT

from ..base import DataAPI, DataDRFAPISet, DRFActionAPI, PassThroughAPI


class _DataFlowApi(object):
    MODULE = _("数据平台计算模块")

    URL_PREFIX = DATAFLOW_APIGATEWAY_ROOT

    @staticmethod
    def after_create_flow(response_result):
        # 创建flow后补充状态字段为默认值，便于前端进行统一状态显示
        response_result["data"].update({"has_exception": False})
        return response_result

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.batch_hdfs_result_tables = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "batch/hdfs/result_tables/",
            module=self.MODULE,
            primary_key="result_table_id",
            before_request=add_esb_info_before_request,
            custom_config={
                "new_line": DRFActionAPI(method="GET", detail=True, description="获取结果表在hdfs上的最新数据"),
            },
        )

        self.flow_flows = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/",
            module=self.MODULE,
            primary_key="flow_id",
            before_request=add_esb_info_before_request,
            custom_config={
                "latest_deploy_data": DRFActionAPI(method="GET", detail=True, description="获取最近部署信息"),
                "deploy_data": DRFActionAPI(method="GET", detail=True, description="获取flow部署信息"),
                "monitor_data": DRFActionAPI(method="GET", detail=True, description="获取监控打点信息"),
                "list_rela_by_rtid": DRFActionAPI(method="GET", detail=False, description="通过rtid获取DataFlow列表"),
                "versions": DRFActionAPI(method="GET", detail=True, description="获取DataFlow历史版本"),
                "monitor": DRFActionAPI(method="GET", detail=True, description="查询DataFlow监控状态信息"),
            },
        )

        self.flow_flows_graph = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/graph/",
            module=self.MODULE,
            primary_key=None,
            url_keys=["flow_id"],
            before_request=add_esb_info_before_request,
            custom_config={
                "retrieve": DRFActionAPI(method="GET", detail=False, description="获取graph"),
                "update": DRFActionAPI(method="PUT", detail=False),
            },
        )

        self.flow_flows_nodes = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/nodes/",
            module=self.MODULE,
            primary_key="node_id",
            url_keys=["flow_id"],
            before_request=add_esb_info_before_request,
            custom_config={},
        )

        self.flow_flows_projects = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/projects/",
            module=self.MODULE,
            primary_key="flow_id",
            before_request=add_esb_info_before_request,
            custom_config={
                "count": DRFActionAPI(method="GET", detail=False, description="通过project_id获取flow相关数量，支持批量"),
            },
        )

        self.flow_nodes = DataDRFAPISet(
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/nodes/",
            module=self.MODULE,
            primary_key="node_id",
            before_request=add_esb_info_before_request,
            custom_config={
                "calculate_nodes": DRFActionAPI(method="GET", detail=False, description="通过结果表查询得到计算节点，支持批量"),
                "monitor_data": DRFActionAPI(method="GET", detail=True, description="获取监控打点信息"),
                "storage_nodes": DRFActionAPI(method="GET", detail=False, description="通过结果表查询得到存储节点，支持批量"),
            },
        )
        # 通过rt查询任务id
        self.processing_nodes = DataAPI(
            method="GET",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/nodes/processing_nodes/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="通过RT获取计算节点",
        )
