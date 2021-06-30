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


from datamanage.pizza_settings import BKSQL_CONVERT_API_ROOT, DATAFLOW_API_ROOT

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI


class _DataflowApi(object):
    def __init__(self):
        self.get_batch_job_param = DataAPI(
            url=DATAFLOW_API_ROOT + "/batch/jobs/{job_id}/get_param/",
            url_keys=["job_id"],
            method="GET",
            module="dataflow",
            description="获取离线作业的参数",
        )

        self.flows = DataDRFAPISet(
            url=DATAFLOW_API_ROOT + "/flow/flows/",
            primary_key="flow_id",
            module="dataflow",
            description="flow信息接口",
            custom_config={
                "create_by_nodes": DRFActionAPI(method="post", url_path="create", detail=True),
            },
        )

        self.flow_nodes = DataDRFAPISet(
            url=DATAFLOW_API_ROOT + "/flow/flows/{flow_id}/nodes/",
            url_keys=["flow_id"],
            primary_key="node_id",
            module="dataflow",
            description="数据流节点接口",
        )

        self.flink_sql_check = DataAPI(
            url=BKSQL_CONVERT_API_ROOT + "/convert/flink-sql-check/",
            method="POST",
            module="dataflow",
            description="flink校验sql接口",
        )

        self.spark_sql_check = DataAPI(
            url=BKSQL_CONVERT_API_ROOT + "/convert/spark-sql/",
            method="POST",
            module="dataflow",
            description="spark校验sql接口",
        )

        self.interactive_servers = DataDRFAPISet(
            url=DATAFLOW_API_ROOT + "/batch/interactive_servers/",
            primary_key="server_id",
            module="dataflow",
            description="Session server接口",
            custom_config={
                "session_status": DRFActionAPI(method="get", detail=True),
            },
        )

        self.interactive_codes = DataDRFAPISet(
            url=DATAFLOW_API_ROOT + "/batch/interactive_servers/{server_id}/codes/",
            primary_key="code_id",
            url_keys=["server_id"],
            module="dataflow",
            description="Session server代码执行接口",
        )

        self.param_verify = DataAPI(
            url=DATAFLOW_API_ROOT + "/flow/param_verify/check/",
            method="POST",
            module="dataflow",
            description="flow节点调度内容参数校验",
        )


DataflowApi = _DataflowApi()
