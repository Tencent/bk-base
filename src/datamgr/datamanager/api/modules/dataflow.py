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
from api.base import DataDRFAPISet, DRFActionAPI
from api.utils import add_dataapi_inner_header, add_esb_common_params
from conf.settings import DATAFLOW_API_URL


class DataflowApi(object):

    MODULE = "DATAFLOW"

    def __init__(self):
        self.batch_interactive_servers = DataDRFAPISet(
            url=DATAFLOW_API_URL + "batch/interactive_servers/",
            primary_key="server_id",
            module="dataflow",
            description="离线SessionServer管理接口",
            custom_config={
                "session_status": DRFActionAPI(method="get", detail=True),
            },
        )

        self.batch_interactive_servers_codes = DataDRFAPISet(
            url=DATAFLOW_API_URL + "batch/interactive_servers/{server_id}/codes/",
            url_keys=["server_id"],
            primary_key="code_id",
            module="dataflow",
            description="离线SessionServer代码接口",
            custom_config={
                "cancel": DRFActionAPI(method="post", detail=True),
            },
        )

        self.flows = DataDRFAPISet(
            url=DATAFLOW_API_URL + "flow/flows/",
            primary_key="flow_id",
            module="dataflow",
            description="DataFlow 接口集",
            custom_config={
                "start": DRFActionAPI(method="post", detail=True),
            },
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
        )
