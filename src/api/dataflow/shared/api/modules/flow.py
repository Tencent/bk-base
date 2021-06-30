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

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_DATAFLOW_URL, BASE_FLOW_URL


class _FlowAPI(object):
    MODULE = "modeling"

    def __init__(self):
        self.log = DataDRFAPISet(
            url=BASE_DATAFLOW_URL + "flow/log/",
            primary_key="result_table_id",
            module=self.MODULE,
            description=_("日志相关接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "get_flow_job_submit_log": DRFActionAPI(method="get", detail=False),
                "get_flow_job_submit_log_file_size": DRFActionAPI(method="get", detail=False),
            },
        )

        self.node = DataDRFAPISet(
            url=BASE_FLOW_URL + "flows/{flow_id}/nodes/",
            primary_key="node_id",
            url_keys=["flow_id"],
            module=self.MODULE,
            description=_("node相关接口"),
        )

        self.flow = DataDRFAPISet(
            url=BASE_FLOW_URL + "flow_migration/",
            primary_key=None,
            url_keys=None,
            module=self.MODULE,
            description=_("重启flow实时节点相关接口"),
            custom_config={"op_flow_task": DRFActionAPI(method="post", detail=False)},
        )


FlowAPI = _FlowAPI()
