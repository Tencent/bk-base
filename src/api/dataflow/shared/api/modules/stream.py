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

from dataflow.pizza_settings import BASE_STREAM_URL

from .test.test_call_stream import TestStream


class _StreamApi(object):

    test_stream = TestStream()

    def __init__(self):

        # 默认会带有create方法
        self.jobs = DataDRFAPISet(
            url=BASE_STREAM_URL + "jobs/",
            module="stream.job",
            primary_key="job_id",
            description=_("流程任务资源操作集合"),
            default_return_value=self.test_stream.set_return_value("jobs"),
            before_request=add_app_info_before_request,
            custom_config={
                "lock": DRFActionAPI(method="post"),
                "unlock": DRFActionAPI(method="post"),
                "check_topology_status_before_start": DRFActionAPI(method="get"),
                "code_version": DRFActionAPI(method="get"),
                "register": DRFActionAPI(method="post"),
                "submit": DRFActionAPI(
                    method="post",
                    default_return_value=self.test_stream.set_return_value("jobs_submit"),
                    timeout_auto_retry=True,
                ),
                "sync_status": DRFActionAPI(method="get"),
                "cancel": DRFActionAPI(
                    method="post", default_return_value=self.test_stream.set_return_value("jobs_cancel")
                ),
                "force_kill": DRFActionAPI(method="post"),
                "create_single_processing": DRFActionAPI(detail=False, method="post"),
                "update_single_processing": DRFActionAPI(method="put"),
                "start": DRFActionAPI(method="post"),
            },
        )

        self.processings = DataDRFAPISet(
            url=BASE_STREAM_URL + "processings/",
            primary_key="processing_id",
            module="stream.processing",
            description=_("创建|更新|删除"),
            default_return_value=self.test_stream.set_return_value("processings"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "change_component_type": DRFActionAPI(detail=False, method="post"),
                "check_stream_param": DRFActionAPI(detail=False, method="post"),
            },
        )

        # create|basic_info|metric_info|stop_debug
        self.debugs = DataDRFAPISet(
            url=BASE_STREAM_URL + "debugs/",
            primary_key="debug_id",
            module="stream.debug",
            description=_("实时调试相关操作集合"),
            default_return_value=self.test_stream.set_return_value("debugs"),
            before_request=add_app_info_before_request,
            custom_config={
                "basic_info": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_stream.set_return_value("get_debug_basic_info"),
                ),
                "node_info": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_stream.set_return_value("get_debug_node_info"),
                ),
                "stop": DRFActionAPI(method="post"),
            },
        )

        self.result_tables = DataDRFAPISet(
            url=BASE_STREAM_URL + "result_tables/",
            primary_key="result_table_id",
            module="stream.result_table",
            description="stream result table api",
            before_request=add_app_info_before_request,
            custom_config={"get_chain": DRFActionAPI(method="get", detail=False)},
        )


StreamApi = _StreamApi()
