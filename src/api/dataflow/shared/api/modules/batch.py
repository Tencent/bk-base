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

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_BATCH_URL

from .test.test_call_batch import TestBatch


class _BatchApi(object):

    test_batch = TestBatch()

    def __init__(self):

        # create|start|stop
        self.jobs = DataDRFAPISet(
            url=BASE_BATCH_URL + "jobs/",
            primary_key="job_id",
            module="batch",
            description=_("离线作业接口"),
            default_return_value=self.test_batch.set_return_value("jobs"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "start": DRFActionAPI(method="post"),
                "stop": DRFActionAPI(method="post"),
                "get_param": DRFActionAPI(method="get"),
                "stop_source": DRFActionAPI(detail=True, method="post"),
            },
        )

        self.processings = DataDRFAPISet(
            url=BASE_BATCH_URL + "processings/",
            primary_key="processing_id",
            module="batch",
            description=_("创建|更新|删除"),
            default_return_value=self.test_batch.set_return_value("processings"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"check_batch_param": DRFActionAPI(method="post", detail=False)},
        )

        self.healthz = DataAPI(
            url=BASE_BATCH_URL + "healthz/",
            method="GET",
            module="batch",
            description=_("健康检查"),
        )

        self.hdfs_result_tables = DataDRFAPISet(
            url=BASE_BATCH_URL + "hdfs/result_tables/",
            primary_key="result_table_id",
            module="batch",
            description=_("hdfs rt表信息"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "new_line": DRFActionAPI(
                    detail=True,
                    method="get",
                    default_return_value=self.test_batch.set_return_value("get_result_table_data_tail"),
                )
            },
        )

        # create|basic_info|metric_info|stop_debug
        self.debugs = DataDRFAPISet(
            url=BASE_BATCH_URL + "debugs/",
            primary_key="debug_id",
            module="batch",
            description=_("离线调试相关操作集合"),
            default_return_value=self.test_batch.set_return_value("debugs"),
            before_request=add_app_info_before_request,
            custom_config={
                "basic_info": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_batch.set_return_value("get_debug_basic_info"),
                ),
                "node_info": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_batch.set_return_value("get_debug_node_info"),
                ),
                "stop": DRFActionAPI(method="post"),
            },
        )

        self.custom_calculates = DataDRFAPISet(
            url=BASE_BATCH_URL + "custom_calculates/",
            primary_key="custom_calculate_id",
            module="batch",
            description=_("重算相关操作集合"),
            default_return_value=self.test_batch.set_return_value("recalculates"),
            before_request=add_app_info_before_request,
            custom_config={
                "basic_info": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_batch.set_return_value("get_recalculate_basic_info"),
                ),
                "stop": DRFActionAPI(
                    method="post",
                    detail=True,
                    default_return_value=self.test_batch.set_return_value("stop_recalculate"),
                ),
                "analyze": DRFActionAPI(method="post", detail=False),
            },
        )

        self.hdfs = DataDRFAPISet(
            url=BASE_BATCH_URL + "hdfs/",
            primary_key="hdfs_ns_id",
            module="batch",
            description=_("hdfs操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "upload": DRFActionAPI(detail=True, method="post"),
                "move": DRFActionAPI(detail=True, method="post"),
                "clean": DRFActionAPI(detail=True, method="post"),
                "list_status": DRFActionAPI(detail=True, method="get"),
            },
        )

        self.data_makeup = DataDRFAPISet(
            url=BASE_BATCH_URL + "data_makeup/",
            primary_key=None,
            module="batch",
            description="离线计算数据补齐",
            default_return_value=self.test_batch.set_return_value("common_success"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "status_list": DRFActionAPI(
                    detail=False,
                    method="get",
                    default_return_value=self.test_batch.set_return_value("data_makeup_status_list"),
                ),
                "check_execution": DRFActionAPI(
                    detail=False,
                    method="get",
                    default_return_value=self.test_batch.set_return_value("data_makeup_check_execution"),
                ),
            },
        )


BatchApi = _BatchApi()
