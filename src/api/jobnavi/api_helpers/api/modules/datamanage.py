# coding=utf-8
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
from django.utils.translation import ugettext_lazy as _
from common.api.base import DataDRFAPISet, DRFActionAPI
from jobnavi.pizza_settings import BASE_DATAMANAGE_URL
from .test.test_call_datamanage import TestDatamanage


class _DatamanageApi(object):

    test_datamanage = TestDatamanage()

    def __init__(self):

        self.metrics = DataDRFAPISet(
            url=BASE_DATAMANAGE_URL + "dmonitor/metrics/",
            primary_key=None,
            module="datamanage",
            description=_("查询数据质量TSDB中的监控指标"),
            default_return_value=None,
            before_request=None,
            after_request=None,
            custom_config={
                "query": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_datamanage.set_return_value("metrics_query")),
                "report": DRFActionAPI(
                    method="post",
                    detail=False)
            }
        )

        self.result_tables = DataDRFAPISet(
            url=BASE_DATAMANAGE_URL + "dmonitor/result_tables/",
            primary_key=None,
            module="datamanage",
            description=_("查询结果表监控状态"),
            default_return_value=None,
            before_request=None,
            after_request=None,
            custom_config={
                "status": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_datamanage.set_return_value("result_tables_status"))
            }
        )

        self.dmonitor = DataDRFAPISet(
            url=BASE_DATAMANAGE_URL + "dmonitor/",
            primary_key=None,
            module="datamanage",
            description=_("相关监控信息查询"),
            default_return_value=None,
            before_request=None,
            after_request=None,
            custom_config={
                "batch_executions": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_datamanage.set_return_value("dmonitor_batch_executions")),
                "alert_details": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_datamanage.set_return_value("dmonitor_alert_details")
                )
            }
        )

        self.dmonitor_dataflow = DataDRFAPISet(
            url=BASE_DATAMANAGE_URL + "dmonitor/{dmonitor_type}/dataflow/",
            primary_key="flow_id",
            url_keys=["dmonitor_type"],
            module="datamanage",
            description="告警策略配置",
            default_return_value=self.test_datamanage.set_return_value("dmonitor_dataflow"),
            before_request=None,
            after_request=None
        )


DatamanageApi = _DatamanageApi()
