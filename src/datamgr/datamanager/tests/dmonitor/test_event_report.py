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
import mock

from api.base import DataResponse

from api import meta_api
from tests import BaseTestCase
from dmonitor.event.event_report import report_dmonitor_event

meta_api.report_events = mock.Mock(
    return_value=DataResponse(
        {"data": "xxxxxx", "result": True, "message": None, "code": "00"}
    )
)


class TestEventReport(BaseTestCase):
    def setUp(self):
        pass

    def test_report_dmonitor_event(self):
        event_message = {
            "message": "xx",
            "message_en": "xx",
            "full_message_en": "xx",
            "full_message": "xx",
            "alert_config_id": 72161,
            "alert_code": "process_time_delay",
            "alert_type": "data_monitor",
            "flow_id": "18403",
            "node_id": "129225",
            "bk_biz_id": 591,
            "project_id": 4716,
            "generate_type": "user",
            "dimensions": {
                "bk_biz_id": 591,
                "version": "1.0",
                "alert_type": "data_monitor",
                "component": "storm",
                "cluster": "default",
                "node_id": 129225,
                "storage_cluster_type": None,
                "data_set_id": "591_dataquery_minstat_StormSqlProject_1",
                "project_id": 4716,
                "alert_config_id": 72161,
                "generate_type": "user",
                "bk_app_code": "dataweb",
                "flow_id": 18403,
                "alert_level": "warning",
                "storage": "None_None",
                "logical_tag": "591_dataquery_minstat_StormSqlProject_1",
                "alert_code": "data_time_delay",
                "module": "realtime",
            },
            "alert_id": "7be1f4fb743849b090771f9b71b5ec8e",
            "alert_status": "alerting",
            "alert_time": "2021-04-20 10:44:11",
            "alert_level": "warning",
        }
        ret = report_dmonitor_event(event_message)
        assert ret is not None
