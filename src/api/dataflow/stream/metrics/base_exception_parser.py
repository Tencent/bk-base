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

from dataflow.shared.handlers import dataflow_info, dataflow_job


class BaseExceptionParser(object):
    def __init__(self, stream_id, ex_timestamp, ex_stacktrace):
        self.stream_id = stream_id
        self.ex_timestamp = ex_timestamp
        self.ex_stacktrace = ex_stacktrace
        self.flow_id = None
        self.flow_name = None

    def message(self):
        raise NotImplementedError

    def message_en(self):
        raise NotImplementedError

    def full_message(self):
        raise NotImplementedError

    def full_message_en(self):
        raise NotImplementedError

    def parse(self):
        raise NotImplementedError

    def build_metric_info(self):
        self.flow_id = dataflow_job.find_flow_id_by_stream_id(self.stream_id)
        self.flow_name = dataflow_info.find_flow_name_by_id(self.flow_id)
        self.parse()
        metrics = {
            "time": self.ex_timestamp,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "recover_time": 0,
                "alert_status": "init",
                "message": self.message(),
                "message_en": self.message_en(),
                "full_message": self.full_message(),
                "full_message_en": self.full_message_en(),
                "tags": {
                    "alert_level": "danger",
                    "alert_type": "task_monitor",
                    "alert_code": "task",
                    "flow_id": self.flow_id,
                },
            },
        }
        return metrics
