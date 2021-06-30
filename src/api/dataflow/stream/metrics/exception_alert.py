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

from dataflow import pizza_settings
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.metrics.cast_exception_parser import TYPICAL_ERR, CastExceptionParser


class ExceptionAlert(object):
    @staticmethod
    def report_exception_metrics(stream_id, ex_timestamp, ex_stacktrace, geog_area_code):
        """
        上报flink异常信息到metrics，供告警监控发送给任务归属者
        @param stream_id: 实时任务ID
        @param ex_timestamp: 异常发生的时间戳
        @param ex_stacktrace: 异常信息堆栈
        @param geog_area_code: 地域
        """
        try:
            if TYPICAL_ERR in ex_stacktrace:
                exception_parser = CastExceptionParser(stream_id, ex_timestamp, ex_stacktrace)
                message = exception_parser.build_metric_info()
                kafka_topic = getattr(pizza_settings, "BK_MONITOR_RAW_ALERT_TOPIC")
                tags = [geog_area_code]
                DatamanageHelper.op_metric_report(message, kafka_topic, tags)
        except Exception as e:
            logger.exception(e)
