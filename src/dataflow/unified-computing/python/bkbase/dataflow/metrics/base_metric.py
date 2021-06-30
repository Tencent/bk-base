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
import uuid

from bkbase.dataflow.metrics.registry.metrics_registry import MetricsRegistry
from bkbase.dataflow.metrics.reporter.console_reporter import console_reporter
from bkbase.dataflow.metrics.reporter.http_reporter import http_reporter
from bkbase.dataflow.metrics.reporter.kafka_reporter import kafka_reporter
from bkbase.dataflow.metrics.util.constants import *
from bkbase.dataflow.metrics.util.exceptions import MetricsVerifyException


class BaseMetric(object):
    """
    参数：
    上报频率（periodic_report_interval_sec）
    目标地址（target_url）
        上报类型为 kafka ，就是 kafka 服务地址
        上报类型为 http ，就是 http_url
        上报类型为 console ，测试使用，则不填
    主题（topic）
        上报类型为 kafka 时，填写。否则填空
    上报类型
        kafka
        http
        console
    """

    def __init__(self, periodic_report_interval_sec, target_url, topic, report_type):
        # 每个对象都有自己唯一的 id
        self._object_id = uuid.uuid4()
        self._registry = MetricsRegistry()
        # 检验类型
        if report_type not in REPORT_TYPE:
            raise MetricsVerifyException(u"参数错误!上报类型:%s" % ",".join(REPORT_TYPE))
        if report_type == KAFKA:
            if not target_url:
                raise MetricsVerifyException(u"参数错误!kafka地址不为空")
            if not topic:
                raise MetricsVerifyException(u"参数错误!topic不为空")
            kafka_reporter.set_producer(target_url, topic)
            self._reporter = kafka_reporter
        elif report_type == HTTP:
            if not target_url:
                raise MetricsVerifyException(u"参数错误!http地址不为空")
            http_reporter.set_http_url(target_url)
            self._reporter = http_reporter
        else:
            self._reporter = console_reporter
        # 检验 上报频率
        if periodic_report_interval_sec <= 0:
            raise MetricsVerifyException(u"参数错误!上报频率必须>=0")
        self._reporter.set_reporting_interval(periodic_report_interval_sec)
        # reporter 上报对象
        self._reporter.set_metric_registry(self._object_id, self._registry)
        self._reporter.start()

    def counter(self, key):
        return self._registry.get_or_add(key, "counter")

    def literal(self, key):
        return self._registry.get_or_add(key, "literal")

    # 停止上报数据,设置 is_end 为 true, 最后一个数据上报完后再移除 key
    def closed(self):
        self._registry.set_end()

    # 停止上报线程, 把所有的上报对象 close 掉, 上报最后一次后 stop 上报线程
    def destroy_reporter(self):
        self._reporter.set_all_registry_closed()
