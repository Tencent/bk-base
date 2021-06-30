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
import json
import logging

from dataquality.exceptions import (
    EventNotExistError,
    MetricNotExistError,
    ParamTypeNotSupportedError,
)
from dataquality.metric.base import Constant
from dataquality.model.base import ParamType


class AuditTaskContext(object):
    def __init__(self, metrics=None, events=None, functions=None):
        self._metrics = metrics or {}
        self._events = events or {}
        self._functions = functions or {}

    @property
    def metrics(self):
        return self._metrics

    @property
    def events(self):
        return self._events

    def add_metric(self, metric_name, metric):
        self._metrics[metric_name] = metric

    def add_event(self, event_id, event):
        self._events[event_id] = event

    def update(self, other_context):
        self._metrics.update(other_context.metrics)
        self._events.update(other_context.events)

    def get_function_result(self, function_config):
        function_name = function_config.get("function_name")
        function_params = function_config.get("function_params", [])

        function = self._functions.get_function(function_name)
        params = []

        for function_param in function_params:
            param_type = function_param.get("param_type")
            if param_type == ParamType.METRIC.value:
                metric = self.get_metric_param(function_param.get("metric_name"))
                params.append(metric.value)
            elif param_type == ParamType.EVENT.value:
                event = self.get_event_param(function_param.get("event_id"))
                params.append(event)
            elif param_type == ParamType.CONSTANT.value:
                constant = Constant(
                    function_param.get("constant_type"),
                    function_param.get("constant_value"),
                )
                params.append(constant.value)
            elif param_type == ParamType.FUNCTION.value:
                function_result = self.get_function_result(
                    function_param.get("function", {})
                )
                params.append(function_result)
            elif param_type == ParamType.EXPRESSION.value:
                # TODO 解析表达式并计算结果
                pass
            else:
                raise ParamTypeNotSupportedError(message_kv={"param_type": param_type})

        try:
            result = function.call(*params)
        except Exception as e:
            logging.error(
                "Get function result error, function: {}, params: {}, reason: {}".format(
                    function_name, json.dumps(params), e
                ),
                exc_info=True,
            )
            raise e

        return result

    def get_metric_param(self, metric_name):
        if metric_name not in self._metrics:
            raise MetricNotExistError(message_kv={"metric_name": metric_name})
        return self._metrics.get(metric_name)

    def get_event_param(self, event_id):
        if event_id not in self._events:
            raise EventNotExistError(message_kv={"event_id": event_id})
        return self._events.get(event_id)
