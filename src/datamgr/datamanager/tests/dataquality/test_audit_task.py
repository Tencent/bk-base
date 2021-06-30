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

import mock
from gevent import monkey, pool
from influxdb.resultset import ResultSet

from dataquality import session
from dataquality.model.core import AuditRule
from tests import BaseTestCase

from dataquality.audit.task import ResultTableAuditTaskGreenlet

monkey.patch_all()
logger = logging.getLogger(__name__)


class TestDataqualityAuditTask(BaseTestCase):
    def setup(self):
        """
        数据质量监控配置
        """
        self.task_pool = pool.Pool(10)
        session.begin_nested()

        self.audit_rule = self.init_unittest_audit_rule()

        self.audit_task_config = {
            "data_set_id": self.audit_rule.data_set_id,
            "rule_id": self.audit_rule.id,
            "rule_config": json.dumps(
                {
                    "version": "1.0",
                    "timer": {
                        "timer_type": "once",
                        "timer_config": "",
                    },
                    "rules": [
                        {
                            "input": [
                                {
                                    "input_type": "metric",
                                    "metric_name": "last10output",
                                },
                                {
                                    "input_type": "metric",
                                    "metric_name": "last10outputbefore7daysago",
                                },
                            ],
                            "rule": {
                                "function": {
                                    "function_type": "builtin",
                                    "function_name": "greater",
                                    "function_params": [
                                        {
                                            "param_type": "function",
                                            "function": {
                                                "function_type": "builtin",
                                                "function_name": "subtraction",
                                                "function_params": [
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10output",
                                                    },
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10outputbefore7daysago",
                                                    },
                                                ],
                                            },
                                        },
                                        {
                                            "param_type": "function",
                                            "function": {
                                                "function_type": "builtin",
                                                "function_name": "multiplication",
                                                "function_params": [
                                                    {
                                                        "param_type": "constant",
                                                        "constant_type": "float",
                                                        "constant_value": 0.3,
                                                    },
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10outputbefore7daysago",
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                }
                            },
                            "output": {
                                "event_id": "output_data_fluctuation",
                                "event_alias": "输出数据量波动",
                                "event_type": "data_quality",
                                "event_sub_type": "data_fluctuation",
                                "event_currency": 600,
                                "tags": [],
                            },
                        }
                    ],
                }
            ),
        }

        self.influx_result1 = ResultSet(
            {
                "series": [
                    {
                        "values": [[1588837954, 693]],
                        "name": "data_loss_output_total",
                        "columns": ["time", "last10output"],
                        "tags": {"data_set_id": "591_datamonitor_clean_test_data"},
                    }
                ]
            }
        )

        self.influx_result2 = ResultSet(
            {
                "series": [
                    {
                        "values": [[1588837954, 300]],
                        "name": "data_loss_output_total",
                        "columns": ["time", "last10outputbefore7daysago"],
                        "tags": {"data_set_id": "591_datamonitor_clean_test_data"},
                    }
                ]
            }
        )

    def init_unittest_audit_rule(self):
        audit_rule = AuditRule(
            id=1,
            data_set_id="591_datamonitor_clean_test_data",
            rule_name="test_rule",
            rule_config=json.dumps(
                {
                    "version": "1.0",
                    "timer": {
                        "timer_type": "once",
                        "timer_config": "",
                    },
                    "rules": [
                        {
                            "input": [
                                {
                                    "input_type": "metric",
                                    "metric_name": "last10output",
                                },
                                {
                                    "input_type": "metric",
                                    "metric_name": "last10outputbefore7daysago",
                                },
                            ],
                            "rule": {
                                "function": {
                                    "function_type": "builtin",
                                    "function_name": "greater",
                                    "function_params": [
                                        {
                                            "param_type": "function",
                                            "function": {
                                                "function_type": "builtin",
                                                "function_name": "subtraction",
                                                "function_params": [
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10output",
                                                    },
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10outputbefore7daysago",
                                                    },
                                                ],
                                            },
                                        },
                                        {
                                            "param_type": "function",
                                            "function": {
                                                "function_type": "builtin",
                                                "function_name": "multiplication",
                                                "function_params": [
                                                    {
                                                        "param_type": "constant",
                                                        "constant_type": "float",
                                                        "constant_value": 0.3,
                                                    },
                                                    {
                                                        "param_type": "metric",
                                                        "metric_name": "last10outputbefore7daysago",
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                }
                            },
                            "output": {
                                "event_id": "output_data_fluctuation",
                                "event_alias": "输出数据量波动",
                                "event_type": "data_quality",
                                "event_sub_type": "data_fluctuation",
                                "event_currency": 600,
                                "tags": [],
                            },
                        }
                    ],
                }
            ),
            created_by="unittest",
            description="unittest",
        )
        self.audit_task_config = audit_rule
        session.add(audit_rule)
        return audit_rule

    def teardown(self):
        session.delete(self.audit_rule)
        session.commit()

    # @mock.patch("utils.rawdata_producer.RawDataProducer.produce_message")
    # @mock.patch("dataquality.metric.factory.influx_query")
    # def test_greenlet_task(self, influx_query_mock, producer_mock):
    #     def print_produce_message(value, *args, **kwargs):
    #         print(value)
    #
    #     influx_query_mock.side_effect = [self.influx_result1, self.influx_result2]
    #     producer_mock.side_effect = print_produce_message
    #
    #     task = ResultTableAuditTaskGreenlet(self.audit_task_config)
    #     task.start()
    #     task.join(timeout=10)

    # # @mock.patch('utils.rawdata_producer.RawDataProducer')
    # @mock.patch('dataquality.audit.task.ResultTableAuditTaskGreenlet.init_producer')
    # @mock.patch('dataquality.metric.factory.influx_query')
    # def test_greenlet_task(self, influx_query_mock, patch_producer_mock):
    #     def print_produce_message(value, *args, **kwargs):
    #         print(value)
    #
    #     influx_query_mock.side_effect = [self.influx_result1, self.influx_result2]
    #     # patch_producer_mock = mock.MagicMock()
    #     # # patch_producer_mock.return_value = producer_mock
    #     # patch_producer_mock = mock.Mock()
    #     patch_producer_mock = mock.MagicMock()
    #
    #     task = ResultTableAuditTaskGreenlet(self.audit_task_config)
    #     task.start()
    #     task.join(timeout=10)
