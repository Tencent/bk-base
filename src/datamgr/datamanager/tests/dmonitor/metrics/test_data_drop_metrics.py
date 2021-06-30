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
import time

from gevent import monkey
import pytest

from dmonitor.metrics.metrics_perceive_and_process import (
    MetricsPerceiveAndProcessTaskGreenlet,
)
from tests import BaseTestCase


monkey.patch_all()


@pytest.mark.usefixtures("patch_data_sets_from_redis", "patch_produce_metric")
class TestMetricsPerceiveAndProcessTask(BaseTestCase):
    def setup(self):
        """ """
        self.task_config = {
            "consumer_configs": {
                "type": "kafka",
                "alias": "op",
                "topic": "bkdata_data_monitor_metrics591",
                "partition": False,
                "group_id": "dmonitor",
                "batch_message_max_count": 5000,
                "batch_message_timeout": 0.1,
            },
            "task_pool_size": 100,
        }
        self.now = int(time.time())
        self.task = MetricsPerceiveAndProcessTaskGreenlet(configs=self.task_config)
        self.task.log_opdata(self.now)

    @pytest.mark.parametrize("topic", ["bkdata_data_monitor_metrics591"])
    def test_common_metric_perceive(self, patch_collect_kafka_data):
        consumed_data = self.task.collect_kafka_data()

        for message in consumed_data:
            self.task.handle_monitor_message(message)

        assert self.task._consume_count == len(consumed_data)
        assert self.task._produce_count == 43677
        assert self.task._handle_error_count == 1

    @pytest.mark.parametrize("topic", ["bkdata_data_monitor_databus_metrics591"])
    def test_databus_metric_perceive(self, patch_collect_kafka_data):
        consumed_data = self.task.collect_kafka_data()

        for message in consumed_data:
            self.task.handle_monitor_message(message)

        assert self.task._consume_count == len(consumed_data)
        assert self.task._produce_count == 100596
