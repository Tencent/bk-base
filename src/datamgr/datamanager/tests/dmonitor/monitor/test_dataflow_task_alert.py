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
from gevent import monkey
import pytest

from dmonitor.monitor.dataflow_task_alert import DataflowTaskAlertTaskGreenlet
from tests import BaseTestCase

monkey.patch_all()


@pytest.mark.usefixtures(
    "patch_dataflow_infos_from_redis",
    "patch_batch_executions_fetch",
    "patch_alert_configs_fetch",
)
class TestDataflowTaskAlertTask(BaseTestCase):
    def setup(self):
        """ """
        self.task_config = {
            "consumer_configs": {
                "type": "kafka",
                "alias": "op",
                "topic": "dmonitor_output_total",
                "partition": False,
                "group_id": "dmonitor",
                "batch_message_max_count": 5000,
                "batch_message_timeout": 0.1,
            },
            "task_pool_size": 100,
        }
        self.task = DataflowTaskAlertTaskGreenlet(configs=self.task_config)

    @pytest.mark.parametrize("topic", ["dmonitor_output_total"])
    def test_dataflow_task_alert(self, patch_collect_kafka_data):
        consumed_data = self.task.collect_kafka_data()

        self.task._last_detect_time = (
            consumed_data[0].timestamp()[1] / 1000 - self.task.PENDING_TIME
        )

        for message in consumed_data:
            now = message.timestamp()[1] / 1000

            # 模拟触发检测逻辑
            last_detect_time = self.task._last_detect_time
            for time_offset in range(int(now - last_detect_time)):
                self.task.do_monitor(
                    last_detect_time + time_offset, self.task._task_pool
                )

            self.task.handle_monitor_message(message, now)

        assert self.task._produce_count == 192
