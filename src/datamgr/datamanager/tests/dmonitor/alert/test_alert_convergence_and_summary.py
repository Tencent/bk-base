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
from collections import Counter

from gevent import monkey
import pytest

from dmonitor.alert.alert_codes import AlertStatus
from dmonitor.alert.alert_convergence_and_summary import (
    AlertConvergenceAndSummaryTaskGreenlet,
    AlertStation,
)
from tests import BaseTestCase

monkey.patch_all()


@pytest.mark.usefixtures(
    "patch_flow_infos_from_redis",
    "patch_alert_configs_fetch",
    "patch_db_write",
    "patch_alert_shields_fetch",
    "patch_bizs_fetch",
    "patch_projects_fetch",
    "patch_alert_send",
)
class TestNoDataAlertTask(BaseTestCase):
    def setup(self):
        """ """
        self.task_config = {
            "consumer_configs": {
                "type": "kafka",
                "alias": "op",
                "topic": "dmonitor_alerts",
                "partition": False,
                "group_id": "dmonitor",
                "batch_message_max_count": 5000,
                "batch_message_timeout": 0.1,
            },
            "task_pool_size": 100,
        }

        self._status_counter = Counter()

        class TestAlertStation(AlertStation):
            def add_alert(inner_self, alert):
                alert_status, a, b, c = super(TestAlertStation, inner_self).add_alert(
                    alert
                )
                self._status_counter[alert_status] += 1
                return alert_status, a, b, c

        self.task = AlertConvergenceAndSummaryTaskGreenlet(configs=self.task_config)
        self.task.station = TestAlertStation(self.task)
        # 由于更换了测试的station，因此这里需要重新更新配置
        for alert_config in self.task._alert_configs:
            if alert_config.get("active", False):
                self.task.station.add_alert_config(alert_config)

    @pytest.mark.parametrize("topic", ["dmonitor_alerts"])
    def test_alert_convergence_and_summary(self, patch_collect_kafka_data):
        consumed_data = self.task.collect_kafka_data()

        self.task._last_tick_time = (
            consumed_data[0].timestamp()[1] / 1000 - self.task.TICK_INTERVAL
        )

        for message in consumed_data:
            now = message.timestamp()[1] / 1000
            self.task.handle_monitor_message(message, now)
            # 模拟触发检测逻辑
            last_tick_time = self.task._last_tick_time
            for time_offset in range(int(now - last_tick_time)):
                self.task.do_monitor(last_tick_time + time_offset, self.task._task_pool)

        assert self._status_counter[AlertStatus.ALERTING.value] == 50
        assert self._status_counter[AlertStatus.CONVERGED.value] == 803
