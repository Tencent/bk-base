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
import click

from dm_engine import dm_task
from dmonitor.alert import alert_match, alert_convergence_and_summary
from dmonitor.meta.sync_meta_to_redis import sync_meta_to_redis
from dmonitor.metrics import (
    data_delay_metrics,
    data_drop_metrics,
    metrics_perceive_and_process,
    metrics_storage,
    rawdata_msg_cnt_metrics,
)
from dmonitor.monitor import (
    batch_data_trend_alert,
    batch_delay_alert,
    data_delay_alert,
    data_drop_alert,
    data_interrupt_alert,
    data_loss_alert,
    data_trend_alert,
    dataflow_task_alert,
    delay_trend_alert,
    no_data_alert,
    process_delay_alert,
)

click.disable_unicode_literals_warnig = True


@click.group()
def entry():
    pass


entry.add_command(click.command("sync-meta-cache")(dm_task(sync_meta_to_redis)))

entry.add_command(click.command("dmonitor-alert-match")(dm_task(alert_match)))
entry.add_command(
    click.command("dmonitor-alert-summary")(dm_task(alert_convergence_and_summary))
)

entry.add_command(
    click.command("dmonitor-metrics-clean")(dm_task(metrics_perceive_and_process))
)
entry.add_command(click.command("dmonitor-delay-metrics")(dm_task(data_delay_metrics)))
entry.add_command(click.command("dmonitor-drop-metrics")(dm_task(data_drop_metrics)))
entry.add_command(click.command("dmonitor-metrics-storage")(dm_task(metrics_storage)))
entry.add_command(
    click.command("dmonitor-rawdata-metrics")(dm_task(rawdata_msg_cnt_metrics))
)

entry.add_command(
    click.command("dmonitor-batch-data-trend-alert")(dm_task(batch_data_trend_alert))
)
entry.add_command(
    click.command("dmonitor-batch-delay-alert")(dm_task(batch_delay_alert))
)
entry.add_command(click.command("dmonitor-data-delay-alert")(dm_task(data_delay_alert)))
entry.add_command(click.command("dmonitor-data-drop-alert")(dm_task(data_drop_alert)))
entry.add_command(
    click.command("dmonitor-data-interrupt-alert")(dm_task(data_interrupt_alert))
)
entry.add_command(click.command("dmonitor-data-loss-alert")(dm_task(data_loss_alert)))
entry.add_command(click.command("dmonitor-data-trend-alert")(dm_task(data_trend_alert)))
entry.add_command(
    click.command("dmonitor-dataflow-task-alert")(dm_task(dataflow_task_alert))
)
entry.add_command(
    click.command("dmonitor-delay-trend-alert")(dm_task(delay_trend_alert))
)
entry.add_command(click.command("dmonitor-no-data-alert")(dm_task(no_data_alert)))
entry.add_command(
    click.command("dmonitor-process-delay-alert")(dm_task(process_delay_alert))
)


if __name__ == "__main__":
    entry()
