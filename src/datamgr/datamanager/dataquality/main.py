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

from dataquality.audit_tasks_beat import audit_rules_beat
from dataquality.data_correct_debug import start_data_correct_debug_session
from dataquality.data_profiling_adhoc import start_data_profiling_adhoc_session
from dataquality.data_quality_audit import audit_rules
from dataquality.data_quality_event_notify import notify_data_quality_event
from dataquality.sampling_consume import sampling_consume_datasets

click.disable_unicode_literals_warning = True


@click.group()
def entry():
    pass


entry.add_command(click.command("audit-rules")(dm_task(audit_rules)))
entry.add_command(click.command("audit-rules-beat")(dm_task(audit_rules_beat)))
entry.add_command(click.command("notify-event")(dm_task(notify_data_quality_event)))
entry.add_command(
    click.command("correct-debug-session")(dm_task(start_data_correct_debug_session))
)
entry.add_command(
    click.command("adhoc-profiling-session")(
        dm_task(start_data_profiling_adhoc_session)
    )
)
entry.add_command(
    click.command("sampling-consume-datasets")(dm_task(sampling_consume_datasets))
)


if __name__ == "__main__":
    entry()
