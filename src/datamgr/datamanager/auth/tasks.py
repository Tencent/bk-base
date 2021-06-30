# -*- coding:utf-8 -*-
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
from __future__ import absolute_import, print_function, unicode_literals

import click
from dm_engine import dm_task

from auth.bkiam.sync_users import migrate_iam_autho
from auth.sync_business import sync_leader, sync_members
from auth.sync_data_managers import init, sync, transmit_meta_operation_record

click.disable_unicode_literals_warning = True


@click.group()
def entry():
    pass


entry.add_command(click.command("init-data-managers")(dm_task(init)))
entry.add_command(click.command("sync-data-managers")(dm_task(sync)))
entry.add_command(
    click.command("transmit-meta-operation-record")(
        dm_task(transmit_meta_operation_record)
    )
)

entry.add_command(click.command("sync-business-members")(dm_task(sync_members)))
entry.add_command(click.command("sync-business-leader")(dm_task(sync_leader)))
entry.add_command(click.command("migrate-iam-auth")(dm_task(migrate_iam_autho)))

if __name__ == "__main__":
    entry()
