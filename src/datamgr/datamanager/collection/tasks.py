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
"""
cmdb 采集任务
"""
from gevent import monkey

monkey.patch_all()  # noqa

import click

from collection.cmdb.biz_info import batch_collect_cmdb_biz_info
from collection.cmdb.datacheck.host_info_check import (
    check_cmdb_host_info,
    check_cmdb_host_info_by_one,
)
from collection.cmdb.datacheck.module_info_check import (
    check_cmdb_module_info,
    check_cmdb_module_info_by_one,
)
from collection.cmdb.datacheck.set_info_check import (
    check_cmdb_set_info,
    check_cmdb_set_info_by_one,
)
from collection.cmdb.host_info import (
    batch_collect_cmdb_host_info,
    collect_cmdb_host_info,
    collect_cmdb_host_info_by_one,
)
from collection.cmdb.module_info import (
    batch_collect_cmdb_module_info,
    collect_cmdb_module_info,
    collect_cmdb_module_info_by_one,
)
from collection.cmdb.processes.biz import process_cmdb_biz_info
from collection.cmdb.processes.host import process_cmdb_host_info
from collection.cmdb.processes.module import process_cmdb_module_info
from collection.cmdb.processes.set import process_cmdb_set_info
from collection.cmdb.set_info import (
    batch_collect_cmdb_set_info,
    collect_cmdb_set_info,
    collect_cmdb_set_info_by_one,
)
from collection.usermanage.processes.user_info import process_user_info
from collection.usermanage.user_info import batch_collect_user_info
from common.log import configure_logging

click.disable_unicode_literals_warning = True

configure_logging()


@click.command("collect-cmdb-host-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True, type=int)
def collect_cmdb_host_info_by_one_cmd(bk_biz_id):
    collect_cmdb_host_info_by_one(bk_biz_id)


@click.command("check-cmdb-host-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True, type=int)
def check_cmdb_host_info_by_one_cmd(bk_biz_id):
    check_cmdb_host_info_by_one(bk_biz_id)


@click.command("collect-cmdb-set-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True)
def collect_cmdb_set_info_by_one_cmd(bk_biz_id):
    collect_cmdb_set_info_by_one(bk_biz_id)


@click.command("check-cmdb-set-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True)
def check_cmdb_set_info_by_one_cmd(bk_biz_id):
    check_cmdb_set_info_by_one(bk_biz_id)


@click.command("collect-cmdb-module-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True)
def collect_cmdb_module_info_by_one_cmd(bk_biz_id):
    collect_cmdb_module_info_by_one(bk_biz_id)


@click.command("check-cmdb-module-info-by-one")
@click.option("-b", "--bid", "bk_biz_id", required=True)
def check_cmdb_module_info_by_one_cmd(bk_biz_id):
    check_cmdb_module_info_by_one(bk_biz_id)


@click.group()
def entry():
    pass


entry.add_command(click.command("collect-cmdb-host-info")(collect_cmdb_host_info))
entry.add_command(
    click.command("batch-collect-cmdb-host-info")(batch_collect_cmdb_host_info)
)
entry.add_command(click.command("process-cmdb-host-info")(process_cmdb_host_info))
entry.add_command(click.command("check-cmdb-host-info")(check_cmdb_host_info))

entry.add_command(click.command("collect-cmdb-set-info")(collect_cmdb_set_info))
entry.add_command(
    click.command("batch-collect-cmdb-set-info")(batch_collect_cmdb_set_info)
)
entry.add_command(click.command("process-cmdb-set-info")(process_cmdb_set_info))
entry.add_command(click.command("check-cmdb-set-info")(check_cmdb_set_info))

entry.add_command(click.command("collect-cmdb-module-info")(collect_cmdb_module_info))
entry.add_command(
    click.command("batch-collect-cmdb-module-info")(batch_collect_cmdb_module_info)
)
entry.add_command(click.command("process-cmdb-module-info")(process_cmdb_module_info))
entry.add_command(click.command("check-cmdb-module-info")(check_cmdb_module_info))

entry.add_command(click.command("process-cmdb-biz-info")(process_cmdb_biz_info))
entry.add_command(
    click.command("batch-collect-cmdb-biz-info")(batch_collect_cmdb_biz_info)
)

entry.add_command(click.command("collect_user_info")(batch_collect_user_info))
entry.add_command(click.command("process-user-info")(process_user_info))

entry.add_command(collect_cmdb_host_info_by_one_cmd)
entry.add_command(check_cmdb_host_info_by_one_cmd)

entry.add_command(collect_cmdb_set_info_by_one_cmd)
entry.add_command(check_cmdb_set_info_by_one_cmd)

entry.add_command(collect_cmdb_module_info_by_one_cmd)
entry.add_command(check_cmdb_module_info_by_one_cmd)


if __name__ == "__main__":
    entry()
