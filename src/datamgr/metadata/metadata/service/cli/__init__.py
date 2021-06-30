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

from metadata.service.cli.for_mysql_backend import (
    gen_common_rdfs,
    gen_custom_rdfs,
    gen_dgraph_schema,
    get_replica_models_to_sync_content,
    manual_meta_sync,
    migrate_to_dgraph,
)
from metadata.service.cli.state import (
    change_online_state,
    change_service_switch,
    check_backend_health,
    check_service_load,
    clear_online_state,
    recover_dgraph_cluster,
    set_dgraph_backend_version,
    show_online_state,
    switch_dgraph_be,
    switch_ha_backends,
)
from metadata.service.cli.sync import replay_db_operate_log
from metadata.service.cli.token import gen_query_token


@click.group()
def cli():
    pass


# 数据处理 相关命令
cli.add_command(get_replica_models_to_sync_content, 'convert-replica_db-to-sync_content')
cli.add_command(manual_meta_sync)
cli.add_command(gen_common_rdfs)
cli.add_command(gen_custom_rdfs)
cli.add_command(gen_dgraph_schema)
cli.add_command(replay_db_operate_log)
cli.add_command(migrate_to_dgraph)

# state 相关命令
cli.add_command(clear_online_state)
cli.add_command(show_online_state)
cli.add_command(change_online_state)
cli.add_command(switch_ha_backends)
cli.add_command(switch_dgraph_be)
cli.add_command(recover_dgraph_cluster)
cli.add_command(check_service_load)
cli.add_command(check_backend_health)
cli.add_command(change_service_switch)
cli.add_command(set_dgraph_backend_version)

# token 相关命令
cli.add_command(gen_query_token)
