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

import logging

import click
from tinyrpc import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

from metadata.runtime import rt_context
from metadata.util.i18n import lazy_selfish as _

module_logger = logging.getLogger(__name__)


@click.command()
@click.option('--min_n', type=int, help=_('Min db operate number.'))
@click.option('--max_n', type=int, help=_('Max db operate number.'))
def replay_db_operate_log(min_n, max_n):
    normal_conf = rt_context.config_collection.normal_config
    rpc_client = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport(
            'http://{}:{}/jsonrpc/2.0/'.format(normal_conf.ACCESS_RPC_SERVER_HOST, normal_conf.ACCESS_RPC_SERVER_PORT)
        ),
    )
    for i in range(min_n, max_n + 1):
        print(i)
        try:
            rpc_client.call(
                'bridge_sync',
                [],
                {"content_mode": "id", "db_operations_list": [i], "batch": False, "rpc_extra": {"language": "zh-hans"}},
            )
        except Exception:
            module_logger.exception('Failt to replay.')
