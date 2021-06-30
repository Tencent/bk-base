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
from os import path

import click

from metadata.runtime import rt_context
from metadata.state.manage import (
    BackendNodesHealthChecker,
    BackendSwitch,
    DgraphBESwitch,
    DgraphVersionManager,
    RecoverDgraphCluster,
    ServiceLoadChecker,
    ServiceSwitch,
)
from metadata.state.state import CommonState, State, StateMode
from metadata.util.i18n import lazy_selfish as _

logger = logging.getLogger(__name__)


@click.command()
def clear_online_state():
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    system_state.cleanup()


@click.command()
def show_online_state():
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    click.echo('\n===online state===\n')
    click.echo('[backends_ha_state]')
    click.echo(json.dumps(system_state['backends_ha_state']) if 'backends_ha_state' in system_state else None)
    click.echo('\n')
    click.echo('[backend_configs]')
    click.echo(json.dumps(system_state['backend_configs']) if 'backend_configs' in system_state else None)
    click.echo('\n')
    click.echo('[backend_warning]')
    click.echo(json.dumps(system_state['backend_warning']) if 'backend_warning' in system_state else None)
    click.echo('\n')
    click.echo('[service_load_info]')
    click.echo(json.dumps(system_state['service_load_info']) if 'service_load_info' in system_state else None)
    click.echo('\n')
    click.echo('[service_switches]')
    click.echo(json.dumps(system_state['service_switches']) if 'service_switches' in system_state else None)
    click.echo('\n')
    click.echo('[transaction_counter]')
    click.echo(
        json.dumps(system_state["counter/transaction_counter"])
        if 'counter/transaction_counter' in system_state
        else None
    )


@click.command()
def change_online_state():
    i_c = rt_context.config_collection.intro_config
    with open(path.join(i_c.DATA_PATH, 'online_state.json')) as fr:
        info = json.load(fr)
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    if info.get('backends_ha_state'):
        system_state['backends_ha_state'] = info['backends_ha_state']
    if info.get('backend_configs'):
        system_state['backend_configs'] = info['backend_configs']
    logger.info('Set online config {}'.format(info))


@click.command()
@click.option('-n', '--name', help=_('cluster name.(master/slave[default])'), default='slave')
def recover_dgraph_cluster(name):
    """
    恢复dgraph集群状态

    :return:  None
    """
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    with RecoverDgraphCluster(system_state) as rs:
        rs.recover(name)


@click.command()
def switch_ha_backends():
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    with BackendSwitch(system_state) as bs:
        bs.switch()


@click.command()
@click.option('-s', '--status', help=_('BE status.'), default=CommonState.ON.value)
def switch_dgraph_be(status):
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    with DgraphBESwitch(system_state) as bs:
        bs.turn(status)


@click.command()
@click.option('-s', '--stat_win', help=_('statistic window (s)'), default=5)
@click.option('-d', '--delay', help=_('window delay (s)'), default=2)
def check_service_load(stat_win, delay):
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    service_checker = ServiceLoadChecker(system_state)
    is_heavy, load_info = service_checker.statistic(stat_win, delay)
    click.echo('is_heavy: {} load_info: {}'.format(is_heavy, load_info))


@click.command()
def check_backend_health():
    system_state = State('/metadata', StateMode.ONLINE)
    system_state.syncing()
    backend_checker = BackendNodesHealthChecker(system_state)
    backend_checker.get_status()
    is_change, backend_config, online_warning_config = backend_checker.check()
    click.echo('is_change: {}'.format(is_change))
    click.echo('backend_config: {}'.format(backend_config))
    click.echo('online_warning_config: {}'.format(online_warning_config))


@click.command()
@click.option('-sn', '--switch_name', help=_('switch name.[service.sub_switch]'), default=None)
@click.option('-sv', '--switch_value', help=_('switch value.[on / off]'), default='off')
def change_service_switch(switch_name, switch_value):
    if switch_name:
        switch_name_parts = '{}.'.format(switch_name).split('.')
        service_name = switch_name_parts[0]
        sub_switch_name = switch_name_parts[1]
        sub_switch_name = sub_switch_name if sub_switch_name else 'master'
        switch_value = True if switch_value.lower() == 'on' else False
        system_state = State('/metadata', StateMode.ONLINE)
        system_state.syncing()
        with ServiceSwitch(system_state) as ss:
            if ss.switch(service_name, sub_switch_name, switch_value):
                click.echo('change {} = {} succeed'.format(switch_name, switch_value))
            else:
                click.echo('change {} = {} failed'.format(switch_name, switch_value))
    else:
        click.echo('switch command invalid')


@click.command()
@click.option('-v', '--version', help=_('dgraph version'), default=None)
@click.option('-b', '--backend_id', help=_('id of dgraph backend'), default=None)
def set_dgraph_backend_version(version, backend_id):
    if version and backend_id:
        system_state = State('/metadata', StateMode.ONLINE)
        system_state.syncing()
        with DgraphVersionManager(system_state) as ds:
            if ds.set_backend_version(backend_id, version):
                click.echo('set backend({}) version = {} succeed'.format(backend_id, version))
            else:
                click.echo('set backend({}) version failed'.format(backend_id))
