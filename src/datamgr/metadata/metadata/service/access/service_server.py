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
import os
import time
from collections import defaultdict
from multiprocessing import current_process
from weakref import WeakValueDictionary

import click
import gevent
from _weakrefset import WeakSet
from gevent.pool import Pool
from gevent.queue import Queue
from gipc import start_process
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from werkzeug.serving import run_with_reloader

from metadata.backend.dgraph.backend import DgraphBackend
from metadata.backend.interface import BackendType
from metadata.backend.mysql.backend import MySQLBackend
from metadata.event_system.register import get_registered_target, registered_event_list
from metadata.runtime import rt_context, rt_g, rt_local_manager
from metadata.service.access.layers import (
    AccessLayersGroup,
    AssetAccessLayer,
    BridgeAccessLayer,
    EntityAccessLayer,
    ImportAccessLayer,
    TagAccessLayer,
)
from metadata.service.access.layers.analyse import AnalyseAccessLayer
from metadata.service.access.rpc import (
    EnhancedRPCDispatcher,
    EnhancedRPCServer,
    EnhancedWsgiServerTransport,
    WSGIServerForRPC,
    tcp_listener,
)
from metadata.service.access.service_functions import (
    persist_record,
    replay_record,
    service_load_monitor,
)
from metadata.service.access.service_functions import (
    watch_backend_nodes_health as watch_backend_nodes_health_function,
)
from metadata.support import join_service_party
from metadata.util.i18n import selfish as _
from metadata.util.log import init_logging, update_record_handler, update_report_logging
from metadata.util.os import get_host_ip
from metadata_biz.interactor.mappings import (
    md_db_model_mappings,
    original_replica_db_model_mappings,
)
from metadata_biz.resource import biz_mysql_session_hubs
from metadata_biz.types import default_registry
from metadata_contents.config.conf import default_configs_collection

module_logger = logging.getLogger(__name__)


class Service(object):
    def __init__(self, host, port, access_worker_num, solo, ha, config_collection=None):
        self.config_collection = config_collection if config_collection else rt_g.config_collection
        self.normal_conf = self.config_collection.normal_config
        self.db_conf = self.config_collection.db_config
        self.logging_conf = self.config_collection.logging_config
        self.available_backends = self.normal_conf.AVAILABLE_BACKENDS
        self.functions = [
            persist_record,
            replay_record,
            service_load_monitor,
            watch_backend_nodes_health_function,
        ]

        self.rpc_host = host if host else self.normal_conf.ACCESS_RPC_SERVER_HOST
        self.rpc_port = port if port else self.normal_conf.ACCESS_RPC_SERVER_PORT
        self.solo = solo
        self.access_worker_num = (
            access_worker_num if access_worker_num else self.normal_conf.ACCESS_RPC_SERVER_PROCESS_NUM
        )
        self.service_ha = True if ha else self.normal_conf.SERVICE_HA
        if self.rpc_host == '0.0.0.0':
            self.rpc_host = get_host_ip()
        rt_g.worker_tag_prefix = self.worker_tag_prefix = self.node_tag = '_'.join([self.rpc_host, str(self.rpc_port)])

        self.leader_status = defaultdict(lambda: None)
        self.party = None
        self.all_processes = WeakSet()
        self.function_processes = WeakValueDictionary()
        self.access_processes = WeakValueDictionary()
        self.rdb = None

    # 打点和保活

    def controller_alive_metric(self):
        while True:
            info = self.controller_metric()
            info['metric_type'] = 'controller_alive'
            module_logger.info(info, extra={'metric': True})
            time.sleep(1)

    def controller_metric(self):
        info = {
            'host': self.rpc_host,
            'port': self.rpc_port,
            'node_tag': self.worker_tag_prefix,
            'controller_pid': os.getpid(),
            'active_workers': [[p.name, p.pid] for p in self.all_processes if p.is_alive()],
            'inactive_workers': [[p.name, p.pid] for p in self.all_processes if not p.is_alive()],
        }
        try:
            if rt_context.system_state.wait_to_valid(raise_err=False):
                for key in ['backends_ha_state', 'backend_configs', "counter/transaction_counter"]:
                    info[key] = rt_context.system_state[key] if key in rt_context.system_state else None
        except Exception:
            pass
        return info

    def worker_alive_metric(self, transport, rpc_greenlets_pool):
        while True:
            info = {
                'host': self.rpc_host,
                'port': self.rpc_port,
                'worker_tag': rt_g.worker_tag,
                'pid': os.getpid(),
                'metric_type': 'worker_alive',
                'queue_length': transport.messages.qsize(),
                'backends_ha_state': rt_context.system_state.cache['backends_ha_state']
                if rt_context.system_state.wait_to_valid(raise_err=False)
                else {},
                'system_state_status': rt_context.system_state.status,
                'worker_pool_free': rpc_greenlets_pool.free_count(),
            }

            time.sleep(1)
            module_logger.info(info, extra={'metric': True})

    def workers_alive_keep_up(self):
        while True:
            try:
                self.run_access_workers()
                self.run_functional_workers()
                time.sleep(10)
            except KeyboardInterrupt:
                break
            except Exception:
                module_logger.info('Error in keep workers alive.')

    def health(self):
        info = self.controller_metric()
        info['healthy'] = True
        return info

    # worker设置
    @classmethod
    def setup_access_layers(cls):
        backends_group_storage = {}
        available_backends = rt_context.config_collection.normal_config.AVAILABLE_BACKENDS
        if available_backends.get(BackendType.DGRAPH.value, False):
            db = DgraphBackend(rt_context.m_resource.dgraph_session)
            db.gen_types(list(rt_context.md_types_registry.values()))
            rt_g.dgraph_backend = db
            backends_group_storage[BackendType('dgraph')] = db

        if available_backends.get(BackendType.MYSQL.value, False):
            mb = cls.gen_mysql_backend(session_hub=rt_context.m_resource.mysql_session)
            rt_g.mysql_backend = mb
            backends_group_storage[BackendType('mysql')] = mb

        rt_g.biz_mysql_backends = {k: cls.gen_mysql_backend(v) for k, v in biz_mysql_session_hubs.items()}

        layers_group_storage = {
            'entity': EntityAccessLayer,
            'bridge': BridgeAccessLayer,
            'tag': TagAccessLayer,
            'import_': ImportAccessLayer,
            'asset': AssetAccessLayer,
            'analyse': AnalyseAccessLayer,
        }

        rt_g.layers = layers_obj = AccessLayersGroup(layers_group_storage, backends_group_storage)
        return layers_obj

    @staticmethod
    def gen_mysql_backend(session_hub):
        mb = MySQLBackend(
            session_hub,
        )
        mb.gen_types(
            rt_context.md_types_registry,
            replica_mappings=original_replica_db_model_mappings,
            mappings={k: v for k, v in md_db_model_mappings},
        )
        return mb

    def setup_access_worker_dispatcher(self, dispatcher):
        layers_obj = self.setup_access_layers()
        dispatcher.add_method(self.health)
        dispatcher.register_instance(layers_obj, 'root_')
        dispatcher.register_instance(layers_obj.entity, 'entity_')
        dispatcher.register_instance(layers_obj.bridge, 'bridge_')
        dispatcher.register_instance(layers_obj.tag, 'tag_')
        dispatcher.register_instance(layers_obj.import_, 'import_')
        dispatcher.register_instance(layers_obj.asset, 'asset_')
        dispatcher.register_instance(layers_obj.analyse, 'analyse_')

        join_service_party(self.service_ha)
        rt_context.system_state.wait_to_valid('backends_is_sat')

    def run_access_worker(self, n):
        """
        TODO:关注RPCServerLoop性能。
        每个进程开一个RPC服务，共同监听同一个Socket:listener。
        """
        listener = None
        try:
            rt_g.worker_tag = '-'.join([self.worker_tag_prefix, 'access', str(n), str(os.getpid())])
            update_report_logging(self.config_collection.logging_config)

            # 构造dispatcher
            dispatcher = EnhancedRPCDispatcher()
            self.setup_access_worker_dispatcher(dispatcher)

            # 构造设置RPC服务组件
            transport = EnhancedWsgiServerTransport(
                queue_class=Queue, auth_info=self.normal_conf.ACCESS_RPC_SERVER_AUTH
            )
            listener = tcp_listener((self.rpc_host, self.rpc_port), reuse_addr=True, reuse_port=True)
            wsgi_server = WSGIServerForRPC(
                listener, transport.handle, spawn=self.normal_conf.ACCESS_RPC_WSGI_CONCURRENT
            )
            rpc_greenlets_pool = Pool(self.normal_conf.ACCESS_RPC_WORKER_CONCURRENT)
            rpc_server = EnhancedRPCServer(transport, JSONRPCProtocol(), dispatcher, rpc_greenlets_pool.spawn)
            rt_local_manager.cleanup()

            # 开启服务
            gevent.spawn(self.worker_alive_metric, transport, rpc_greenlets_pool)
            gevent.spawn(wsgi_server.serve_forever)
            rpc_server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if listener:
                listener.close()

    def run_functional_workers(self):
        for func in self.functions:
            process_name = self.worker_tag_prefix + '-{}'.format(func.__name__)
            process = self.function_processes.get(process_name, None)
            if process:
                if process.is_alive():
                    continue
                else:
                    process.terminate()
            p = start_process(target=func, name=process_name, daemon=True)
            module_logger.info('Started a new functional worker process {}'.format(process_name))
            self.function_processes[process_name] = p
            self.all_processes.add(p)

    def run_access_workers(self):
        for i in range(self.access_worker_num):
            process_name = self.worker_tag_prefix + '-access-{}'.format(i)
            process = self.access_processes.get(process_name, None)
            if process:
                if process.is_alive():
                    continue
                else:
                    process.terminate()
            p = start_process(target=self.run_access_worker, args=(i,), name=process_name, daemon=True)
            module_logger.info('Started a new access worker process {}'.format(process_name))
            self.access_processes[process_name] = p
            self.all_processes.add(p)

    # controller/server设置

    def setup(self):
        rt_g.md_type_registry = default_registry
        rt_g.event_list = registered_event_list
        rt_g.event_targets = get_registered_target()
        rt_g.language = self.config_collection.normal_config.LANGUAGE
        rt_g.worker_tag = '-'.join([self.worker_tag_prefix, 'access', 'controller', str(os.getpid())])

    def run(self):
        self.setup()
        module_logger.info(_('Access RPC server {}:{} is starting.').format(self.rpc_host, self.rpc_port))
        gevent.spawn(
            self.controller_alive_metric,
        )
        try:
            if not self.solo:
                self.run_access_workers()
                self.run_functional_workers()
                join_service_party(self.service_ha)
                self.workers_alive_keep_up()
            else:
                self.all_processes.add(current_process())
                self.run_access_worker('controller')
        except KeyboardInterrupt:
            module_logger.info(_('Access RPC server is stopped.'))
        finally:
            pass


@click.command('run-service-server')
@click.option('-h', '--host', help=_('Server host.'))
@click.option('-p', '--port', type=int, help=_('Server port.'))
@click.option('-w', '--access_worker_num', type=int, help=_('Worker num.'))
@click.option(
    '-s',
    '--solo',
    is_flag=True,
    help=_('Start single process server.'),
)
@click.option('--ha', is_flag=True, help=_('Start in HA mode.'))
def run_server_from_cli(host, port, access_worker_num, solo, ha):
    def run_server():
        ss = Service(host, port, access_worker_num, solo, ha)
        ss.run()

    rt_g.config_collection = default_configs_collection
    logging_config = rt_g.config_collection.logging_config
    logging_config.suffix_postfix = 'access_rpc_server'

    init_logging(logging_config, True)
    update_report_logging(logging_config)
    update_record_handler(logging_config)
    if solo:
        run_with_reloader(run_server)
    else:
        run_server()


@click.command()
def run_service_function_persist_record():
    persist_record()


@click.command()
def run_service_function_replay_record():
    replay_record()


@click.command()
def watch_backend_nodes_health():
    """
    此命令仅供测试
    """
    watch_backend_nodes_health_function()


@click.group()
def cli():
    pass


cli.add_command(run_server_from_cli)
cli.add_command(run_service_function_replay_record)
cli.add_command(run_service_function_persist_record)
cli.add_command(watch_backend_nodes_health)  # 此命令仅供测试
