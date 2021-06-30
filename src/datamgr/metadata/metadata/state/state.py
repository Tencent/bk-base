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
import os
import time
from abc import abstractmethod
from copy import deepcopy
from enum import Enum
from functools import partial, wraps

import gevent
from cached_property import threaded_cached_property
from gevent import Timeout, kill
from gevent.event import Event
from gevent.lock import RLock
from kazoo.client import KazooClient, KazooRetry
from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.protocol.states import KazooState
from kazoo.recipe.cache import TreeCache, TreeEvent
from kazoo.recipe.lock import Lock
from retry import retry

from metadata.backend.interface import BackendType, RawBackendType
from metadata.exc import InteractTimeOut, ValidWaitError
from metadata.runtime import rt_context, rt_g
from metadata.util.common import StrictABCMeta
from metadata.util.i18n import lazy_selfish as _

state_lock = RLock()


class StateMode(Enum):
    """
    状态模式
    """

    ONLINE = 'online'  # 使用ZK的在线模式
    LOCAL = 'local'  # 本地模式


class BackendHAState(Enum):
    """
    后端高可用状态
    """

    # 仅在集群的ha_type中使用
    MASTER = 'master'
    SLAVE = 'slave'
    MAIN = 'main'
    # 在ha_state中使用
    SWITCH_PREPARE = 'switch_prepare'
    SWITCHING = 'switching'
    OFF = 'off'
    ON = 'on'

    # 当slave状态异常时使用
    CHAOTIC = 'chaotic'


class CommonState(Enum):
    """
    后端运行状态
    """

    OFF = 'off'
    ON = 'on'


class EnhancedZKLock(Lock):
    def __init__(self, acquire_blocking=True, acquire_timeout=None, acquire_ephemeral=True, *args, **kwargs):
        super(EnhancedZKLock, self).__init__(*args, **kwargs)
        self.acquire_blocking = acquire_blocking
        self.acquire_timeout = acquire_timeout
        self.acquire_ephemeral = acquire_ephemeral

    def __enter__(self):
        self.acquire(self.acquire_blocking, self.acquire_timeout, self.acquire_ephemeral)


class OnlineCache(TreeCache):
    def __init__(self, *args, **kwargs):
        super(OnlineCache, self).__init__(*args, **kwargs)

    def __getitem__(self, item):
        node_data = self.get_data('/'.join([self._root._path, item]))
        if node_data:
            return json.loads(node_data.data)
        else:
            raise KeyError('{} is not existed in zk cache'.format(item))


class BaseState(object, metaclass=StrictABCMeta):
    """
    状态实例基类。
    """

    __abstract__ = True

    ONLINE = 0  # 在线（online模式有效模式）
    SUSPENDED = 1  # 未明状态
    EXPIRED = 2  # 过期
    IN_EXCEPTION = 3  # 异常状态
    LIMBO = 4  # 未启动
    LOCAL = 5  # 本地 （local模式有效模式）

    def __init__(self, config_collection=None):
        self.config_collection = rt_context.config_collection if not config_collection else config_collection
        self.db_conf = self.config_collection.db_config
        self.normal_conf = self.config_collection.normal_config
        self.status = self.LIMBO
        self.lock = RLock()

        self.logger = logging.getLogger(self.__class__.__name__)

    @threaded_cached_property
    def identifier(self):
        """当前状态实例标识"""
        default_tag = '_'.join([getattr(rt_context, 'worker_tag_prefix', 'alone'), str(os.getpid())])
        return getattr(rt_context, 'worker_tag', default_tag)

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @abstractmethod
    def __delitem__(self, key):
        pass

    @abstractmethod
    def syncing(self):
        """
        初始化并开启状态实例。
        :return:
        """

    @abstractmethod
    def stop(self):
        """
        停止状态实例。
        :return:
        """


class BasicOnlineState(BaseState):
    """
    在线状态实例基类。基于ZK。
    """

    def __init__(self, path='/metadata', *args, **kwargs):
        super(BasicOnlineState, self).__init__(*args, **kwargs)
        self.root_path = path

        # 在线状态
        self.is_online = Event()
        # 缓存有效状态
        self.is_cache_valid = Event()

        self.cache = OnlineCache(self.zk_client, self.root_path)
        self.cache.listen_fault(self.watch_cache_fault)
        self.cache.listen(self.watch_cache_event)

    @threaded_cached_property
    def zk_client(self):
        if not hasattr(rt_context, 'zk_client'):
            client = KazooClient(
                self.db_conf.ZK_ADDR,
                handler=SequentialGeventHandler(),
                connection_retry=KazooRetry(max_tries=-1, backoff=2, deadline=86400),
            )
            rt_g.zk_client = client
        return rt_context.zk_client

    def watch_cache_fault(self, exc):
        """
        检测基于数据变更的回调异常。

        :param exc: 异常
        :return:
        """
        self.status = self.IN_EXCEPTION
        self.is_online.clear()
        self.logger.error('Fail in online state process. The exc is {}. Rejoin party now.'.format(exc))
        time.sleep(1)
        from metadata.support import join_service_party

        gevent.spawn(join_service_party, service_ha=True, rejoin=True)

    def watch_cache_event(self, event):
        """
        检测ZK和Cache状态来刷新状态实例的状态。

        :param event: TreeEvent事件
        :return:
        """
        if event.event_data:
            zk_path = event.event_data.path
            if zk_path.startswith('/'.join([self.root_path, 'backends_ha_state'])) or zk_path.startswith(
                '/'.join([self.root_path, 'backend_configs'])
            ):
                self.logger.info(
                    'Caching data is changing. The status is {}/{}.'.format(event.event_type, event.event_data)
                )
        if event.event_type == TreeEvent.CONNECTION_RECONNECTED:
            self.status = self.ONLINE
            self.is_online.set()
        elif event.event_type == TreeEvent.CONNECTION_LOST:
            self.status = self.EXPIRED
            self.is_online.clear()
            self.is_cache_valid.clear()
        elif event.event_type == TreeEvent.CONNECTION_SUSPENDED:
            self.status = self.SUSPENDED
            self.is_online.clear()
        elif event.event_type == TreeEvent.INITIALIZED:
            self.is_cache_valid.set()
            self.is_online.set()

    def __getitem__(self, key):
        info = self.zk_client.get('/'.join([self.root_path, key]))
        return json.loads(info[0])

    def __setitem__(self, key, value):
        value = json.dumps(value).encode('utf-8')
        path = '/'.join([self.root_path, key])
        if not self.zk_client.exists(path):
            self.zk_client.create(path, value, makepath=True)
        else:
            self.zk_client.set(path, value)

    def __delitem__(self, key):
        path = '/'.join([self.root_path, key])
        return self.zk_client.delete(path)

    def __contains__(self, key):
        path = '/'.join([self.root_path, key])
        return self.zk_client.exists(path)

    def wait_to_valid(self, timeout=0, event='is_online', raise_err=True):
        """
        确认当前状态实例的某个状态正常，或等待其变为正常。

        :param event: 某个状态事件
        :param timeout: 等待时间
        :param raise_err: 若等待online超时，是否抛出异常。
        :return:
        """
        ret = getattr(self, event).wait(timeout)
        if raise_err and not ret:
            raise ValidWaitError('Too long to wait state status {} valid. Now status is {}'.format(event, self.status))
        return ret

    def syncing(self):
        self.zk_client.start()
        self.cache.start()
        self.is_online.wait(self.normal_conf.STATE_WAIT_TIMEOUT)

    def stop(self):
        self.cache.close()
        self.zk_client.stop()


class OnlineSyncPrimitiveMixIn(BasicOnlineState):
    """
    各种分布式元语
    """

    @threaded_cached_property
    def interactor_commit_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "interactor_commit_lock"]),
            identifier=self.identifier,
            acquire_timeout=self.config_collection.interactor_config.COMMIT_TIMEOUT,
        )

    @threaded_cached_property
    def transaction_counter(self):
        return self.zk_client.Counter(
            '/'.join([self.root_path, "counter", "transaction_counter"]),
        )

    @threaded_cached_property
    def replay_switch_barrier(self):
        return self.zk_client.Barrier('/'.join([self.root_path, "barrier", "replay_switch_barrier"]))

    @threaded_cached_property
    def state_manager_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "state_manager_lock"]),
            identifier=self.identifier,
            acquire_timeout=self.normal_conf.STATE_WAIT_TIMEOUT,
        )

    @threaded_cached_property
    def record_replay_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "record_replay_lock"]),
            identifier=self.identifier,
            acquire_timeout=self.normal_conf.STATE_WAIT_TIMEOUT,
        )

    @threaded_cached_property
    def backend_nodes_health_watch_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "backend_nodes_health_watch"]),
            identifier=self.identifier,
            acquire_timeout=self.normal_conf.STATE_WAIT_TIMEOUT,
        )

    @threaded_cached_property
    def service_load_info_watch_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "service_load_info_watch_lock"]),
            identifier=self.identifier,
            acquire_timeout=self.normal_conf.STATE_WAIT_TIMEOUT,
        )

    @threaded_cached_property
    def record_persistence_lock(self):
        return EnhancedZKLock(
            client=self.zk_client,
            path='/'.join([self.root_path, "lock", "record_persistence_lock"]),
            identifier=self.identifier,
            acquire_timeout=self.normal_conf.STATE_WAIT_TIMEOUT,
        )

    def zk_fault_react(
        self,
        greenlet,
        state,
    ):
        if state in (KazooState.LOST, KazooState.SUSPENDED):
            if not greenlet.dead:
                kill(greenlet)
                self.logger.info('Greenlet {} is killed'.format(greenlet))

    def run_with_state_control(self, func):
        @wraps(func)
        def wrap(*args, **kwargs):
            listener = None
            try:
                greenlet = gevent.spawn(func, *args, **kwargs)
                greenlet.name = func.__name__
                listener = partial(self.zk_fault_react, greenlet)
                self.zk_client.add_listener(listener)
                self.logger.info('Running greenlet {}.'.format(greenlet))
                greenlet.join()
                self.logger.info('Greenlet {} exited.'.format(greenlet))
            finally:
                if listener in self.zk_client.state_listeners:
                    self.zk_client.remove_listener(listener)

        return wrap


class OnlineState(OnlineSyncPrimitiveMixIn, BasicOnlineState):
    pass


class LocalState(BaseState):
    def __init__(self, *args, **kwargs):
        super(LocalState, self).__init__(*args, **kwargs)
        self.local_store = {}

    def __getitem__(self, key):
        return self.local_store.__getitem__(key)

    def __setitem__(self, key, value):
        self.local_store.__setitem__(key, value)

    def __delitem__(self, key):
        return self.local_store.__delitem__(key)

    def __contains__(self, key):
        return self.local_store.__contains__(key)

    def syncing(self):
        return

    def stop(self):
        return


class UniversalState(object):
    """
    支持多模式的状态实例，用于维护和监控不同模式下的系统状态变更。
    """

    def __init__(self, path='/metadata', mode=None, config_collection=None):
        self.config_collection = rt_context.config_collection if not config_collection else config_collection
        self.normal_conf = self.config_collection.normal_config
        self.root_path = path
        self.mode = StateMode(mode) if mode else StateMode(self.normal_conf.STATE_MODE)
        if self.mode is StateMode.ONLINE:
            self.inner_state = OnlineState(path)
        elif self.mode is StateMode.LOCAL:
            self.inner_state = LocalState()

        self.logger = logging.getLogger(self.__class__.__name__)

    def __getitem__(self, key):
        return self.inner_state.__getitem__(key)

    def __setitem__(self, key, value):
        self.inner_state.__setitem__(key, value)

    def __delitem__(self, key):
        return self.inner_state.__delitem__(key)

    def __contains__(self, key):
        return self.inner_state.__contains__(key)

    def __getattr__(self, item):
        return getattr(self.inner_state, item)

    def syncing(self):
        """
        启动状态实例，保持状态同步

        :return: 当前状态实例的状态
        """
        self.inner_state.syncing()
        self.logger.info('State is syncing now.')
        return self.status

    def dgraph_best_effort(self, backend_id):
        info = self.cache['backend_configs']
        return info[RawBackendType.DGRAPH.value].get(backend_id, {}).get('be', False)


class StateUtils(UniversalState):
    def __init__(self, *args, **kwargs):
        super(StateUtils, self).__init__(*args, **kwargs)
        self.supported_state_categories = [
            'backends_ha_state',
            'backend_configs',
            'backend_warning',
            'service_switches',
            'service_load_info',
        ]
        if self.mode is StateMode.ONLINE:
            self.cache.listen(self.react_on_online_data_change)
        self.inner_state.backends_is_sat = Event()

    def init(self):
        """
        初始化一系列状态配置。

        """
        backend_info = getattr(self.config_collection.normal_config, 'AVAILABLE_BACKEND_INSTANCES')
        backend_ha = getattr(self.config_collection.normal_config, 'BACKEND_HA')
        backends_ha_state = {}
        service_switches = getattr(self.config_collection.normal_config, 'SERVICE_SWITCHES')
        cold_backend_info = getattr(self.config_collection.normal_config, 'COLD_BACKEND_INSTANCE', {})
        if backend_ha and backend_info:
            for k, v in backend_info.items():
                if len(v) == 2:
                    backends_ha_state[k] = {}
                    for i_k, i_v in v.items():
                        backends_ha_state[k][i_v['ha_state']] = {'id': i_k, 'state': BackendHAState.ON.value}
        if self.mode == StateMode.ONLINE and self.wait_to_valid():
            with self.state_manager_lock:
                if 'backends_ha_state' not in self and 'backend_configs' not in self:
                    self['backends_ha_state'] = backends_ha_state
                    self['backend_configs'] = backend_info
                if 'service_switches' not in self:
                    self['service_switches'] = service_switches
                if 'backend_warning' not in self:
                    self['backend_warning'] = {}
                if 'service_load_info' not in self:
                    self['service_load_info'] = {}
                if backend_ha and backends_ha_state:
                    # 创建主备线上服务集群实例
                    self.setup_backend_adapters(
                        self['backends_ha_state'],
                        self['backend_configs'],
                    )
                self.logger.info('Online state is inited.')
        else:
            self['backend_configs'] = backend_info
            self['service_switches'] = service_switches
            self['backend_warning'] = {}
            self['service_load_info'] = {}
            self.logger.info('Local state is inited.')
        # 冷数据集群backend设置
        if cold_backend_info:
            gevent.spawn(self.setup_cold_backend_adapters, cold_backend_info)

    def cleanup(self):
        """
        清理已存在的状态配置。

        :return:
        """
        if self.mode == StateMode.ONLINE:
            for c in self.supported_state_categories:
                if c in self:
                    del self[c]

            self.logger.info('The online status is cleaned.')
        else:
            self.local_store.clear()

    @retry(tries=3, delay=0.1)
    def setup_backend_adapters(self, ha_state, config):
        """
        根据配置，设置主备集群适配器。（目前仅支持dgraph）

        :param config:
        :param ha_state:
        :return:
        """
        if ha_state:
            master_id = ha_state['dgraph']['master']['id']
            master_config = config['dgraph'][master_id]['config']
            master_ver = config['dgraph'][master_id].get('ver', None)
            ret_m, id_m = self.set_per_dgraph_adapters('master', master_config, master_id, master_ver)
            slave_id = ha_state['dgraph']['slave']['id'] if ha_state['dgraph'].get(str('slave'), {}) else None
            ret_s = False
            id_s = None
            if slave_id:
                slave_config = config['dgraph'][slave_id]['config']
                slave_ver = config['dgraph'][slave_id].get('ver', None)
                ret_s, id_s = self.set_per_dgraph_adapters('slave', slave_config, slave_id, slave_ver)
            if ret_m or ret_s:
                if hasattr(rt_g, 'layers'):
                    rt_g.layers.backends_group_storage[BackendType('dgraph')] = rt_g.dgraph_backend
                    rt_g.layers.dgraph_backend = rt_g.dgraph_backend
                    rt_g.layers.backends_group_storage[BackendType('dgraph_backup')] = rt_g.dgraph_backup_backend
                    rt_g.layers.dgraph_backup_backend = rt_g.dgraph_backup_backend
                    self.inner_state.backends_is_sat.set()
                self.logger.info('Dgraph HA adapters M/S: {}/{} is sat.'.format(id_m, id_s))

    @retry(tries=1, delay=0.1)
    def setup_cold_backend_adapters(self, config):
        """
        根据配置，设置冷数据集群适配器。（目前仅支持dgraph）

        :param config: 冷数据集群配置
        """
        with Timeout(3, InteractTimeOut(_('set cold backend timeout in {}.').format(3))):
            backend_id = 'cold'
            backend_config = config['dgraph'][backend_id]['config']
            if backend_config:
                backend_ver = backend_config.get('ver', None)
                ret_c, id_c = self.set_per_dgraph_adapters(
                    'cold', backend_config, backend_id, backend_ver, rt_var_name='dgraph_cold_backend'
                )
                if ret_c and hasattr(rt_g, 'layers'):
                    rt_g.layers.backends_group_storage[BackendType('dgraph_cold')] = rt_g.dgraph_cold_backend
                    rt_g.layers.dgraph_cold_backend = rt_g.dgraph_cold_backend
                    self.logger.info('Dgraph Cold adapters {} is sat.'.format(id_c))

    @staticmethod
    def set_per_dgraph_adapters(state, config, backend_id, backend_ver=None, rt_var_name=None, read_only=False):
        from metadata.backend.dgraph.backend import DgraphBackend, DgraphSessionHub

        if rt_var_name is None:
            rt_var_name = 'dgraph_backend' if state == 'master' else 'dgraph_backup_backend'
        if (
            hasattr(
                rt_g,
                rt_var_name,
            )
            and getattr(getattr(rt_g, rt_var_name), 'id') == backend_id
        ):
            if set(getattr(rt_g, rt_var_name).session_hub.client.servers) != set(config['SERVERS']):
                pass
            else:
                return False, None
        dgraph_conf = deepcopy(rt_context.config_collection.dgraph_backend_config)
        for k, v in list(config.items()):
            setattr(dgraph_conf, k, v)
        db = DgraphBackend(
            DgraphSessionHub(dgraph_conf.SERVERS, dgraph_conf.POOL_SIZE, dgraph_conf.POOL_MAX_SIZE),
            id_=backend_id,
            ver_=backend_ver,
        )
        # 非只读模式则载入类型系统
        if not read_only:
            db.gen_types(list(rt_context.md_types_registry.values()))
        setattr(rt_g, rt_var_name, db)
        return True, backend_id

    def react_on_online_data_change(self, event):
        """
        监控节点数据变动。

        :param event: TreeEvent事件。
        :return:
        """
        if event.event_type in (TreeEvent.NODE_ADDED, TreeEvent.NODE_UPDATED):
            if event.event_data.path == '/'.join([self.root_path, 'backends_ha_state']):
                self.setup_backend_adapters(
                    json.loads(event.event_data.data),
                    self['backend_configs'],
                )
            elif event.event_data.path == '/'.join([self.root_path, 'backend_configs']):
                self.setup_backend_adapters(self['backends_ha_state'], json.loads(event.event_data.data))


class State(StateUtils, UniversalState):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.inner_state.stop()
