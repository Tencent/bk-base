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
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from enum import Enum
from random import choice

import numpy as np
import requests
from retry import retry

from metadata.backend.interface import RawBackendType
from metadata.exc import StateManageError
from metadata.runtime import rt_context, rt_g
from metadata.state.state import BackendHAState, CommonState
from metadata.util.common import StrictABCMeta
from metadata.util.i18n import lazy_selfish as _


class StateManager(object, metaclass=StrictABCMeta):
    __abstract__ = True

    def __init__(self, state, check_cnt=None, config_collection=None):
        self.state = state
        self.check_cnt = check_cnt if check_cnt else 20
        self.logger = logging.getLogger(__name__)
        self.config_collection = rt_context.config_collection if not config_collection else config_collection
        self.normal_conf = self.config_collection.normal_config
        self.logging_conf = self.config_collection.logging_config

    def __enter__(self):
        self.state.state_manager_lock.acquire(timeout=0.1 * self.check_cnt)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state.state_manager_lock.release()


class BackendSwitch(StateManager):
    def __init__(self, state, backend_type=RawBackendType.DGRAPH, check_cnt=20):
        super(BackendSwitch, self).__init__(state, check_cnt)
        self.backend_type = backend_type
        self.master_backend_id, self.slave_backend_id = None, None

    def __enter__(self):
        if (
            self.state['backends_ha_state'][self.backend_type.value]['master']['state'] == BackendHAState.ON.value
            and self.state['backends_ha_state'][self.backend_type.value]['slave']['state'] == BackendHAState.ON.value
        ):
            return super(BackendSwitch, self).__enter__()
        else:
            raise StateManageError(
                _('The backends ha state {} is not allowed to switch.'.format(self.state['backends_ha_state']))
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        backends_ha_state = self.state['backends_ha_state']
        backends_ha_state[self.backend_type.value]['master']['state'] = BackendHAState.ON.value
        backends_ha_state[self.backend_type.value]['slave']['state'] = BackendHAState.ON.value
        self.state['backends_ha_state'] = backends_ha_state
        super(BackendSwitch, self).__exit__(exc_type, exc_val, exc_tb)

    def switch(self):
        self.master_backend_id, self.slave_backend_id = (
            self.state['backends_ha_state'][self.backend_type.value]['master']['id'],
            self.state['backends_ha_state'][self.backend_type.value]['slave']['id'],
        )
        if self.switch_master():
            if self.switch_slave():
                # 切换backends_ha_state中的主从状态
                backends_ha_state = self.state['backends_ha_state']
                backends_ha_state[self.backend_type.value]['master']['id'] = self.slave_backend_id
                backends_ha_state[self.backend_type.value]['slave']['id'] = self.master_backend_id
                self.state['backends_ha_state'] = backends_ha_state
                # 切换backend_configs中的主从状态
                backend_configs = self.state['backend_configs']
                backend_configs[self.backend_type.value][self.slave_backend_id]['ha_state'] = 'master'
                backend_configs[self.backend_type.value][self.master_backend_id]['ha_state'] = 'slave'
                self.state['backend_configs'] = backend_configs
            else:
                raise StateManageError(_('Fail to switch slave {} backend.'.format(self.backend_type.value)))
        else:
            raise StateManageError(_('Fail to switch master {} backend.'.format(self.backend_type.value)))

    def switch_master(self):
        backends_ha_state = self.state['backends_ha_state']
        backends_ha_state[self.backend_type.value]['master']['state'] = BackendHAState.SWITCH_PREPARE.value
        self.state['backends_ha_state'] = backends_ha_state
        for i in range(self.check_cnt):
            if not self.state.interactor_commit_lock.contenders():
                backends_ha_state[self.backend_type.value]['master']['state'] = BackendHAState.SWITCHING.value
                self.state['backends_ha_state'] = backends_ha_state
                return True
            time.sleep(0.1)
        return False

    def switch_slave(self):
        backends_ha_state = self.state['backends_ha_state']
        backends_ha_state[self.backend_type.value]['slave']['state'] = BackendHAState.SWITCH_PREPARE.value
        self.state['backends_ha_state'] = backends_ha_state
        self.state.replay_switch_barrier.create()
        ret = self.state.replay_switch_barrier.wait(0.1 * self.check_cnt)
        if ret:
            backends_ha_state[self.backend_type.value]['slave']['state'] = BackendHAState.SWITCHING.value
            self.state['backends_ha_state'] = backends_ha_state
            return True
        return False


class DgraphBESwitch(StateManager):
    """
    切换BestEffort模式
    """

    def turn(self, status=CommonState.ON.value):
        master_id = self.state['backends_ha_state'][RawBackendType.DGRAPH.value]['master']['id']
        backend_configs = self.state['backend_configs']
        backend_configs[RawBackendType.DGRAPH.value][master_id]['be'] = status
        self.state['backend_configs'] = backend_configs
        self.logger.info('BE status is turned. The status now is {}'.format(self.state['backend_configs']))


class RecoverDgraphCluster(StateManager):
    """
    恢复Dgraph集群在线状态标记
    """

    def __init__(self, state, backend_type=RawBackendType.DGRAPH, check_cnt=20):
        super(RecoverDgraphCluster, self).__init__(state, check_cnt)
        self.backend_type = backend_type

    def recover(self, name='slave'):
        backends_ha_state = self.state['backends_ha_state']
        if backends_ha_state[self.backend_type.value][name]['state'] == BackendHAState.CHAOTIC.value:
            backends_ha_state[self.backend_type.value][name]['state'] = BackendHAState.ON.value
            self.state['backends_ha_state'] = backends_ha_state
            self.logger.info('the state of slave cluster is recovered: {}'.format(self.state['backends_ha_state']))


class DgraphVersionManager(StateManager):
    """
    DgraphBackend的版本管理
    """

    def __init__(self, state, backend_type=RawBackendType.DGRAPH, check_cnt=20):
        super(DgraphVersionManager, self).__init__(state, check_cnt)
        self.backend_type = backend_type
        self.master_backend_id, self.slave_backend_id = None, None

    def get_backend_version(self, backend_id=None):
        if backend_id:
            backend_configs = self.state['backend_configs']
            target_backend = backend_configs[self.backend_type.value].get(backend_id, None)
            if target_backend:
                return target_backend.get('ver', None)
        StateManageError(_('fail to get backend version, because {} is invalid.'.format(backend_id)))

    def set_backend_version(self, backend_id=None, ver=None):
        self.master_backend_id, self.slave_backend_id = (
            self.state['backends_ha_state'][self.backend_type.value]['master']['id'],
            self.state['backends_ha_state'][self.backend_type.value]['slave']['id'],
        )
        if backend_id and ver and backend_id in (self.master_backend_id, self.slave_backend_id):
            backend_configs = self.state['backend_configs']
            backend_configs[self.backend_type.value][backend_id]['ver'] = ver
            self.state['backend_configs'] = backend_configs
        else:
            raise StateManageError(
                _(
                    'fail to set backend version, because {} is invalid. should be one of[{}, {}]'.format(
                        backend_id, self.master_backend_id, self.slave_backend_id
                    )
                )
            )
        return True


class NodeHealthy(Enum):
    STATUS_NOT_ENOUGH = 'status_not_enough'
    FAIL_TO_GET_STATUS = 'fail_to_get_status'
    NOT_HEALTHY = False
    HEALTHY = True


class BackendNodesHealthChecker(StateManager):
    def __init__(self, *args, **kwargs):
        super(BackendNodesHealthChecker, self).__init__(*args, **kwargs)
        if not getattr(rt_context, 'backend_nodes_health_status', None):
            rt_g.backend_nodes_health_status = {'dgraph': defaultdict(dict)}
        self.backend_nodes_health_status = rt_g.backend_nodes_health_status
        self.last_maintain_time = None
        self.last_get_time = None

    def get_status(self, backend_type=RawBackendType.DGRAPH):
        """
        获取后端集群节点状态

        :param backend_type:集群类型
        :return:
        """
        if backend_type is not RawBackendType.DGRAPH:
            raise NotImplementedError('This manager only support dgraph now.')
        backend_type = backend_type.value

        ha_state, config, master_config, slave_config = self.get_config_in_state(backend_type)
        self.logger.info(
            {'backends_ha_state': ha_state, 'metric_type': 'backend_config', 'backend_configs': config},
            extra={'metric': True},
        )
        servers = set()
        for n, config_to_detect in enumerate([master_config, slave_config]):
            for k in ['SERVERS', 'OFF_SERVERS']:
                for item in config_to_detect.get(k, []):
                    self.logger.info(
                        {
                            'node': item,
                            'backend_type': 'dgraph',
                            'metric_type': 'backend_healthy_config',
                            'config_healthy_status': k,
                            'config_ha_status': 'master' if n == 0 else 'slave',
                        },
                        extra={'metric': True},
                    )
                    servers.add(item)
        for server in servers:
            status, get_time = self.get_dgraph_node_status(server)
            self.backend_nodes_health_status['dgraph'][server][get_time] = status
            self.last_get_time = get_time

    def maintain_status(self):
        """
        维护缓存的后端集群状态

        :return:
        """
        self.last_maintain_time = datetime.now()
        for server, series in self.backend_nodes_health_status['dgraph'].items():
            sorted_series = sorted(iter(series.items()), key=lambda item: item[0], reverse=True)
            for n, (get_time, status) in enumerate(sorted_series):
                if n > self.normal_conf.BACKEND_STATUS_CACHE_LENGTH:
                    series.pop(get_time)
                elif self.last_maintain_time - get_time >= timedelta(
                    seconds=self.normal_conf.BACKEND_STATUS_CACHE_TIME
                ):
                    series.pop(get_time)

    def get_dgraph_node_status(self, node_url):
        """
        获取具体某dgraph集群状态

        :param node_url: 集群url
        :return:
        """

        @retry(tries=3, delay=0.1, backoff=2)
        def _check():
            require_metrics_mapping = {
                'dgraph_num_queries_total{method="Server.Mutate",status=""}': 'dgraph_num_queries_total',
                'dgraph_num_queries_total{method="Server.Query",status=""}': 'dgraph_num_queries_total',
            }
            r = requests.get(node_url + '/debug/prometheus_metrics', timeout=3)
            r.raise_for_status()
            metrics_dict = dict()
            lines = r.text.split('\n')
            for line in lines:
                if line.startswith('#'):
                    continue
                if ' ' in line:
                    metric_key, metric_val = line.split(' ')
                    metrics_dict[metric_key] = float(metric_val)
                    if metric_key in require_metrics_mapping:
                        upper_metric_key = require_metrics_mapping[metric_key]
                        if upper_metric_key not in metrics_dict:
                            metrics_dict[upper_metric_key] = 0
                        metrics_dict[upper_metric_key] += float(metric_val)
            return metrics_dict, datetime.now()

        try:
            return _check()
        except Exception:
            self.logger.exception('Fail to check dgraph health.')
            return NodeHealthy.FAIL_TO_GET_STATUS, datetime.now()

    def get_config_in_state(self, backend_type_value):
        """
        获取当前状态配置

        :param backend_type_value: 集群类型
        :return:
        """
        ha_state, config = self.state['backends_ha_state'], self.state['backend_configs']
        master_id = ha_state[backend_type_value]['master']['id']
        slave_id = ha_state[backend_type_value]['slave']['id']
        master_config = config[backend_type_value][master_id]['config']
        slave_config = config[backend_type_value][slave_id]['config']
        return ha_state, config, master_config, slave_config

    def check(self, backend_type=RawBackendType.DGRAPH):
        """
        检查集群状态

        :param backend_type: 集群类型
        :return: (是否需要变更，变更内容)
        """
        if backend_type is not RawBackendType.DGRAPH:
            raise NotImplementedError('This manager only support dgraph now.')

        ha_state, config, master_config, slave_config = self.get_config_in_state(backend_type.value)

        nodes_state_change = False
        not_enough = False
        online_warning_config = None
        for (env, config_to_detect) in list({'master': master_config, 'slave': slave_config}.items()):
            active_servers = set()
            off_services = set()
            for k in ['SERVERS', 'OFF_SERVERS']:
                for node_url in config_to_detect.get(k, []):
                    state_now = NodeHealthy.HEALTHY if k == 'SERVERS' else NodeHealthy.NOT_HEALTHY
                    ret, node_static = self.check_dgraph_node_health(node_url, state_now=state_now)
                    if isinstance(node_static, dict):
                        for k_ in ['dgraph_predicate_stats', 'memstats']:
                            if k_ in node_static:
                                node_static.pop(k_)
                    self.logger.info(
                        {
                            'node_url': node_url,
                            'backend_type': 'dgraph',
                            'metric_type': 'backend_node_static',
                            'static': node_static,
                        },
                        extra={'output_metric': True},
                    )
                    if ret is NodeHealthy.HEALTHY:
                        active_servers.add(node_url)
                    elif ret is NodeHealthy.STATUS_NOT_ENOUGH:
                        not_enough = True
                        self.logger.warning('Status series of node {} is not enough.'.format(node_url))
                    else:
                        self.logger.info(
                            {
                                'node_url': node_url,
                                'backend_type': 'dgraph',
                                'metric_type': 'backend_unhealthy',
                                'static': node_static,
                                'status': ret,
                            },
                            extra={'output_metric': True},
                        )
                        off_services.add(node_url)
            if len(active_servers) == 0 and not not_enough:
                item = choice(list(off_services))
                active_servers.add(item)
                off_services.remove(item)
            if active_servers != set(config_to_detect['SERVERS']) and not not_enough:
                config_to_detect['SERVERS'] = list(active_servers)
                config_to_detect['OFF_SERVERS'] = list(off_services)
                nodes_state_change = True
            if env == 'master' and nodes_state_change:
                online_warning_config = config_to_detect
        return nodes_state_change, config, online_warning_config

    def maintain(self, config, warning=None):
        """
        根据check反馈的内容，维护配置

        :param config: 变更的配置
        :param warning: 警告信息
        :return:
        """
        self.state['backend_configs'] = config
        self.logger.info('Backend configs is changing, the status is {}'.format(config))
        self.logger.info(
            {
                'config': config,
                'backend_type': 'dgraph',
                'metric_type': 'maintain_unhealthy_backend',
            },
            extra={'metric': True},
        )
        if warning is not None:
            self.state['backend_warning'] = warning

    def check_dgraph_node_health(self, node_url, state_now=NodeHealthy.HEALTHY):
        """
        Dgraph节点后端健康判断逻辑

        :param node_url: 节点url
        :return:
        """
        series = self.backend_nodes_health_status['dgraph'][node_url]

        range_cnt = self.normal_conf.BACKEND_STATUS_DETECT_RANGE

        # 处理节点联不通问题
        # [Logic]统计近期的健康请求失败情况；若发现近期失败次数>1或最新一次健康请求失败,返回`FAIL_TO_GET_STATUS`
        sorted_series = sorted(iter(series.items()), key=lambda item: item[0], reverse=True)
        last_fail_get_counter = Counter(
            s for t, s in sorted_series[: range_cnt * 2] if s in (NodeHealthy.FAIL_TO_GET_STATUS,)
        )
        if (
            last_fail_get_counter[NodeHealthy.FAIL_TO_GET_STATUS] > 1
            or sorted_series[0][1] == NodeHealthy.FAIL_TO_GET_STATUS
        ):
            return NodeHealthy.FAIL_TO_GET_STATUS, None

        # 处理节点异常的问题
        # [Logic]有效健康信息数量不足, 返回`STATUS_NOT_ENOUGH`
        sorted_gotten_series = sorted(
            ((k, v) for k, v in series.items() if v is not NodeHealthy.FAIL_TO_GET_STATUS),
            key=lambda item: item[0],
            reverse=True,
        )
        if len(sorted_gotten_series) < range_cnt * 2 + 1:
            return NodeHealthy.STATUS_NOT_ENOUGH, sorted_series[0][1]

        # 处理故障节点恢复策略
        # [Logic]当前状态不健康情况下,如果pending_queries数量过高，继续保持`NOT_HEALTHY`
        if state_now is NodeHealthy.NOT_HEALTHY:
            if (
                sorted_gotten_series[0][1]['dgraph_pending_queries_total']
                > self.normal_conf.BACKEND_NODE_RECOVERY_PENDING_QUERIES_CNT
            ):
                return NodeHealthy.NOT_HEALTHY, sorted_gotten_series[0][1]

        # 故障发生
        # [Logic]获取当前时段和前一时段的请求量均值，如果backend处理的请求平均请求量下降70%以上，同事pending_query仍然维持1000以上，
        # 则认定发生故障, 返回`NOT_HEALTHY`
        concurrent_query_num_series = [
            s['dgraph_num_queries_total'] - sorted_gotten_series[n + 1][1]['dgraph_num_queries_total']
            for n, (t, s) in enumerate(sorted_gotten_series)
            if n < len(sorted_gotten_series) - 1
        ]
        average_concurrent_nums_recently = np.average(concurrent_query_num_series[range_cnt : range_cnt * 2])
        average_concurrent_nums_now = np.average(concurrent_query_num_series[0:range_cnt])
        self.logger.info(
            {
                'recently': average_concurrent_nums_recently,
                'now': average_concurrent_nums_now,
                'backend_type': 'dgraph',
                'metric_type': 'backend_concurrent_queries',
                'node': node_url,
            },
            extra={'metric': True},
        )
        if average_concurrent_nums_recently:
            current_query_num_dropped = (
                average_concurrent_nums_recently - average_concurrent_nums_now
            ) / average_concurrent_nums_recently > self.normal_conf.BACKEND_QUERY_DROP_PERCENT
        else:
            current_query_num_dropped = True
        if (
            current_query_num_dropped
            and sorted_gotten_series[0][1]['dgraph_pending_queries_total']
            >= self.normal_conf.BACKEND_NODE_PENDING_QUERIES_CNT
        ):
            return NodeHealthy.NOT_HEALTHY, sorted_gotten_series[0][1]

        # 节点正常，返回`HEALTHY`
        return NodeHealthy.HEALTHY, sorted_gotten_series[0][1]


class ServiceLoadChecker(StateManager):
    """
    metadata服务负载检测
    """

    def __init__(self, *args, **kwargs):
        super(ServiceLoadChecker, self).__init__(*args, **kwargs)

        self.config_collection = rt_context.config_collection
        self.normal_conf = self.config_collection.normal_config
        self.db_conf = self.config_collection.db_config
        self.logger = logging.getLogger(__name__)
        self.summarize_method = self.normal_conf.SUMMARIZE_METHOD

    def statistic(self, stat_win=5, delay=2):
        """
        根据策略检查服务器负载状态
        :param stat_win: 时间窗口(默认5s)
        :param delay: 时间窗口延时(默认2s)
        :return: tuple (boolean{是否触发过滤策略}, dict{负载信息})
        """
        now = datetime.now().replace(microsecond=0)
        load_info = dict(info={}, rules={})
        try:
            nearest_summaries = self.get_nearest_summaries(now, stat_win, delay)
            nearest_metric = self.get_nearest_metric(nearest_summaries, stat_win)
            rules = self.get_filter_rules(nearest_metric)
            load_info['info'] = nearest_metric
            load_info['rules'] = rules
        except Exception as e:
            self.logger.error('get load info failed, detail: {}'.format(e))
            return False, None
        filter_flag = False if not rules else True
        return filter_flag, load_info

    def get_nearest_summaries(self, now, stat_win=5, delay=2):
        """
        取最近的数据进行汇总计算

        :param now: 当前时间
        :param stat_win: 统计窗口,目前支持最大10s(s)
        :param delay: 窗口延时(s)
        :return: dict 统计详情
        """
        now = now - timedelta(seconds=delay)
        stat_win = 10 if stat_win > 10 else stat_win
        min_prefix_set = set()
        sec_range_dict = dict()
        cmd_list = []
        cmd_proto = (
            "grep \"{grep_pattern}\" {log_path} | grep '\"msg\": \"Executed' | grep '\"level\": \"INFO\"' | "
            "perl -ne 'print \"$1\\t$2\\t$3\\t$4\\t$5\\t$6\\n\" "
            "if /\"thread_name\": \"([^\"]+)\".+\"timestamp\": \"({perl_pattern}).+"
            "\"process_id\": \"([\\d]+)\".+Executed RPC Call ([\\S]+) in ([\\.0-9]+) with r_state ([^,]+)/'"
        )
        rpc_server_log_path = self.logging_conf.file_path.replace(
            self.logging_conf.file_name, 'metadata_access_rpc_server.common.log'
        )

        for i in range(stat_win):
            stat_time = now - timedelta(seconds=i)
            min_prefix = stat_time.strftime('%Y-%m-%dT%H:%M')
            sec_range = stat_time.strftime('%S')
            min_prefix_set.add(min_prefix)
            if min_prefix not in sec_range_dict:
                sec_range_dict[min_prefix] = dict(sec_from=sec_range, sec_to=sec_range)
            else:
                sec_range_dict[min_prefix]['sec_from'] = sec_range
        for min_prefix in sorted(min_prefix_set):
            sec_from = sec_range_dict[min_prefix]['sec_from']
            sec_to = sec_range_dict[min_prefix]['sec_to']
            from_tens, from_unit = sec_from[0:1], sec_from[1:2]
            to_tens, to_unit = sec_to[0:1], sec_to[1:2]
            if from_tens == to_tens:
                tens_str = to_tens
                unit_str = to_unit if from_unit == to_unit else '[{}-{}]'.format(from_unit, to_unit)
                cmd_list.append(
                    cmd_proto.format(
                        grep_pattern=min_prefix,
                        perl_pattern='{}:{}{}'.format(min_prefix, tens_str, unit_str),
                        log_path=rpc_server_log_path,
                    )
                )
            else:
                tens_str = from_tens
                unit_str = from_unit if from_unit == '9' else '[{}-{}]'.format(from_unit, 9)
                cmd_list.append(
                    cmd_proto.format(
                        grep_pattern=min_prefix,
                        perl_pattern='{}:{}{}'.format(min_prefix, tens_str, unit_str),
                        log_path=rpc_server_log_path,
                    )
                )
                if int(sec_to) > int(sec_from):
                    tens_str = to_tens
                    unit_str = to_unit if to_unit == '0' else '[{}-{}]'.format(0, to_unit)
                    cmd_list.append(
                        cmd_proto.format(
                            grep_pattern=min_prefix,
                            perl_pattern='{}:{}{}'.format(min_prefix, tens_str, unit_str),
                            log_path=rpc_server_log_path,
                        )
                    )
        nearest_summaries = dict()
        for cmd in cmd_list:
            status, capture = subprocess.getstatusoutput(cmd)
            if int(status) == 0:
                capture_lines = capture.split("\n")
                for line in capture_lines:
                    if not line:
                        continue
                    c_name, tm_str, p_id, method_name, cost, r_state = line.split("\t")
                    query_type = None
                    for query_type_name, query_item_list in list(self.summarize_method.items()):
                        if method_name in query_item_list:
                            query_type = query_type_name
                            break
                    if not query_type:
                        continue
                    if query_type not in nearest_summaries:
                        nearest_summaries[query_type] = dict()
                    if r_state not in nearest_summaries[query_type]:
                        nearest_summaries[query_type][r_state] = dict(cnt=0, dur_ms=0)
                    nearest_summaries[query_type][r_state]['cnt'] += 1
                    nearest_summaries[query_type][r_state]['dur_ms'] += int(float(cost) * 1000)
        return nearest_summaries

    @staticmethod
    def get_nearest_metric(nearest_summaries, stat_win=5):
        """
        获取指标信息
        :param nearest_summaries: 统计详情
        :param stat_win: 统计窗口
        :return: dict 指标字典
        """
        metric_info = dict()
        for query_type, item in list(nearest_summaries.items()):
            if not metric_info.get(query_type, {}):
                metric_info[query_type] = dict()
            metric_info[query_type]['succ_cnt'] = int(item.get('success', {}).get('cnt', 0))
            metric_info[query_type]['fail_cnt'] = int(item.get('fail', {}).get('cnt', 0))
            metric_info[query_type]['tot_cnt'] = int(
                metric_info[query_type]['succ_cnt'] + metric_info[query_type]['fail_cnt']
            )
            metric_info[query_type]['succ_qps'] = int(metric_info[query_type]['succ_cnt'] / stat_win)
            metric_info[query_type]['fail_qps'] = int(metric_info[query_type]['fail_cnt'] / stat_win)
            # 指标除数默认+1,防止非法除法运算
            metric_info[query_type]['succ_dur_ms'] = round(
                float(item.get('success', {}).get('dur_ms', 0) / (metric_info[query_type]['succ_cnt'] + 1)), 2
            )
            metric_info[query_type]['fail_dur_ms'] = round(
                float(item.get('fail', {}).get('dur_ms', 0) / (metric_info[query_type]['succ_cnt'] + 1)), 2
            )
            metric_info[query_type]['fail_rate'] = 100 * round(
                float(
                    metric_info[query_type]['fail_cnt']
                    / (metric_info[query_type]['succ_cnt'] + metric_info[query_type]['fail_cnt'] + 1)
                ),
                2,
            )
        return metric_info

    def get_filter_rules(self, metric_info):
        """
        获取服务负载情况信息
        :param metric_info: 指标信息
        :return: 过滤策略
        """
        warning_conf = self.normal_conf.FILTER_METRIC_WARNING_CONF
        rules = dict()
        for query_type in list(warning_conf.keys()):
            metric_item = metric_info.get(query_type, {})
            if metric_item and metric_item.get('tot_cnt', 0) > warning_conf[query_type]['enable_cnt']:
                # 失败率检查
                if (
                    metric_item.get('fail_cnt', 0) >= warning_conf[query_type]['fail_cnt']
                    and metric_item.get('fail_rate', 0) >= warning_conf[query_type]['fail_rate']
                ):
                    filter_rate = round(metric_item['fail_rate'] * warning_conf[query_type]['rate_factor'], 2)
                    filter_rate = (
                        filter_rate
                        if filter_rate < warning_conf[query_type]['filter_limit']
                        else warning_conf[query_type]['filter_limit']
                    )
                    if query_type not in rules:
                        rules[query_type] = {}
                    rules[query_type]['fail_rate_check'] = {
                        'rate': filter_rate,
                        'metric': {'fail_rate': metric_item['fail_rate'], 'fail_cnt': metric_item['fail_cnt']},
                    }
                # 平均耗时
                if metric_item and metric_item.get('succ_dur_ms', 0) > warning_conf[query_type]['cost_time_ms']:
                    filter_rate = round(
                        (metric_item['succ_dur_ms'] - warning_conf[query_type]['cost_time_ms'])
                        / metric_item['succ_dur_ms'],
                        2,
                    )
                    filter_rate = (
                        filter_rate
                        if filter_rate < warning_conf[query_type]['filter_limit']
                        else warning_conf[query_type]['filter_limit']
                    )
                    if query_type not in rules:
                        rules[query_type] = {}
                    rules[query_type]['avg_dur_check'] = {
                        'rate': filter_rate,
                        'metric': {'succ_dur_ms': metric_item['succ_dur_ms']},
                    }
        return rules

    def maintain(self, load_info=None):
        """
        记录负载信息到zk
        :param load_info: 负载信息
        :return: None
        """
        if isinstance(load_info, dict):
            self.state['service_load_info'] = load_info
            self.logger.info('service load info is changed, detail:{}'.format(load_info))


class ServiceSwitch(StateManager):
    """
    服务内容开关管理
    管理功能范围: shunt-分流; filter-限流; kafka-数据同步
    """

    def switch(self, service_name, sub_switch_name, switch_value=False):
        valid_keys = list(self.normal_conf.SERVICE_SWITCHES.keys())
        if service_name not in valid_keys:
            return False
        valid_sub_keys = list(self.normal_conf.SERVICE_SWITCHES[service_name].keys())
        if sub_switch_name not in valid_sub_keys:
            return False
        service_switches = self.state['service_switches']
        if service_name not in service_switches:
            service_switches[service_name] = dict()
        service_switches[service_name][sub_switch_name] = switch_value
        self.state['service_switches'] = service_switches
        self.logger.info('service switch status is turned. The status now is {}'.format(self.state['service_switches']))
        return True
