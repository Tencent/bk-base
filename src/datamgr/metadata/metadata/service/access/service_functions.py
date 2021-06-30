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

from kazoo.exceptions import LockTimeout

from metadata.interactor.record import KafkaRecordsPersistence, RecordsReplay
from metadata.runtime import rt_context, rt_g
from metadata.state.manage import BackendNodesHealthChecker, ServiceLoadChecker
from metadata.support import join_service_party
from metadata.util.log import init_logging, update_report_logging
from metadata_contents.config.conf import default_configs_collection

logger = logging.getLogger(__name__)


def reset_logging(suffix_postfix='service_functions'):
    rt_g.config_collection = default_configs_collection
    logging_config = rt_g.config_collection.logging_config
    logging_config.suffix_postfix = suffix_postfix

    init_logging(logging_config, True)
    update_report_logging(logging_config)


def persist_record():
    reset_logging('persist_record')
    normal_config = rt_context.config_collection.normal_config
    if not normal_config.SERVICE_HA:
        return
    logger.info('Start to run record persisting.')
    if not getattr(rt_context, 'worker_tag', None):
        rt_g.worker_tag = '_'.join([getattr(rt_context, 'worker_tag_prefix', 'alone'), str(os.getpid())])
    join_service_party(True)

    def run_persist_record():
        with KafkaRecordsPersistence() as p:
            p.persisting()

    while True:
        try:
            with rt_context.system_state.record_persistence_lock:
                rt_context.system_state.run_with_state_control(run_persist_record)()
        except LockTimeout:
            pass
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception('Fail to run persist recording.')
        time.sleep(1)


def replay_record():
    reset_logging('replay_record')
    normal_config = rt_context.config_collection.normal_config
    if not normal_config.SERVICE_HA or not normal_config.BACKEND_HA:
        return
    logger.info('Start to run record replay.')
    if not getattr(rt_context, 'worker_tag', None):
        rt_g.worker_tag = '_'.join([getattr(rt_context, 'worker_tag_prefix', 'alone'), str(os.getpid())])
    join_service_party(True)

    def run_replay_record():
        r = RecordsReplay()
        r.replay_serving()

    while True:
        try:
            with rt_context.system_state.record_replay_lock:
                rt_context.system_state.run_with_state_control(run_replay_record)()
        except LockTimeout:
            pass
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception('Fail to run persist recording.')
        time.sleep(1)


def watch_backend_nodes_health():
    reset_logging('dgraph_backends_health')
    normal_config = rt_context.config_collection.normal_config
    if not normal_config.SERVICE_HA or not normal_config.BACKEND_HA:
        return
    logger.info('Start to watch dgraph backends health.')
    if not getattr(rt_context, 'worker_tag', None):
        rt_g.worker_tag = '_'.join([getattr(rt_context, 'worker_tag_prefix', 'alone'), str(os.getpid())])
    join_service_party(True)

    c = BackendNodesHealthChecker(rt_context.system_state)

    def check_dgraph_backend_nodes_health():
        n = 0
        while True:
            c.get_status()
            c.maintain_status()
            if n % 3 == 0:
                n = 0
                ret, config, warning = c.check()
                if ret is True:
                    with c:
                        c.maintain(config, warning)
            time.sleep(1)
            n += 1

    while True:
        try:
            with rt_context.system_state.backend_nodes_health_watch_lock:
                rt_context.system_state.run_with_state_control(check_dgraph_backend_nodes_health)()
        except LockTimeout:
            pass
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception('Fail to watch dgraph backends health.')
        time.sleep(1)


def service_load_monitor():
    """监控metadata负载情况(每ss一次)"""
    reset_logging('service_load')
    logger.info('Start to watch metadata server load.')
    if not getattr(rt_context, 'worker_tag', None):
        rt_g.worker_tag = '_'.join([getattr(rt_context, 'worker_tag_prefix', 'alone'), str(os.getpid())])
    join_service_party(True)

    srv = ServiceLoadChecker(rt_context.system_state)

    def get_service_load_info():
        n = 0
        current_rules = None
        while True:
            # 约每5秒统计1次
            if n % 5 == 0:
                n = 0
                ret, load_info = srv.statistic()
                if ret is True or current_rules != {}:
                    logger.info('Service_load info changed, update to zk, rules: {}'.format(load_info.get('rules', {})))
                    with srv:
                        srv.maintain(load_info)
                        current_rules = load_info.get('rules', {})
            time.sleep(1)
            n += 1

    while True:
        try:
            with rt_context.system_state.service_load_info_watch_lock:
                rt_context.system_state.run_with_state_control(get_service_load_info)()
        except LockTimeout:
            pass
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception('Fail to watch service load info.')
        time.sleep(1)
