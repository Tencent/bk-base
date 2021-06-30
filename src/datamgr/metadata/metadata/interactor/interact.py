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
from abc import abstractmethod
from datetime import datetime

import gevent
from about_time import about_time
from cached_property import threaded_cached_property
from gevent import Timeout, joinall, killall

from metadata.backend.interface import BackendType
from metadata.exc import InteractTimeOut, NotImplementedByBackendError
from metadata.interactor.core import TransactionalOperations
from metadata.interactor.orchestrate import (
    BatchDGraphOrchestrator,
    BatchMySQLReplicaOrchestrator,
    MySQLReplicaOrchestrator,
    SingleDGraphOrchestrator,
)
from metadata.interactor.record import LocalRecordsPersistence
from metadata.interactor.transcation import TransactionalDispatchMixIn
from metadata.runtime import rt_context, rt_local, rt_local_manager
from metadata.util.common import StrictABCMeta
from metadata.util.context import inherit_local_ctx
from metadata.util.i18n import selfish as _


class Interactor(object, metaclass=StrictABCMeta):
    __abstract__ = True

    def __init__(self, backends_in_use, timeout_sec=None, if_recording=True):
        self.backends_in_use = backends_in_use
        self.config_collection = rt_context.config_collection
        self.timeout_sec = (
            self.config_collection.interactor_config.INTERACTOR_TIMEOUT if not timeout_sec else self.timeout_sec
        )
        self.son_timeout_sec = self.timeout_sec - 5 if self.timeout_sec > 10 else self.timeout_sec
        self.if_recording = if_recording
        self.local_persistence = LocalRecordsPersistence()
        self.transactional_operations = None
        self.operate_records = None
        self.batch = False
        self.transaction_count = 0

        self.logger = logging.getLogger(self.__module__ + '.' + self.__class__.__name__)

    @threaded_cached_property
    def sessions(self):
        """
        Interactor使用的各类session。

        """
        sessions = {}
        if self.backends_in_use.get(BackendType.MYSQL, False):
            sessions[BackendType.MYSQL] = rt_context.mysql_backend.operate_session().session
        if self.backends_in_use.get(BackendType.DGRAPH, False):
            sessions[BackendType.DGRAPH] = rt_context.dgraph_backend.operate_session()
        if self.backends_in_use.get(BackendType.DGRAPH_BACKUP, False):
            sessions[BackendType.DGRAPH_BACKUP] = rt_context.dgraph_backup_backend.operate_session()
        if self.backends_in_use.get(BackendType.DGRAPH_COLD, False):
            sessions[BackendType.DGRAPH_COLD] = rt_context.dgraph_cold_backend.operate_session()
        db_name = self.backends_in_use.get(BackendType.CONFIG_DB, None)
        if db_name:
            sessions[BackendType.CONFIG_DB] = rt_context.biz_mysql_backends[db_name].operate_session().session
        return sessions

    @abstractmethod
    def dispatch(self, *args, **kwargs):
        """
        与后端进行一次完整交互。

        """
        pass

    @abstractmethod
    def invoke(self, *args, **kwargs):
        """
        执行各类数据操作。提交前，可多次调用。

        """

    def apply(self, *args, **kwargs):
        """
        提交操作变更。

        """
        self.renew_at_apply_prepare()
        ret = self.basic_apply()
        self.renew_at_applied()
        return ret

    @abstractmethod
    def basic_apply(self, *args, **kwargs):
        """
        核心变更提交操作。

        """
        pass

    def setup_transactional_operations(self):
        dispatch_id = getattr(rt_context, 'dispatch_id', None)
        if dispatch_id:
            for record in self.operate_records:
                if not record.operate_id:
                    record.operate_id = dispatch_id
            self.transactional_operations = TransactionalOperations(
                operations=self.operate_records, operate_id=dispatch_id
            )
        else:
            self.transactional_operations = TransactionalOperations(operations=self.operate_records)

    def renew_at_apply_prepare(self):
        check_dict = {}
        self.logger.info('Transaction with ID {} is preparing to apply.'.format(self.transaction_count))
        for item in self.operate_records:
            item.transaction_id = self.transaction_count
            item_dict = {
                name: getattr(item, name)
                for name in dir(item)
                if not name.startswith('__') and not callable(getattr(item, name))
            }
            mysql_table_name = item_dict.get('extra_', {}).get('mysql_table_name', None)
            if mysql_table_name:
                if mysql_table_name not in check_dict:
                    check_dict[mysql_table_name] = []
                check_dict[mysql_table_name].append(item_dict)
        try:
            if not self.check_data_legality(check_dict):
                self.logger.exception('[check_lineage] Data sync is illegal: {}'.format(check_dict))
        except Exception as err:
            self.logger.exception('[check_lineage] check lineage occur error: {}'.format(err))
        self.transactional_operations.transaction_id = self.transaction_count
        self.transactional_operations.state = 'ApplyPrepare'
        self.local_persistence.save(self.transactional_operations) if self.if_recording else None

    def renew_at_applied(self):
        self.logger.info('Transaction with ID {} is applied.'.format(self.transaction_count))
        self.transactional_operations.apply_time = datetime.now()
        self.transactional_operations.state = 'Applied'
        self.local_persistence.save(self.transactional_operations) if self.if_recording else None

    def check_data_legality(self, check_dict):
        """
        检查同步数据的合法性
        check rules list:
        1.监控data_processing_relation写入,检查血缘完整性
        """
        if 'data_processing_relation' in check_dict and self.backends_in_use.get(BackendType.DGRAPH, False):
            return rt_context.dgraph_backend.check_lineage_integrity(
                check_dict['data_processing_relation'], self.sessions[BackendType.DGRAPH]
            )
        return True


class SingleInteractor(TransactionalDispatchMixIn, Interactor):
    """
    仅供单个后端使用的Interactor。
    """

    backup_backend_support = True

    def __init__(self, backend_type, *args, **kwargs):
        if backend_type is BackendType.MYSQL:
            raise NotImplementedByBackendError(_('Edit function is not enabled in MySQL backend.'))
        self.backend_type = backend_type
        super(SingleInteractor, self).__init__(backends_in_use={self.backend_type: True}, *args, **kwargs)
        self.backend_session = self.sessions[self.backend_type]

    def dispatch(self, operate_records, batch=False):
        self.operate_records = operate_records
        self.batch = batch
        self.setup_transactional_operations()
        self.local_persistence.save(self.transactional_operations)
        with Timeout(self.timeout_sec, InteractTimeOut(_('dispatch timeout in {}.').format(self.timeout_sec))):
            with self.backend_session:
                self.transactional_inner_dispatch()

    def invoke(self):
        setattr(rt_local, '{}_session_now'.format(self.backend_type.raw.value), self.backend_session)
        b = (
            BatchDGraphOrchestrator(backend_session=self.backend_session)
            if self.batch
            else SingleDGraphOrchestrator(backend_session=self.backend_session)
        )
        b.dispatch(self.operate_records)

    def basic_apply(self):
        ret = self.backend_session.commit()
        del self.sessions
        return ret


class ParallelInteractor(TransactionalDispatchMixIn, Interactor):
    """
    多个后端同时调度的Interactor。
    """

    backup_backend_support = False

    def __init__(self, *args, **kwargs):
        super(ParallelInteractor, self).__init__(*args, **kwargs)
        self.available_backends = self.backends_in_use
        self.metric_store = {}
        self.mysql_gl, self.dgraph_gl, self.config_db_gl = None, None, None
        self.invoking_greenlets = []
        self.record_keys_lst = None

    def dispatch(self, operate_records, batch):
        self.operate_records = operate_records
        self.batch = batch
        self.setup_transactional_operations()
        self.local_persistence.save(self.transactional_operations)
        self.record_keys_lst = [item.operate_id if item.operate_id else item for item in operate_records]
        with Timeout(self.timeout_sec, InteractTimeOut(_('dispatch timeout in {}.').format(self.timeout_sec))):
            try:
                # 针对多种后端启用情况，持有事务，执行数据交互操作
                for k, v in list(self.sessions.items()):
                    v.__enter__()
                self.transactional_inner_dispatch()
            finally:
                # 任何状态下，子同步协程都应该在完成该函数调用时结束。
                killall(self.invoking_greenlets)
                for k, v in list(self.sessions.items()):
                    v.__exit__(None, None, None)

    def invoke(self):
        self.invoking_greenlets = []
        if self.available_backends.get(BackendType.CONFIG_DB):
            g = gevent.spawn(inherit_local_ctx(self.renew_config_db, rt_local, rt_local_manager))
            self.invoking_greenlets.append(g)
        if self.available_backends.get(BackendType.MYSQL):
            g = gevent.spawn(inherit_local_ctx(self.interact_with_mysql, rt_local, rt_local_manager))
            self.invoking_greenlets.append(g)
        if self.available_backends.get(BackendType.DGRAPH):
            g = gevent.spawn(inherit_local_ctx(self.interact_with_dgraph, rt_local, rt_local_manager))
            self.invoking_greenlets.append(g)
        joinall(self.invoking_greenlets, raise_error=False)
        for item in self.invoking_greenlets:
            if not item.successful():
                item._raise_exception()

    def basic_apply(self):
        self.logger.info('sessions_this_time is {}'.format(self.sessions))
        if self.available_backends.get(BackendType.DGRAPH):
            with about_time() as t:
                self.sessions[BackendType.DGRAPH].commit()
            self.commit_metric(t, BackendType.DGRAPH.value)

        if self.available_backends.get(BackendType.CONFIG_DB, False):
            with about_time() as t:
                self.sessions[BackendType.CONFIG_DB].commit()
            self.commit_metric(t, BackendType.CONFIG_DB.value)

        if self.available_backends.get(BackendType.MYSQL, False):
            with about_time() as t:
                self.sessions[BackendType.MYSQL].commit()
            self.commit_metric(t, BackendType.MYSQL.value)

        del self.sessions

    def commit_metric(self, t, session_type):
        """
        commit 统计。

        :param t: 时间
        :param session_type: session类型
        :return:
        """
        self.logger.info(
            {
                'session_type': '{}'.format(session_type),
                'metric_type': 'session_commit',
                'elapsed_time': t.duration,
                'invoke_elapsed_time': self.metric_store.get('{}_invoke_elapsed_time'.format(session_type), 0.0),
                'operate_ids': self.metric_store.get('{}_operate_ids'.format(session_type), []),
            },
            extra={'output_metric': True},
        )

    def interact_with_mysql(
        self,
    ):
        """
        Interact with MySql

        :return:
        """
        with Timeout(
            self.son_timeout_sec, InteractTimeOut(_('mysql interact timeout in {}.').format(self.son_timeout_sec))
        ):
            rt_local.mysql_session_now = mysql_session = self.sessions[BackendType.MYSQL]
            mysql_b = (
                BatchMySQLReplicaOrchestrator(backend_session=mysql_session)
                if self.batch
                else MySQLReplicaOrchestrator(backend_session=mysql_session)
            )
            with about_time() as t:
                mysql_b.dispatch(self.operate_records)
            self.sync_metric(self.record_keys_lst, 'mysql', 'interactor_dispatch', t, self.metric_store)

    def interact_with_dgraph(self):
        """
        Interact with Dgraph

        :return:
        """
        with Timeout(
            self.son_timeout_sec, InteractTimeOut(_('dgraph interact timeout in {}.').format(self.son_timeout_sec))
        ):
            rt_local.dgraph_session_now = dgraph_session = self.sessions[BackendType.DGRAPH]
            dgraph_b = (
                BatchDGraphOrchestrator(backend_session=dgraph_session)
                if self.batch
                else SingleDGraphOrchestrator(backend_session=dgraph_session)
            )
            with about_time() as t:
                dgraph_b.dispatch(self.operate_records)
            self.sync_metric(self.record_keys_lst, 'dgraph', 'interactor_dispatch', t, self.metric_store)

    def renew_config_db(
        self,
    ):
        """
        Interact with ConfigDB

        :return:
        """
        with Timeout(
            self.son_timeout_sec, InteractTimeOut(_('config_db interact timeout in {}.').format(self.son_timeout_sec))
        ):
            rt_local.mysql_session_now = mysql_session = self.sessions[BackendType.CONFIG_DB]
            mysql_b = (
                BatchMySQLReplicaOrchestrator(backend_session=mysql_session)
                if self.batch
                else MySQLReplicaOrchestrator(backend_session=mysql_session)
            )
            with about_time() as t:
                mysql_b.dispatch(self.operate_records)
            self.sync_metric(self.record_keys_lst, 'config_db', 'interactor_dispatch', t, self.metric_store)

    def sync_metric(self, record_id_lst, backend_type, metric_type, cost, metric_store=None):
        if metric_store is not None:
            metric_store['{}_invoke_elapsed_time'.format(backend_type)] = cost.duration
            metric_store['{}_operate_ids'.format(backend_type)] = record_id_lst
        self.logger.info(
            {
                'record_ids': record_id_lst,
                'backend_type': backend_type,
                'metric_type': metric_type,
                'elapsed_time': cost.duration,
            },
            extra={'output_metric': True},
        )
