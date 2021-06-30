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
from enum import Enum

from attr import fields_dict
from retry import retry
from tinyrpc.dispatch import public

from metadata.backend.interface import BackendType
from metadata.db_models.meta_service.log_models import DbOperateLog
from metadata.exc import InteractorExecuteError
from metadata.interactor.core import Operation
from metadata.interactor.interact import ParallelInteractor, SingleInteractor
from metadata.runtime import rt_context
from metadata.service.access.layer_interface import BaseAccessLayer
from metadata.support import quick_support
from metadata.util.i18n import selfish as _
from metadata.util.orm import model_to_dict
from metadata_biz.interactor.mappings import db_model_md_name_mappings

module_logger = logging.getLogger(__name__)


class ContentMode(Enum):
    ID = 'id'
    CONTENT = 'content'


class BridgeInteractor(ParallelInteractor):
    operate_record_fields = fields_dict(Operation)

    def __init__(self, *args, **kwargs):
        super(BridgeInteractor, self).__init__(*args, **kwargs)
        self.db_operate_logs = None
        self.log_session = None
        self.mode = None

    def dispatch(self, db_operations_lst, batch=False, mode='id'):
        self.mode = ContentMode(mode)
        if self.mode == ContentMode.ID:
            with quick_support.bkdata_log_session() as self.log_session:
                self.db_operate_logs, self.operate_records = self.build_operate_records(db_operations_lst)
                super(BridgeInteractor, self).dispatch(self.operate_records, batch)
        else:
            self.db_operate_logs, self.operate_records = self.build_operate_records(db_operations_lst)
            super(BridgeInteractor, self).dispatch(self.operate_records, batch)
        self.log_session = None

    def apply(self):
        super(BridgeInteractor, self).apply()
        if self.log_session:
            for item in self.db_operate_logs:
                item.synced = True
            self.log_session.commit()

    def build_operate_records(self, db_operations_lst):
        db_operate_logs = None
        operate_records = []

        if self.mode == ContentMode.ID:
            operate_id_lst = db_operations_lst
            db_operate_logs = self.log_session.query(DbOperateLog).filter(DbOperateLog.id.in_(operate_id_lst)).all()
            diff = set(operate_id_lst) - {ret.id for ret in db_operate_logs}
            if diff:
                raise InteractorExecuteError(_('Some db operate logs are missing. The id list is {}').format(diff))
        elif self.mode == ContentMode.CONTENT:
            db_operate_logs = [DbOperateLog(**item) for item in db_operations_lst]
        for log in db_operate_logs:
            kwargs = {k: v for k, v in model_to_dict(log).items()}
            kwargs['identifier_value'] = kwargs['primary_key_value']
            if kwargs['table_name'] not in db_model_md_name_mappings:
                raise InteractorExecuteError('The table name {} is not watched in bridge.'.format(kwargs['table_name']))
            kwargs['type_name'] = db_model_md_name_mappings[kwargs['table_name']]
            kwargs['extra_'] = dict(mysql_table_name=kwargs['table_name'], updated_by=kwargs['updated_by'])
            o_r = Operation(**{k: v for k, v in kwargs.items() if k in self.operate_record_fields})
            if self.mode == ContentMode.ID:
                o_r.operate_id = 'sync_{}'.format(log.id)
            operate_records.append(o_r)
        if BackendType.CONFIG_DB in self.available_backends:
            db_names = {item.db_name for item in db_operate_logs}
            if len(db_names) > 1:
                raise InteractorExecuteError('Only accept one config db in affect_original mode.')
            self.available_backends[BackendType.CONFIG_DB] = db_names.pop()
        return db_operate_logs, operate_records


class ColdInteractor(SingleInteractor):
    """
    冷数据(离线更新)集群调度
    """

    operate_record_fields = fields_dict(Operation)

    def __init__(self, *args, **kwargs):
        super(ColdInteractor, self).__init__(*args, **kwargs)

    def check_target_servers(self):
        """
        检查dgraph备集群配置是否指向指定冷数据后端集群
        :return: boolean
        """
        cold_config = getattr(self.config_collection.normal_config, 'COLD_BACKEND_INSTANCE', {})
        cold_servers = cold_config['dgraph']['cold']['config']['SERVERS']
        dgraph_cold_backend = getattr(rt_context, 'dgraph_cold_backend', None)
        if dgraph_cold_backend is None:
            return False
        random_server = rt_context.dgraph_cold_backend.session_hub.client.server
        if random_server not in cold_servers:
            return False
        return True

    def build_operate_records(self, db_operations_lst):
        """
        sync数据转换为标准的操作数据格式
        :param db_operations_lst: list sync数据列表
        :return: list 标准操作数据列表
        """
        operate_records = []
        for kwargs in db_operations_lst:
            kwargs['identifier_value'] = kwargs['primary_key_value']
            if kwargs['table_name'] not in db_model_md_name_mappings:
                raise InteractorExecuteError('The table name {} is not watched in bridge.'.format(kwargs['table_name']))
            kwargs['type_name'] = db_model_md_name_mappings[kwargs['table_name']]
            kwargs['extra_'] = dict(mysql_table_name=kwargs['table_name'])
            o_r = Operation(**{k: v for k, v in list(kwargs.items()) if k in self.operate_record_fields})
            operate_records.append(o_r)
        return operate_records

    def dispatch(self, db_operations_list, batch=False, content_mode='id'):
        if self.check_target_servers() and content_mode == 'content':
            operate_records = self.build_operate_records(db_operations_list)
            super(ColdInteractor, self).dispatch(operate_records, batch)


class BridgeAccessLayer(BaseAccessLayer):
    """
    Bridge访问层。
    """

    @public
    @retry(tries=3, delay=0.5)
    def sync(self, db_operations_list, batch=False, content_mode='id', affect_original=False, affect_cold_only=False):
        """
        元数据同步。

        :param db_operations_list: 元数据db操作记录项的id列表或完整信息列表
        :param batch: 是否批量操作
        :param content_mode: content的类型，为id或完整内容
        :param affect_original: 是否先变更原始数据源，然后触发同步
        :param affect_cold_only: 只对冷数据集群进行操作
        :return:
        """
        try:
            backends_in_use_info = {BackendType(k): v for k, v in list(self.normal_conf.AVAILABLE_BACKENDS.items())}
            batch = True if len(db_operations_list) > self.normal_conf.BATCH_AUTO_ENABLE else batch
            if affect_original:
                backends_in_use_info[BackendType.CONFIG_DB] = None
            if affect_cold_only:
                b = ColdInteractor(
                    backend_type=BackendType.DGRAPH_COLD, interact_transaction_enabled=False, if_recording=False
                )
            else:
                b = BridgeInteractor(backends_in_use=backends_in_use_info)
            b.dispatch(db_operations_list, batch, content_mode)
        except Exception:
            self.logger.exception('Fail to sync.')
            raise


class ImportAccessLayer(BridgeAccessLayer):
    """
    Import访问层。
    """

    @public
    def import_content(self, db_operations_list):
        return self.sync(db_operations_list=db_operations_list, batch=True, content_mode='content')
