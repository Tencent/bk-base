# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有

from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
import time
from abc import abstractmethod
from enum import Enum
from threading import RLock

from dictdiffer import diff
from sqlalchemy import create_engine, event
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session as SessionProtoCls

from metadata_client.exc import SyncHookNotEnabledError
from metadata_client.models import DbOperateLog
from metadata_client.resource import global_settings, resource_lock
from metadata_client.util.common import StrictABCMeta
from metadata_client.util.orm import model_to_dict

# meta_sync 配置
client_for_meta_sync = None
model_logger = logging.getLogger(__name__)


class ContextType(Enum):
    INSTANCE = "instance"
    FACTORY = "factory"


class MetaSync(object):
    """
    通用元数据Sync Hook类

    Attributes:
        context: 上下文管理实例
        adapter: 实例对应的同步方案的适配器实例
        context_type: 传入上下文实例的类型
    """

    def __init__(self, context_cls, context_type="instance", adapt_to="sqlalchemy", *args, **kwargs):
        """
        sync_hook上下文构造函数

        :param context_cls: 上下文实例or上下文工厂
        :param context_type: factory(使用工厂新生成上下文) or instance(直接使用传入的上下文实例)
        :param adapt_to: 选择实现sync_hook的适配器
        :return: None
        """
        if not client_for_meta_sync:
            raise SyncHookNotEnabledError()
        self.context_type = context_type
        if context_type == ContextType.INSTANCE.value:
            self.context = context_cls
        else:
            self.context = context_cls(*args, **kwargs)
        self.adapter = supported_adapter[adapt_to](self)
        # 注册hooks,client全局只注册一次
        if adapt_to not in client_for_meta_sync.hooks_registered:
            with resource_lock:
                if adapt_to not in client_for_meta_sync.hooks_registered:
                    self.adapter.register_session_event()
                    client_for_meta_sync.hooks_registered[adapt_to] = True

    def __enter__(self):
        self.adapter.track()
        self.adapter.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.adapter.__exit__(exc_type, exc_val, exc_tb)
        by_force = False if exc_val is None else True
        r = self.adapter.untrack(by_force=by_force)
        # 通过sync_hook工厂创建的会话结束后直接关闭
        if self.context_type == ContextType.FACTORY.value and r:
            self.adapter.close_context()

    def sync(self, session=None):
        """
        利用bkdata_log数据库和rpc接口进行同步

        :param session: obj 实际进行sync操作的会话
        :return: None
        """
        records_list = self.adapter.get_records(session)
        if global_settings.SYNC_CONTENT_TYPE == "id":
            sync_contents = self.db_log_prepare(records_list)
        else:
            sync_contents = records_list
        if sync_contents:
            client_for_meta_sync.bridge_sync(
                sync_contents, content_mode=global_settings.SYNC_CONTENT_TYPE, batch=global_settings.SYNC_BATCH_FLAG
            )

    @staticmethod
    def get_log_db_session_factory():
        if not client_for_meta_sync.orp_log_instance:
            with resource_lock:
                if not client_for_meta_sync.orp_log_instance:
                    engine = create_engine(
                        global_settings.BKDATA_LOG_DB_URL, pool_recycle=global_settings.BKDATA_LOG_DB_RECYCLE
                    )
                    client_for_meta_sync.orp_log_instance = sessionmaker(bind=engine)
        return client_for_meta_sync.orp_log_instance

    @classmethod
    def db_log_prepare(cls, sync_contents):
        """
        预先将同步记录写入bkdata_log数据库, 返回存入数据的自增id, 以id模式进行同步

        :param sync_contents: 同步记录
        :return: list 同步记录的id列表
        """
        session_factory = cls.get_log_db_session_factory()
        session = session_factory()
        operations = [DbOperateLog(**operation) for operation in sync_contents]
        try:
            for operation in operations:
                session.add(operation)
            session.commit()
            op_ids = [m.id for m in operations]
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return op_ids


class RecordsStorage(object):
    """
    上下文记录基类

    """

    __metaclass__ = StrictABCMeta

    @abstractmethod
    def get_slot(self, identify):
        """
        获取当前事务的存储信息

        :param identify: 事务标识
        :return: None
        """
        pass

    @abstractmethod
    def set_slot(self, identify):
        """
        为当前事务注册存储位置

        :param identify: 事务标识
        :return: None
        """
        pass

    @abstractmethod
    def clear_slot(self, identify):
        """
        清空当前存储空间

        :param identify: 事务标识
        :return: None
        """
        pass

    @abstractmethod
    def delete_slot(self, identify):
        """
        删除当前事务的存储位置

        :param identify: 事务标识
        :return: None
        """
        pass


class SyncAdapter(object):
    """
    sync hook适配器基类

    Attributes:
        meta_sync_instance: 元数据同步Sync Hook实例
    """

    __metaclass__ = StrictABCMeta

    def __init__(self, meta_sync_instance):
        self.meta_sync_instance = meta_sync_instance

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def track(self):
        """
        开启当前上下文记录

        :return: None
        """
        pass

    @abstractmethod
    def untrack(self):
        """
        删除当前上下文记录

        :return: None
        """
        pass

    @abstractmethod
    def get_records(self):
        """
        获取当前上下文记录

        :return:
        """
        pass

    @abstractmethod
    def close_context(self):
        """
        关闭上下文

        :return:
        """
        pass


class SqlAlchemyRecordsStorage(RecordsStorage):
    """
    SQLAlchemy上下文记录实例
    """

    def __init__(self):
        self._storage = {}
        self._reference = {}

    def get_slot(self, identify):
        """
        获取当前事务存储上下文

        :param identify: 事务标识
        :return: list 对应会话的存储上下文
        """
        return self._storage.get(identify, [])

    def set_slot(self, identify):
        """
        为当前事务注册上下文存储位置

        :param identify: 事务标识
        :return: None
        """
        if identify not in self._storage:
            self._storage[identify] = []
        if identify not in self._reference:
            self._reference[identify] = 0

    def clear_slot(self, identify):
        """one
        清空当前事务上下文的存储空间

        :param identify: 事务标识
        :return: None
        """
        if identify in self._storage:
            self._storage[identify] = []

    def delete_slot(self, identify):
        """
        删除当前事务上下文的存储位置

        :param identify: 事务标识
        :return: None
        """
        self._storage.pop(identify, None)
        self._reference.pop(identify, None)

    def get_slots_index(self):
        """
        获取当前事务存储的会话索引

        :return: list 存储当前管理的
        """
        return self._storage.keys()

    def incr_reference(self, identify):
        """
        增加存储的引用数

        :param identify: 事务标识
        :return: None
        """
        if identify in self._reference:
            self._reference[identify] += 1

    def decr_reference(self, identify):
        """
        减少存储的引用数

        :param identify: 事务标识
        :return: None
        """
        if identify in self._reference:
            self._reference[identify] -= 1

    def none_reference(self, identify):
        """
        检查存储的引用数是否为空

        :param identify: 事务标识
        :return: boolean
        """
        return False if identify in self._reference and self._reference[identify] > 0 else True


class SqlAlchemySyncAdapter(SyncAdapter):
    """
    SQLAlchemy同步适配器

    Attributes:
        meta_sync_instance: 元数据同步Sync Hook实例
    """

    records_storage = SqlAlchemyRecordsStorage()
    storage_lock = RLock()

    def __init__(self, meta_sync_instance):
        super(SqlAlchemySyncAdapter, self).__init__(meta_sync_instance)
        self._session = self.meta_sync_instance.context

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if not self._session.autocommit:
                self._session.commit()
        except Exception:
            self._session.rollback()
            raise

    def close_context(self):
        self._session.close()

    def get_records(self, target_session=None):
        if not target_session:
            target_session = self._session
        return self.records_storage.get_slot(target_session)

    def track(self):
        with self.storage_lock:
            self.records_storage.set_slot(self._session)
            self.records_storage.incr_reference(self._session)

    def untrack(self, by_force=False):
        """
        退出上下文，记录引用数目，若已清空引用，则清理存储空间，并返回True通知关闭上下文

        :param by_force: 是否强制清空存储
        :return: None or True
        """
        with self.storage_lock:
            self.records_storage.decr_reference(self._session)
            if by_force or self.records_storage.none_reference(self._session):
                self.records_storage.delete_slot(self._session)
                return True

    def clear_records(self, target_session):
        with self.storage_lock:
            self.records_storage.clear_slot(target_session)

    def register_session_event(self):
        """
        注册SqlAlchemy事件监听，记录对应变更
        增: 抓取全量变更;
        更新: 抓取变更前快照和变更对比生成增量变更;
        删: 抓取删前快照;
        提交: 上下文管理器过程中使用者不能commit, 若监听到commit事件, 需要拦截和抛出错误

        :return: None
        """

        @event.listens_for(SessionProtoCls, "before_flush")
        def handle_before_flush(target_session, flush_context, instances):
            """
            listen for the 'before_flush' event

            update 和 delete 操作在变更发生前记录本地变更和数据库记录主键
            """
            if target_session in self.records_storage.get_slots_index():
                # update flush
                if target_session.dirty:
                    self.save_records_into_storage(target_session, "UPDATE")

                # delete flush
                if target_session.deleted:
                    self.save_records_into_storage(target_session, "DELETE")

        @event.listens_for(SessionProtoCls, "after_flush")
        def handle_after_flush(target_session, flush_context):
            """
            listen for the 'after_flush' event

            create 新增在将数据flush到数据库之后，获取数据库记录和主键
            """
            if target_session in self.records_storage.get_slots_index():
                # add flush
                if target_session.new:
                    self.save_records_into_storage(target_session, "CREATE")

        @event.listens_for(SessionProtoCls, "before_commit")
        def handle_before_commit(target_session):
            """
            listen for the 'before_commit' event

            上下文管理器环境中不允许提交，提交前检查提交触发时机
            """
            if target_session in self.records_storage.get_slots_index():
                self.meta_sync_instance.sync(target_session)
                self.clear_records(target_session)

        @event.listens_for(SessionProtoCls, "after_rollback")
        def handle_after_rollback(target_session):
            """
            listen for the 'after_rollback' event

            回滚session事件处理
            """
            target_session.new.clear()
            target_session.deleted.clear()
            target_session.dirty.clear()

    def save_records_into_storage(self, session, scene):
        """
        捕捉session Attribute 实例(new dirty deleted), 保存实例记录的变更

        :param session: 当前会话实例
        :param scene: 场景
        :return: None
        """
        session_attr_instance = []
        if scene == "CREATE":
            session_attr_instance = session.new
        elif scene == "UPDATE":
            session_attr_instance = session.dirty
        elif scene == "DELETE":
            session_attr_instance = session.deleted

        for record_obj in session_attr_instance:
            table_instance = inspect(record_obj).class_.__table__
            # 只处理注册了的models
            if table_instance.name not in client_for_meta_sync.managed_models:
                continue
            primary_column = [column for column in table_instance.primary_key.columns][0]
            primary_id = model_to_dict(record_obj)[primary_column.name]
            # 记录同步内容
            changed_data = None
            if scene == "CREATE":
                changed_data = model_to_dict(record_obj)
            else:
                results_tuple = session.query(table_instance).filter(primary_column == primary_id)
                snapshots = [dict(zip(result.keys(), result)) for result in results_tuple]
                if snapshots and scene == "UPDATE":
                    changed_data = {}
                    changed_diffs = diff(snapshots[0], model_to_dict(record_obj))
                    for changed_tuple in changed_diffs:
                        # TODO: 补充迭代的changed_key处理
                        changed_type, changed_key, changed_value = list(changed_tuple)
                        if changed_type == "change":
                            changed_data[changed_key] = list(changed_value)[1]
                if snapshots and scene == "DELETE":
                    changed_data = snapshots[0]
            if changed_data:
                opr_record = {
                    "change_time": time.strftime(str("%Y-%m-%d %H:%M:%S"), time.localtime()),
                    "changed_data": json.dumps(changed_data, default=str),
                    "primary_key_value": primary_id,
                    "db_name": session.bind.url.database,
                    "table_name": table_instance.name,
                    "method": scene,
                }
                with self.storage_lock:
                    self.records_storage.get_slot(session).append(opr_record)


"""
sync_hook支持的适配器列表
"""
supported_adapter = {"sqlalchemy": SqlAlchemySyncAdapter}
