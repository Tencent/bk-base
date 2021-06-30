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

from __future__ import absolute_import, print_function, unicode_literals

import functools
import json
from threading import Event

import attr

from metadata_client import sync
from metadata_client.backend import ApiBackendMixIn
from metadata_client.core import OperateRecord
from metadata_client.event.subscriber import MetaEventSubscriber
from metadata_client.exc import MetaDataClientError
from metadata_client.resource import global_settings, resource_lock
from metadata_client.util.common import Empty


def meta_sync(context_cls, context_type="instance", *context_args, **context_kwargs):
    """
    sync hook流程入口装饰器方法

    :param: session_cls obj 上下文会话或工厂方法
    :param: context_type str 上下文类型
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with sync.MetaSync(context_cls, context_type, *context_args, **context_kwargs):
                return func(*args, **kwargs)

        return wrapper

    return decorator


class MetadataClient(ApiBackendMixIn, object):
    """
    Metadata客户端
    所有对MetaData的操作都要通过实例化此类执行

    Attributes:
        settings: 全局配置
    """

    is_sync_enabled = False
    hooks_registered = {}
    managed_models = []
    orp_log_instance = None
    rpc_client_instance = None

    def __init__(self, settings):
        """
        :param settings: 适用于该实例的配置。默认配置从metadata_client.default_settings获取。
        """
        self.settings = settings
        """线上指定id同步方式"""
        self.settings.SYNC_CONTENT_TYPE = "id"
        if not global_settings.is_set:
            self.set_global_setting(self.settings)

    @staticmethod
    def set_global_setting(global_settings_to_set):
        """
        设置全局配置。客户端第一次执行前需要调用。

        :param global_settings_to_set: 配置字典
        :return: None
        """
        with resource_lock:
            if not global_settings.is_set:
                for k in dir(global_settings_to_set):
                    if not k.startswith("_") and not callable(getattr(global_settings_to_set, k)):
                        setattr(global_settings, k, getattr(global_settings_to_set, k))
                global_settings.is_set = True

    @classmethod
    def register_sync_model(cls, model_table_name):
        """
        注册meta_sync要监听的model列表

        :param model_table_name: str or list of str  model table name list
        :return:
        """
        if isinstance(model_table_name, list):
            cls.managed_models.extend(model_table_name)
        else:
            cls.managed_models.append(model_table_name)

    def enable_meta_sync_hook(self):
        """
        打开元数据sync_hook支持,只能打开一次，sync_hook实例只能绑定第一个metadata_client

        :return:
        """
        if not self.__class__.is_sync_enabled:
            with resource_lock:
                if not self.__class__.is_sync_enabled:
                    self.__class__.is_sync_enabled = True
                    sync.client_for_meta_sync = self

    def sync(self, sync_contents):
        """
        直接同步数据到meta服务后端

        :param sync_contents: 同步内容
        :return: rpc调用结果
        """
        return self.bridge_sync(sync_contents)

    def query(self, statement, backend_type="dgraph"):
        """
        使用GraphQL查询元数据。

        :param backend_type: 选择查询后端和集群，支持选项：dgraph和dgraph_backup
        :param statement: 查询语句
        :return: 结果
        """
        return self.entity_complex_search(statement, backend_type)

    def query_via_erp(self, statement, backend_type="dgraph"):
        """
        使用ERP查询元数据。

        :param backend_type: 选择查询后端和集群，支持选项：dgraph和dgraph_backup
        :param statement: 查询语句
        :return: 结果
        """
        return self.entity_query_via_erp(statement, backend_type=backend_type)

    @property
    def session(self):
        """
        元数据操作事务
        仅支持图存储，通过上下文管理器使用

        - 例子::

            client = MetadataClient()
            with client.session as se:
                se.create(md)
                se.commit(md)

        :return: 结果
        """
        return EditSession(self)

    def event_subscriber(self, subscribe_config=None):
        """
        元数据事件订阅器
        支持配置化订阅多个主题多种元数据事件并设置对应的回调处理函数
        """
        if self.is_sync_enabled:
            raise MetaDataClientError("subscriber can not work with sync_hook at the same time")
        supports_config = dict(
            supporter=self.settings.endpoints("event_support"),
            crypt_key=self.settings.CRYPT_INSTANCE_KEY,
        )
        return MetaEventSubscriber(subscribe_config, supports_config)


class EditSession(object):
    """
    **元数据操作事务。**
    """

    def __init__(self, client):
        self.operate_records = []
        self.client = client
        self.committed = Event()

    def __enter__(self):
        if self.committed.is_set():
            raise MetaDataClientError("The session has been committed.")
        return self

    def create(self, md_objs):
        """
        新建元数据。

        :param md_objs: 元数据单个实例或列表。
        :return: None
        """
        if not isinstance(md_objs, (list, tuple)):
            md_objs = [md_objs]
        self.operate_records.extend([self._generate_operate_record("CREATE", item) for item in md_objs])

    def update(self, md_objs):
        """
        更新元数据。

        :param md_objs: 元数据单个实例或列表。
        :return: None
        """
        if not isinstance(md_objs, (list, tuple)):
            md_objs = [md_objs]
        self.operate_records.extend([self._generate_operate_record("UPDATE", item) for item in md_objs])

    def delete(self, md_objs):
        """
        删除元数据。

        :param md_objs: 元数据单个实例或列表。
        :return: None
        """
        if not isinstance(md_objs, (list, tuple)):
            md_objs = [md_objs]
        self.operate_records.extend([self._generate_operate_record("DELETE", item) for item in md_objs])

    def commit(self):
        """
        提交此事务实例内的所有元数据变更。
        """
        if self.committed.is_set():
            raise MetaDataClientError("The session has been committed.")
        self.client.entity_edit(operate_records_lst=[attr.asdict(item) for item in self.operate_records])
        self.committed.set()

    @staticmethod
    def _generate_operate_record(method, md_obj):
        identifier_attr_name = md_obj.__class__.metadata["identifier"].name
        op_r = OperateRecord(
            method=method,
            type_name=md_obj.__class__.__name__,
            changed_data=json.dumps({k: v for k, v in attr.asdict(md_obj).items() if v is not Empty}),
            identifier_value=getattr(md_obj, identifier_attr_name),
        )
        return op_r

    def __exit__(self, exc_type, exc_val, exc_tb):
        return
