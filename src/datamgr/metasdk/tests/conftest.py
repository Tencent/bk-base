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

import os
from datetime import datetime

import attr
import pytest
from sqlalchemy import (
    BOOLEAN,
    INTEGER,
    TIMESTAMP,
    Column,
    Enum,
    Integer,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from metadata_client import DEFAULT_SETTINGS, MetadataClient, meta_sync
from metadata_client.backend import ApiBackendMixIn
from metadata_client.event.subscriber import MetaEventSubscriber
from metadata_client.resource import global_settings, resource_lock
from metadata_client.support.rabbitmq import BackendClient, RabbitMQConsumer
from metadata_client.sync import MetaSync
from metadata_client.type_system_lite.core import LocalMetaData, as_local_metadata

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def metadata_client():
    settings = DEFAULT_SETTINGS.copy()
    settings.META_ACCESS_RPC_ENDPOINT = "http://localhost/jsonrpc/2.0/"
    # sync_hook客户端配置
    settings.META_ACCESS_RPC_HOST = "localhost"  # sync同步host
    settings.META_ACCESS_RPC_PORT = "80"  # sync同步port
    # ip同步模式需设置operation_log_db用于暂存操作记录
    settings.BKDATA_LOG_DB_URL = "sqlite:///:memory:"
    settings.CRYPT_INSTANCE_KEY = "test"

    # early_patch
    ApiBackendMixIn.bridge_sync = patch_bridge_sync
    ApiBackendMixIn.entity_complex_search = patch_entity_complex_search
    ApiBackendMixIn.entity_query_via_erp = patch_entity_query_via_erp
    ApiBackendMixIn.entity_edit = patch_entity_edit
    MetaSync.db_log_prepare = patch_db_log_prepare
    MetaEventSubscriber.start_to_listening = patch_start_to_listening
    BackendClient.get_backend_config = patch_get_backend_config

    metadata_client = MetadataClient(settings)
    return metadata_client


def patch_bridge_sync(obj, sync_contents, content_mode="id", batch=False, **kwargs):
    kwargs[str("db_operations_list")] = sync_contents
    kwargs[str("content_mode")] = content_mode
    kwargs[str("batch")] = batch
    print(kwargs)
    return kwargs


def patch_entity_complex_search(obj, statement, backend_type="mysql", **kwargs):
    kwargs[str("statement")] = statement
    kwargs[str("backend_type")] = backend_type
    print(kwargs)
    return kwargs


def patch_entity_query_via_erp(obj, retrieve_args, **kwargs):
    kwargs[str("retrieve_args")] = retrieve_args
    print(kwargs)
    return kwargs


def patch_entity_edit(obj, operate_records_lst, **kwargs):
    kwargs[str("operate_records_lst")] = operate_records_lst
    print(kwargs)
    return kwargs


def patch_db_log_prepare(obj, sync_contents):
    """
    预先将同步记录写入bkdata_log数据库, 返回存入数据的自增id, 以id模式进行同步

    :param obj
    :param sync_contents: 同步记录
    :return: list 同步记录的id列表
    """
    print(sync_contents)
    return [1, 2, 3, 4, 5]


def patch_start_to_listening(obj):
    set_orders = None
    if obj.subscribe_orders and not obj.__class__.subscriber_existed:
        with resource_lock:
            if not obj.__class__.subscriber_existed:
                obj.__class__.subscriber_existed = True
                # obj.register_subscriber()
                set_orders = obj.subscribe_orders
                print("subscriber registered!")
    print(obj.subscribe_orders)
    print("start scan!!!")
    return set_orders


def patch_get_backend_config(obj):
    return {"addr": "DP0puSkFrhifSjohAHhBmOD8Vw/FzJ3xR/+yQY9zXMHoLpQ+ljwUjltGSxu5ew=="}
