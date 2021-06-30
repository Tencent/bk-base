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

from datetime import datetime

import attr

from metadata.type_system.basic_type import Entity, as_metadata
from metadata_biz.types.entities.management import BKBiz, ProjectInfo


@as_metadata
@attr.s
class Infrastructure(Entity):
    __abstract__ = True
    pass


@as_metadata
@attr.s
class DatabusChannelClusterConfig(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    cluster_name = attr.ib(type=str)
    cluster_type = attr.ib(type=str)
    cluster_role = attr.ib(type=str)
    cluster_domain = attr.ib(type=str)
    cluster_backup_ips = attr.ib(type=str)
    cluster_port = attr.ib(type=int)
    zk_domain = attr.ib(type=str)
    zk_port = attr.ib(type=int)
    zk_root_path = attr.ib(type=str)
    description = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    created_by = attr.ib(type=str)
    stream_to_id = attr.ib(type=int)
    storage_name = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    priority = attr.ib(type=int, default=0)
    attribute = attr.ib(type=str, default='bkdata')
    ip_list = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class StorageClusterConfig(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    cluster_name = attr.ib(type=str)
    cluster_type = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    cluster_group = attr.ib(type=str)
    priority = attr.ib(type=int)
    version = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    created_by = attr.ib(type=str)
    connection_info = attr.ib(type=str, default=None)
    expires = attr.ib(type=str, default=None)
    belongs_to = attr.ib(type=str, default='bkdata')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class DatabusConnectorClusterConfig(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True})
    cluster_name = attr.ib(type=str)
    cluster_bootstrap_servers = attr.ib(type=str)
    description = attr.ib(type=str)
    channel_name = attr.ib(type=str)
    cluster_type = attr.ib(type=str, default='kafka')
    cluster_rest_domain = attr.ib(type=str, default='')
    cluster_rest_port = attr.ib(type=int, default=8083)
    cluster_props = attr.ib(type=str, default=None)
    consumer_bootstrap_servers = attr.ib(type=str, default='')
    consumer_props = attr.ib(type=str, default=None)
    monitor_props = attr.ib(type=str, default=None)
    other_props = attr.ib(type=str, default=None)
    state = attr.ib(type=str, default='RUNNING')
    limit_per_day = attr.ib(type=int, default=1440000)
    priority = attr.ib(type=int, default=10)
    created_by = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    module = attr.ib(type=str, default='')
    component = attr.ib(type=str, default='')
    ip_list = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class DataflowJobnaviClusterConfig(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True})
    cluster_name = attr.ib(type=str)
    cluster_domain = attr.ib(type=str)
    version = attr.ib(type=str)
    description = attr.ib(type=str)
    geog_area_code = attr.ib(type=str)
    created_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ProcessingClusterConfig(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True})
    component_type = attr.ib(type=str)
    description = attr.ib(type=str)
    cluster_group = attr.ib(type=str)
    cluster_name = attr.ib(type=str)
    version = attr.ib(type=str)
    geog_area_code = attr.ib(type=str)
    cluster_label = attr.ib(type=str, default="standard")
    priority = attr.ib(type=int, default=1)
    cluster_domain = attr.ib(type=str, default='default')
    belong = attr.ib(type=str, default='bkdata')
    created_by = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ProcessingTypeConfig(Infrastructure):
    id = attr.ib(type=int)
    processing_type_name = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    processing_type_alias = attr.ib(type=str)
    description = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class ResourceGroupInfo(Infrastructure):
    resource_group_id = attr.ib(
        type=str,
        metadata={
            'identifier': True,
            'dgraph': {
                'index': [
                    'exact',
                ],
            },
        },
    )
    group_name = attr.ib(type=str)
    group_type = attr.ib(type=str)
    bk_biz_id = attr.ib(type=int)
    bk_biz = attr.ib(type=BKBiz)
    process_id = attr.ib(type=str)
    description = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    status = attr.ib(type=str, default='approve')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class TransferringTypeConfig(Infrastructure):
    id = attr.ib(type=int)
    transferring_type_name = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    transferring_type_alias = attr.ib(type=str)
    description = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class DatamapLayout(Infrastructure):
    id = attr.ib(type=int, metadata={'identifier': True})
    category = attr.ib(type=str)
    seq_index = attr.ib(type=int)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str, default=None)
    loc = attr.ib(type=bool, default=True)
    active = attr.ib(type=bool, default=True)
    config = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class TagTypeConfig(Infrastructure):
    # 标签类型配置，用于标签选择器
    id = attr.ib(type=int, metadata={'identifier': True})
    name = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    alias = attr.ib(type=str)
    seq_index = attr.ib(type=int)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
