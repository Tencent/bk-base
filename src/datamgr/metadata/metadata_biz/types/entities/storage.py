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

from metadata.type_system.basic_type import Entity
from metadata.type_system.core import as_metadata
from metadata_biz.types.entities.data_set import ResultTable
from metadata_biz.types.entities.infrastructure import (
    DatabusChannelClusterConfig,
    StorageClusterConfig,
)


@as_metadata
@attr.s
class Storage(Entity):
    __abstract__ = True
    pass


@as_metadata
@attr.s
class StorageResultTable(Storage):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    result_table_id = attr.ib(type=str)
    result_table = attr.ib(type=ResultTable)
    physical_table_name = attr.ib(type=str)
    expires = attr.ib(type=str)
    storage_config = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    storage_cluster_config_id = attr.ib(type=int, default=None)
    storage_cluster = attr.ib(type=StorageClusterConfig, default=None)
    storage_channel_id = attr.ib(type=int, default=None)
    storage_channel = attr.ib(type=DatabusChannelClusterConfig, default=None)
    active = attr.ib(type=bool, default=True)
    priority = attr.ib(type=int, default=0)
    generate_type = attr.ib(type=str, default='user')
    data_type = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    previous_cluster_name = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
