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
from typing import List

import attr
from attr.validators import in_

from metadata.type_system.basic_type import Asset, Entity, Relation, as_metadata
from metadata.type_system.core import StringDeclaredType
from metadata.util.common import Empty
from metadata_biz.types.entities.data_set import DataSet
from metadata_biz.types.entities.infrastructure import (
    DatabusChannelClusterConfig,
    ProcessingTypeConfig,
    StorageClusterConfig,
    TransferringTypeConfig,
)
from metadata_biz.types.entities.management import ProjectInfo


@as_metadata
@attr.s
class Procedure(Entity):
    __abstract__ = True


@as_metadata
@attr.s
class DataProcessing(Procedure):
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    processing_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    processing_alias = attr.ib(type=str)
    processing_type = attr.ib(type=str)
    processing_type_obj = attr.ib(type=ProcessingTypeConfig)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    description = attr.ib(type=str)
    platform = attr.ib(type=str, default='bkdata')
    generate_type = attr.ib(type=str, default='user')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    lineage_descend = attr.ib(
        type=List[StringDeclaredType.typed('Asset')],
        default=tuple([Empty]),
        metadata={'dgraph': {'predicate_name': 'lineage.descend'}},
    )


@as_metadata
@attr.s
class DataTransferring(Procedure):
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    transferring_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    transferring_alias = attr.ib(type=str)
    transferring_type = attr.ib(type=str)
    transferring_type_obj = attr.ib(type=TransferringTypeConfig)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    description = attr.ib(type=str)
    generate_type = attr.ib(type=str, default='user')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DataProcessingRelation(Relation):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    data_directing = attr.ib(type=str, validator=in_(('input', 'output')))
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    data_set = attr.ib(type=DataSet)
    storage_type = attr.ib(type=str)
    processing_id = attr.ib(type=str)
    processing = attr.ib(type=DataProcessing)
    storage_cluster_config_id = attr.ib(type=int, default=None)
    storage_cluster = attr.ib(type=StorageClusterConfig, default=None)
    channel_cluster_config_id = attr.ib(type=int, default=None)
    channel_cluster = attr.ib(type=DatabusChannelClusterConfig, default=None)


@as_metadata
@attr.s
class DataTransferringRelation(Relation):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    data_directing = attr.ib(type=str, validator=in_(('input', 'output')))
    data_set_type = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    data_set = attr.ib(type=DataSet)
    storage_type = attr.ib(type=str)
    transferring_id = attr.ib(type=str)
    transferring = attr.ib(type=DataTransferring)
    storage_cluster_config_id = attr.ib(type=int, default=None)
    storage_cluster = attr.ib(type=StorageClusterConfig, default=None)
    channel_cluster_config_id = attr.ib(type=int, default=None)
    channel_cluster = attr.ib(type=DatabusChannelClusterConfig, default=None)
