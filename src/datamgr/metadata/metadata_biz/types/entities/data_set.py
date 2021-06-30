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

from metadata.type_system.basic_type import Asset, Entity, as_metadata
from metadata.type_system.core import StringDeclaredType
from metadata.util.common import Empty
from metadata_biz.types.entities.infrastructure import (
    DatabusChannelClusterConfig,
    ProcessingTypeConfig,
)
from metadata_biz.types.entities.management import BKBiz, ProjectInfo


@as_metadata
@attr.s
class DataSet(Entity):
    __abstract__ = True


@as_metadata
@attr.s
class AccessRawData(DataSet):
    id = attr.ib(type=int, metadata={'identifier': True, 'event': ['on_delete'], 'dgraph': {'index': ['int']}})
    bk_biz_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    bk_biz = attr.ib(type=BKBiz)
    raw_data_name = attr.ib(
        type=str,
        metadata={
            'dgraph': {
                'index': [
                    'exact',
                    'trigram',
                ],
            }
        },
    )
    raw_data_alias = attr.ib(type=str, metadata={'dgraph': {'index': ['fulltext', 'trigram']}, 'event': ['on_update']})
    data_source = attr.ib(type=str)
    data_scenario = attr.ib(type=str)
    bk_app_code = attr.ib(type=str)
    storage_partitions = attr.ib(type=int)
    description = attr.ib(type=str, metadata={'dgraph': {'index': ['fulltext', 'trigram']}, 'event': ['on_update']})
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    storage_channel_id = attr.ib(type=int)
    storage_channel = attr.ib(type=DatabusChannelClusterConfig)
    topic_name = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    data_encoding = attr.ib(type=str, default='UTF8')
    data_category = attr.ib(type=str, default=None)
    sensitivity = attr.ib(type=str, default='public', metadata={'event': ['on_update']})
    maintainer = attr.ib(type=str, default=None, metadata={'event': ['on_update']})
    active = attr.ib(type=bool, default=True, metadata={'event': ['on_update']})
    permission = attr.ib(type=str, default='all', metadata={'event': ['on_update']})
    lineage_descend = attr.ib(
        type=List[StringDeclaredType.typed('Asset')],
        default=tuple([Empty]),
        metadata={'dgraph': {'predicate_name': 'lineage.descend', 'reverse': True}},
    )


@as_metadata
@attr.s
class ResultTable(DataSet):
    bk_biz_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    bk_biz = attr.ib(type=BKBiz)
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(type=ProjectInfo)
    result_table_id = attr.ib(
        type=str,
        metadata={'identifier': True, 'event': ['on_delete'], 'dgraph': {'index': ['exact', 'trigram']}},
    )
    result_table_name = attr.ib(type=str)
    result_table_name_alias = attr.ib(
        type=str, metadata={'dgraph': {'index': ['fulltext', 'trigram']}, 'event': ['on_update']}
    )
    processing_type = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    processing_type_obj = attr.ib(type=ProcessingTypeConfig)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    description = attr.ib(type=str, metadata={'dgraph': {'index': ['fulltext', 'trigram']}, 'event': ['on_update']})
    result_table_type = attr.ib(type=str, default=None)
    generate_type = attr.ib(type=str, default='user', metadata={'dgraph': {'index': ['exact']}})
    sensitivity = attr.ib(type=str, default='public', metadata={'dgraph': {'index': ['exact']}, 'event': ['on_update']})
    count_freq = attr.ib(type=int, default=0)
    count_freq_unit = attr.ib(type=str, default='s')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    is_managed = attr.ib(type=bool, default=True)
    platform = attr.ib(type=str, default='bkdata')
    data_category = attr.ib(type=str, default='')
    lineage_descend = attr.ib(
        type=List[StringDeclaredType.typed('Asset')],
        default=tuple([Empty]),
        metadata={
            'dgraph': {
                'predicate_name': 'lineage.descend',
            }
        },
    )
