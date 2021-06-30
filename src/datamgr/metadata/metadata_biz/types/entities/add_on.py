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
from attr.validators import in_

from metadata.type_system.basic_type import AddOn, Asset, as_metadata
from metadata.type_system.core import StringDeclaredType
from metadata_biz.types.entities.data_set import ResultTable
from metadata_biz.types.entities.management import ProjectInfo


@as_metadata
@attr.s
class BelongsToConfig(AddOn):
    belongs_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    belongs_name = attr.ib(type=str)
    belongs_alias = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class ClusterGroupConfig(AddOn):
    cluster_group_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    cluster_group_name = attr.ib(type=str)
    scope = attr.ib(
        type=str,
        validator=in_(('private', 'public')),
    )
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    cluster_group_alias = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class ContentLanguageConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    content_key = attr.ib(type=str)
    language = attr.ib(type=str)
    content_value = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class DMCategoryConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    category_name = attr.ib(type=str)
    category_alias = attr.ib(type=str)
    parent_id = attr.ib(type=int)
    parent = attr.ib(type=StringDeclaredType.typed(type_name='DMCategoryConfig'))
    seq_index = attr.ib(type=int)
    icon = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    visible = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class DMLayerConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    layer_name = attr.ib(type=str)
    layer_alias = attr.ib(type=str)
    parent_id = attr.ib(type=int)
    parent = attr.ib(type=StringDeclaredType.typed(type_name='DMLayerConfig'))
    seq_index = attr.ib(type=int)
    icon = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    visible = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class EncodingConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    encoding_name = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    encoding_alias = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class FieldTypeConfig(AddOn):
    field_type = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    field_type_name = attr.ib(type=str)
    field_type_alias = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class JobStatusConfig(AddOn):
    status_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    status_name = attr.ib(type=str)
    status_alias = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class PlatformConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    platform_name = attr.ib(type=str)
    platform_alias = attr.ib(type=str)
    description = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class ProjectClusterGroupConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    project_id = attr.ib(type=str)
    cluster_group_id = attr.ib(type=str)
    project = attr.ib(type=ProjectInfo)
    created_by = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class ResultTableField(AddOn):
    id = attr.ib(
        type=int,
        metadata={'identifier': True, 'event': ['on_create', 'on_delete'], 'dgraph': {'index': ['int']}},
    )
    result_table_id = attr.ib(type=str)
    result_table = attr.ib(type=ResultTable)
    field_index = attr.ib(type=int, metadata={'event': ['on_update']})
    field_name = attr.ib(type=str)
    field_alias = attr.ib(type=str, metadata={'event': ['on_update']})
    field_type = attr.ib(type=str)
    field_type_obj = attr.ib(type=FieldTypeConfig)
    is_dimension = attr.ib(type=bool)
    origins = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    description = attr.ib(type=str, default=None, metadata={'event': ['on_update']})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    roles = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class ResultTableTypeConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    result_table_type_name = attr.ib(type=str)
    result_table_type_alias = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class TimeFormatConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    time_format_name = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    time_format_alias = attr.ib(type=str, default=None)
    time_format_example = attr.ib(type=str, default=None)
    timestamp_len = attr.ib(type=int, default=0)
    format_unit = attr.ib(type=str, default='d')
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    description = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class DataSetFeature(AddOn):
    # 数据集特征（关联数据集）
    feature_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    feature_topic = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    target = attr.ib(type=Asset)
    feature_description = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DataSetIndex(AddOn):
    # 数据集特征指标
    index_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    index_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    index_value = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    index_description = attr.ib(type=str)
    data_set_feature_id = attr.ib(type=str)
    data_set_feature = attr.ib(type=DataSetFeature)


@as_metadata
@attr.s
class DataSetSummary(AddOn):
    # 数据集特征摘要
    summary_id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    summary_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    summary_cont = attr.ib(type=str)
    summary_description = attr.ib(type=str)
    data_set_feature_id = attr.ib(type=str)
    data_set_feature = attr.ib(type=DataSetFeature)
