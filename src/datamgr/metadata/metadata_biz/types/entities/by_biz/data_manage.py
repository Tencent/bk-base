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

from metadata.type_system.basic_type import Asset, as_metadata
from metadata_biz.types.entities.add_on import AddOn
from metadata_biz.types.entities.data_set import DataSet, ResultTable
from metadata_biz.types.entities.management import BKBiz


@as_metadata
@attr.s
class DmStandardConfig(DataSet):
    id = attr.ib(type=int, metadata={'identifier': True})
    standard_name = attr.ib(type=str)
    category_id = attr.ib(type=int)
    created_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmStandardVersionConfig(AddOn):
    """
    TODO：DM元数据的继承关系还得优化。
    """

    id = attr.ib(type=int, metadata={'identifier': True})
    standard_id = attr.ib(type=int)
    standard = attr.ib(type=DmStandardConfig)
    standard_version = attr.ib(type=str)
    standard_version_status = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmStandardContentConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    standard_version_id = attr.ib(type=int)
    standard_version = attr.ib(type=DmStandardVersionConfig)
    standard_content_name = attr.ib(type=str)
    parent_id = attr.ib(type=str)
    source_record_id = attr.ib(type=int)
    created_by = attr.ib(type=str)
    category_id = attr.ib(type=int)
    standard_content_type = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    standard_content_sql = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    window_period = attr.ib(type=str, default=None)
    filter_cond = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmDetaildataFieldConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    standard_content_id = attr.ib(type=int)
    standard_content = attr.ib(type=DmStandardContentConfig)
    source_record_id = attr.ib(type=int)
    field_name = attr.ib(type=str)
    created_by = attr.ib(type=str)
    field_type = attr.ib(type=str)
    field_index = attr.ib(type=int)
    active = attr.ib(type=bool, default=True)
    field_alias = attr.ib(type=str, default=None)
    unit = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    constraint_id = attr.ib(type=int, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmIndicatorFieldConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    standard_content_id = attr.ib(type=int)
    standard_content = attr.ib(type=DmStandardContentConfig)
    source_record_id = attr.ib(type=int)
    field_name = attr.ib(type=str)
    field_type = attr.ib(type=str)
    field_index = attr.ib(type=int)
    compute_model_id = attr.ib(type=int)
    created_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    description = attr.ib(type=str, default=None)
    constraint_id = attr.ib(type=int, default=None)
    add_type = attr.ib(type=str, default=None)
    unit = attr.ib(type=str, default=None)
    is_dimension = attr.ib(type=bool, default=True)
    field_alias = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmConstraintConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    constraint_name = attr.ib(type=str)
    rule = attr.ib(type=str)
    created_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmComputeModelConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    field_type = attr.ib(type=str)
    data_type_group = attr.ib(type=str)
    compute_model = attr.ib(type=str)
    created_by = attr.ib(type=str)
    description = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmComparisonOperatorConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    comparison_operator_name = attr.ib(type=str)
    comparison_operator_alias = attr.ib(type=str)
    data_type_group = attr.ib(type=str)
    created_by = attr.ib(type=str)
    description = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmTaskConfig(AddOn):
    __tablename__ = 'dm_task_config'
    id = attr.ib(type=int, metadata={'identifier': True})
    task_name = attr.ib(type=str)
    project_id = attr.ib(type=int)
    standard_version_id = attr.ib(type=int)
    standard_version = attr.ib(type=DmStandardVersionConfig)
    data_set_type = attr.ib(type=str)
    standardization_type = attr.ib(type=int)
    task_status = attr.ib(type=str)
    edit_status = attr.ib(type=str)
    created_by = attr.ib(type=str)
    flow_id = attr.ib(type=int, default=None)
    data_set_id = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmTaskContentConfig(AddOn):
    __tablename__ = 'dm_task_content_config'
    id = attr.ib(type=int, metadata={'identifier': True})
    task_id = attr.ib(type=int)
    task = attr.ib(type=DmTaskConfig)
    parent_id = attr.ib(type=str)
    standard_version_id = attr.ib(type=int)
    standard_version = attr.ib(type=DmStandardVersionConfig)
    standard_content = attr.ib(type=DmStandardContentConfig)
    source_type = attr.ib(type=str)
    result_table_id = attr.ib(type=str)
    result_table = attr.ib(type=ResultTable)
    result_table_name = attr.ib(type=str)
    task_content_name = attr.ib(type=str)
    task_type = attr.ib(type=str)
    created_by = attr.ib(type=str)
    standard_content_id = attr.ib(type=int, default=0)
    task_content_sql = attr.ib(type=str, default=None)
    node_config = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    request_body = attr.ib(type=str, default=None)
    flow_body = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmTaskDetail(AddOn):
    __tablename__ = 'dm_task_detail'
    id = attr.ib(type=int, metadata={'identifier': True})
    task_id = attr.ib(type=int)
    task = attr.ib(type=DmTaskConfig)
    task_content_id = attr.ib(type=int)
    task_content = attr.ib(type=DmTaskContentConfig)
    standard_version_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    standard_version = attr.ib(type=DmStandardVersionConfig)
    standard_content = attr.ib(type=DmStandardContentConfig)
    data_set_type = attr.ib(type=str)
    task_type = attr.ib(type=str)
    created_by = attr.ib(type=str)
    data_set_id = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    data_set = attr.ib(type=ResultTable)
    standard_content_id = attr.ib(type=int, default=0, metadata={'dgraph': {'index': ['int']}})
    active = attr.ib(type=bool, default=True)
    bk_biz_id = attr.ib(type=int, default=None, metadata={'dgraph': {'index': ['int']}})
    bk_biz = attr.ib(type=BKBiz, default=None)
    project_id = attr.ib(type=int, default=None, metadata={'dgraph': {'index': ['int']}})
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmTaskDetaildataFieldConfig(AddOn):
    __tablename__ = 'dm_task_detaildata_field_config'
    id = attr.ib(type=int, metadata={'identifier': True})
    task_content_id = attr.ib(type=int)
    task_content = attr.ib(type=DmTaskContentConfig)
    field_name = attr.ib(type=str)
    field_type = attr.ib(type=str)
    field_index = attr.ib(type=int)
    compute_model = attr.ib(type=str)
    active = attr.ib(type=bool)
    created_by = attr.ib(type=str)
    unit = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    field_alias = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmTaskIndicatorFieldConfig(AddOn):
    __tablename__ = 'dm_task_indicator_field_config'
    id = attr.ib(type=int, metadata={'identifier': True})
    task_content_id = attr.ib(type=int)
    task_content = attr.ib(type=DmTaskContentConfig)
    field_name = attr.ib(type=str)
    field_type = attr.ib(type=str)
    add_type = attr.ib(type=str)
    field_index = attr.ib(type=int)
    compute_model = attr.ib(type=str)
    created_by = attr.ib(type=str)
    unit = attr.ib(type=str, default=None)
    is_dimension = attr.ib(type=int, default=0)
    field_alias = attr.ib(type=str, default=None)
    description = attr.ib(type=str, default=None)
    updated_by = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmUnitConfig(DataSet):
    __tablename__ = 'dm_unit_config'
    id = attr.ib(type=int, metadata={'identifier': True})
    name = attr.ib(type=str)
    alias = attr.ib(type=str)
    category_name = attr.ib(type=str)
    category_alias = attr.ib(type=str)
    created_by = attr.ib(type=str)
    description = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_by = attr.ib(type=str, default=None)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class Range(AddOn):
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    project_count = attr.ib(type=int)
    biz_count = attr.ib(type=int)
    depth = attr.ib(type=int)
    node_count_list = attr.ib(type=str)
    node_count = attr.ib(type=int)
    weighted_node_count = attr.ib(type=float)
    app_code_count = attr.ib(type=int)
    range_score = attr.ib(type=float)
    range_score_ranking = attr.ib(type=float)
    normalized_range_score = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})


@as_metadata
@attr.s
class Heat(AddOn):
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    query_count = attr.ib(type=int)
    heat_score = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    heat_score_ranking = attr.ib(type=float)
    queue_service_count = attr.ib(type=int, default=0)
    day_query_count = attr.ib(type=int, default=0)
    day_query_count_dict = attr.ib(type=str, default='{}')
    app_code_dict = attr.ib(type=str, default='{}')


@as_metadata
@attr.s
class Importance(AddOn):
    # 重要度得分及指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    dataset_score = attr.ib(type=float)
    biz_score = attr.ib(type=float)
    project_score = attr.ib(type=float)
    oper_state_name = attr.ib(type=str)
    oper_state = attr.ib(type=float)
    bip_grade_name = attr.ib(type=str)
    bip_grade_id = attr.ib(type=float)
    app_important_level_name = attr.ib(type=str)
    app_important_level = attr.ib(type=float)
    importance_score = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    importance_score_ranking = attr.ib(type=float)
    is_bip = attr.ib(type=bool, default=True)
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class AssetValue(AddOn):
    # 价值得分及指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    target = attr.ib(type=Asset)
    range_id = attr.ib(type=str)
    range = attr.ib(type=Range)
    normalized_range_score = attr.ib(type=float)
    heat_id = attr.ib(type=str)
    heat = attr.ib(type=Heat)
    heat_score = attr.ib(type=float)
    importance_id = attr.ib(type=str)
    importance = attr.ib(type=Importance)
    importance_score = attr.ib(type=float)
    asset_value_score = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    asset_value_score_ranking = attr.ib(type=float)


@as_metadata
@attr.s
class StorageCapacity(AddOn):
    # 存储成本
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    hdfs_capacity = attr.ib(type=float)
    tspider_capacity = attr.ib(type=float)
    total_capacity = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    log_capacity = attr.ib(type=float)
    capacity_score = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})


@as_metadata
@attr.s
class Cost(AddOn):
    # 成本
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    capacity_id = attr.ib(type=str)
    capacity = attr.ib(type=StorageCapacity)
    capacity_score = attr.ib(type=float)


@as_metadata
@attr.s
class LifeCycle(AddOn):
    # 生命周期指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    target = attr.ib(type=Asset)
    range_id = attr.ib(type=str)
    range = attr.ib(type=Range)
    heat_id = attr.ib(type=str)
    heat = attr.ib(type=Heat)
    importance_id = attr.ib(type=str)
    importance = attr.ib(type=Importance)
    asset_value_id = attr.ib(type=str)
    asset_value = attr.ib(type=AssetValue)
    cost_id = attr.ib(type=str)
    cost = attr.ib(type=Cost)
    assetvalue_to_cost = attr.ib(type=float, metadata={'dgraph': {'index': ['float']}})
    assetvalue_to_cost_ranking = attr.ib(type=float)
