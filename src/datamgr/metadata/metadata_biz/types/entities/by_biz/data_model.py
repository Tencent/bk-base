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

import sys
from datetime import datetime

import attr

from metadata.type_system.basic_type import AddOn, Entity, Relation
from metadata.type_system.core import as_metadata
from metadata_biz.types.entities.add_on import FieldTypeConfig
from metadata_biz.types.entities.data_set import DataSet, ResultTable
from metadata_biz.types.entities.management import ProjectInfo

python_string_type = str
default_json = '{}'


@as_metadata
@attr.s
class DmmModelInfo(Entity):
    model_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    model_alias = attr.ib(type=python_string_type)
    model_type = attr.ib(type=python_string_type)
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    table_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    table_alias = attr.ib(type=python_string_type)
    latest_version_id = attr.ib(type=python_string_type)
    publish_status = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    active_status = attr.ib(type=python_string_type, default='active', metadata={'dgraph': {'index': ['exact']}})
    step_id = attr.ib(type=int, default=0)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelField(AddOn):
    """
    数据模型字段
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    field_name = attr.ib(type=python_string_type)
    field_alias = attr.ib(type=python_string_type)
    field_type = attr.ib(type=python_string_type)
    field_type_obj = attr.ib(type=FieldTypeConfig)
    field_category = attr.ib(type=python_string_type)
    is_primary_key = attr.ib(type=bool)
    description = attr.ib(type=python_string_type)
    field_constraint_content = attr.ib(type=python_string_type)
    field_clean_content = attr.ib(type=python_string_type)
    origin_fields = attr.ib(type=python_string_type)
    field_index = attr.ib(type=int)
    source_model_id = attr.ib(type=int)
    source_model = attr.ib(type=DmmModelInfo)
    source_field_name = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelTop(DataSet):
    """
    数据模型用户置顶表
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmFieldConstraintConfig(AddOn):
    """
    字段约束表
    """

    constraint_id = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    constraint_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    constraint_name = attr.ib(type=python_string_type)
    constraint_value = attr.ib(type=python_string_type)
    validator = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    editable = attr.ib(type=bool)
    allow_field_type = attr.ib(type=python_string_type)


@as_metadata
@attr.s
class DmmModelRelation(Relation):
    """
    数据模型关系表
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model = attr.ib(type=DmmModelInfo)
    field_name = attr.ib(type=python_string_type)
    related_model_id = attr.ib(type=int)
    related_model = attr.ib(type=DmmModelInfo)
    related_field_name = attr.ib(type=python_string_type)
    related_model_version_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    related_method = attr.ib(type=python_string_type, default='left-join')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelRelease(Entity):
    """
    数据模型发布记录
    """

    __tablename__ = 'dmm_model_release'

    version_id = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    version_log = attr.ib(type=python_string_type)
    model_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model = attr.ib(type=DmmModelInfo)
    model_content = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelCalculationAtom(Entity):
    """
    数据模型统计口径表
    """

    calculation_atom_name = attr.ib(
        type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}}
    )
    calculation_atom_alias = attr.ib(type=python_string_type)
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    description = attr.ib(type=python_string_type)
    field_type = attr.ib(type=python_string_type)
    field_type_obj = attr.ib(type=FieldTypeConfig)
    calculation_content = attr.ib(type=python_string_type)
    calculation_formula = attr.ib(type=python_string_type)
    origin_fields = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelCalculationAtomImage(Relation):
    """
    数据模型统计口径引用表
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    project_id = attr.ib(type=int)
    calculation_atom_name = attr.ib(type=python_string_type)
    calculation_atom = attr.ib(type=DmmModelCalculationAtom)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelIndicator(Entity):
    """
    数据模型指标表
    """

    indicator_name = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    indicator_alias = attr.ib(type=python_string_type)
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    description = attr.ib(type=python_string_type)
    calculation_atom_name = attr.ib(type=python_string_type)
    calculation_atom = attr.ib(type=DmmModelCalculationAtom)
    aggregation_fields = attr.ib(type=python_string_type)
    filter_formula = attr.ib(type=python_string_type)
    condition_fields = attr.ib(type=python_string_type)
    scheduling_type = attr.ib(type=python_string_type)
    scheduling_content = attr.ib(type=python_string_type)
    parent_indicator_name = attr.ib(type=python_string_type)
    hash = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmCalculationFunctionConfig(AddOn):
    """
    SQL 统计函数表
    """

    function_name = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    output_type = attr.ib(type=python_string_type)
    allow_field_type = attr.ib(type=python_string_type)


@as_metadata
@attr.s
class DmmModelInstance(Entity):
    """
    数据模型实例
    在数据开发阶段，基于已构建的数据模型创建的任务，将作为该数据模型应用阶段的实例，每个模型应用实例会包含一个主表和多个指标
    """

    # 实例配置和属性
    instance_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    model_id = attr.ib(type=int)
    model = attr.ib(type=DmmModelInfo)
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    version_id = attr.ib(type=python_string_type)
    # 任务相关配置
    flow_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelInstanceTable(DataSet):
    """
    数据模型实例主表
    即明细数据表
    """

    # 主表配置及属性
    result_table_id = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    result_table = attr.ib(type=ResultTable)
    bk_biz_id = attr.ib(type=int)
    instance_id = attr.ib(type=int)
    instance = attr.ib(type=DmmModelInstance)
    model_id = attr.ib(type=int)
    flow_node_id = attr.ib(type=int)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelInstanceField(AddOn):
    """
    数据模型实例主表字段
    主要记录了主表输出哪些字段及字段应用阶段的清洗规则
    """

    # 字段基本属性
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    instance_id = attr.ib(type=int)
    instance = attr.ib(type=DmmModelInstance)
    model_id = attr.ib(type=int)
    field_name = attr.ib(type=python_string_type)
    # 字段来源信息
    input_result_table_id = attr.ib(type=python_string_type)
    input_field_name = attr.ib(type=python_string_type)
    application_clean_content = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelInstanceRelation(Relation):
    """
    数据模型实例字段映射关联关系
    对于需要进行维度关联的字段，需要在关联关系表中记录维度关联的相关相信
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    instance_id = attr.ib(type=int)
    instance = attr.ib(type=DmmModelInstance)
    model_id = attr.ib(type=int)
    field_name = attr.ib(type=python_string_type)
    # 关联字段来源及关联信息
    input_result_table_id = attr.ib(type=python_string_type)
    input_field_name = attr.ib(type=python_string_type)
    related_model_id = attr.ib(type=int)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelInstanceIndicator(DataSet):
    """
    数据模型实例指标
    """

    # 应用实例指标基本配置和属性
    result_table_id = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    result_table = attr.ib(type=ResultTable)
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    bk_biz_id = attr.ib(type=int)
    instance_id = attr.ib(type=int)
    instance = attr.ib(type=DmmModelInstance)
    model_id = attr.ib(type=int)
    # 关联结果表和节点的信息
    parent_result_table_id = attr.ib(type=python_string_type)
    flow_node_id = attr.ib(type=int)
    # 来自模型定义的指标继承写入，可以重载
    calculation_atom_name = attr.ib(type=python_string_type)
    calculation_atom = attr.ib(type=DmmModelCalculationAtom)
    aggregation_fields = attr.ib(type=python_string_type)
    filter_formula = attr.ib(type=python_string_type)
    scheduling_type = attr.ib(type=python_string_type)
    scheduling_content = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DmmModelInstanceSource(Entity):
    """
    模型实例输入表
    """

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    instance_id = attr.ib(type=int)
    instance = attr.ib(type=DmmModelInstance)
    input_type = attr.ib(type=python_string_type)
    input_result_table_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
