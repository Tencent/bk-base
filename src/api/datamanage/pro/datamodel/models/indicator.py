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
from django.db import models
from django.utils.translation import ugettext_lazy as _

from common.transaction import meta_sync_register

from datamanage.pro.datamodel.models.json_field import JsonField
from datamanage.pro.datamodel.models.model_dict import SchedulingType
from datamanage.pro.datamodel.models.base import OverrideHashModel


class DmmModelCalculationAtom(models.Model):
    # 数据模型统计口径表
    model_id = models.IntegerField(_('模型ID'))
    project_id = models.IntegerField(_('项目ID'))
    calculation_atom_name = models.CharField(_('统计口径名称'), max_length=255, primary_key=True)
    calculation_atom_alias = models.CharField(_('统计口径别名'), max_length=255)
    description = models.TextField(_('描述'), blank=True, null=True)
    field_type = models.CharField(_('数据类型'), max_length=32)
    calculation_content = JsonField(_('统计方式'))
    calculation_formula = models.TextField(_('统计SQL'))
    origin_fields = JsonField(_('计算来源字段'))
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_calculation_atom'
        managed = False
        app_label = 'datamodel'


class DmmModelCalculationAtomImage(OverrideHashModel):
    # 数据模型统计口径引用表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    project_id = models.IntegerField(_('项目ID'))
    calculation_atom_name = models.CharField(_('模型名称'), max_length=255)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_calculation_atom_image'
        unique_together = ('model_id', 'calculation_atom_name')
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        attr_names = ['model_id', 'calculation_atom_name']
        return hash('-'.join([str(getattr(self, attr_name)) for attr_name in attr_names]))


class DmmModelCalculationAtomImageStage(models.Model):
    # 数据模型统计口径引用草稿表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    project_id = models.IntegerField(_('项目ID'))
    calculation_atom_name = models.CharField(_('统计口径名称'), max_length=255)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_calculation_atom_image_stage'
        unique_together = ('model_id', 'calculation_atom_name')
        managed = False
        app_label = 'datamodel'


class DmmModelIndicator(OverrideHashModel):
    # 数据模型指标表
    model_id = models.IntegerField(_('模型ID'))
    project_id = models.IntegerField(_('项目ID'))
    indicator_name = models.CharField(_('指标名称'), max_length=255, primary_key=True)
    indicator_alias = models.CharField(_('指标别名'), max_length=255)
    description = models.TextField(_('描述'), blank=True, null=True)
    calculation_atom_name = models.CharField(_('统计口径名称'), max_length=255)
    aggregation_fields = JsonField(_('聚合字段列表'))
    filter_formula = models.TextField(_('过滤SQL'), blank=True, null=True)
    condition_fields = JsonField(_('过滤字段'), null=True)
    scheduling_type = models.CharField(_('调度类型'), max_length=32, choices=SchedulingType.get_enum_alias_map_list())
    scheduling_content = JsonField(_('调度内容'))
    parent_indicator_name = models.CharField(_('父指标名称'), max_length=255, null=True)
    hash = models.CharField(_('指标关键参数对应的hash值'), max_length=64, null=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_indicator'
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        attr_names = ['model_id', 'indicator_name']
        return hash('-'.join([str(getattr(self, attr_name)) for attr_name in attr_names]))


class DmmModelIndicatorStage(models.Model):
    # 数据模型指标草稿态临时表
    indicator_id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    project_id = models.IntegerField(_('项目ID'))
    indicator_name = models.CharField(_('指标名称'), max_length=255)
    indicator_alias = models.CharField(_('指标别名'), max_length=255)
    description = models.TextField(_('描述'), blank=True, null=True)
    calculation_atom_name = models.CharField(_('统计口径名称'), max_length=255)
    aggregation_fields = JsonField(_('聚合字段列表'))
    filter_formula = models.TextField(_('过滤SQL'), blank=True, null=True)
    condition_fields = JsonField(_('过滤字段'), null=True)
    scheduling_type = models.CharField(_('调度类型'), max_length=32, choices=SchedulingType.get_enum_alias_map_list())
    scheduling_content = JsonField(_('调度内容'))
    parent_indicator_name = models.CharField(_('父指标名称'), max_length=255, null=True)
    hash = models.CharField(_('指标关键参数对应的hash值'), max_length=64, null=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_indicator_stage'
        unique_together = (('model_id', 'indicator_name'),)
        managed = False
        app_label = 'datamodel'


class DmmCalculationFunctionConfig(models.Model):
    # SQL 统计函数表
    function_name = models.CharField(_('SQL 统计函数名称'), primary_key=True, max_length=64)
    output_type = models.CharField(_('输出字段类型'), max_length=32)
    allow_field_type = JsonField(_('允许被聚合字段的数据类型'))

    class Meta:
        db_table = 'dmm_calculation_function_config'
        managed = False
        app_label = 'datamodel'


meta_sync_register(DmmModelCalculationAtom)
meta_sync_register(DmmModelCalculationAtomImage)
meta_sync_register(DmmModelIndicator)
meta_sync_register(DmmCalculationFunctionConfig)
