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


import json

from django.db import models
from django.utils.translation import ugettext_lazy as _

from datamanage.pro.dstan.utils import JsonField
from common.transaction import meta_sync_register
from common.meta.models import MetaSyncSupport
from common.base_utils import model_to_dict


class DmStandardConfig(models.Model):
    # 数据标准总表
    id = models.AutoField(_('标准id'), primary_key=True)
    standard_name = models.CharField(_('标准名称'), max_length=128)
    description = models.TextField(_('标准描述'), blank=True, null=True)
    category_id = models.IntegerField(_('所属分类'))
    active = models.BooleanField(_('是否有效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    def get_latest_online_version(self):
        """
        获取最新的版本ID
        """
        latest_version = DmStandardVersionConfig.objects.filter(
            standard_id=self.id, standard_version_status='online'
        ).order_by('-id')[0]
        return latest_version

    class Meta:
        db_table = 'dm_standard_config'
        managed = False
        app_label = 'dstan'


class DmStandardVersionConfig(models.Model):
    # 数据标准版本表
    id = models.AutoField(_('版本id'), primary_key=True)
    standard_id = models.IntegerField(_('关联的dm_standard_config表id'))
    standard_version = models.CharField(_('标准版本号,例子:v1.0,v2.0...'), max_length=128)
    description = models.TextField(_('版本描述'), blank=True, null=True)
    # 版本状态:developing/online/offline
    standard_version_status = models.CharField(_('版本状态'), max_length=32)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'dm_standard_version_config'
        managed = False
        app_label = 'dstan'


class DmStandardContentConfig(MetaSyncSupport):
    # 数据标准内容表
    id = models.AutoField(_('标准内容id'), primary_key=True)
    standard_version_id = models.IntegerField(_('关联的dm_standard_version_config表id'))
    standard_content_name = models.CharField(_('标准内容名称'), max_length=128)
    # parent_id = models.CharField(u'父表id,格式例子:[1,2]', max_length=256)
    parent_id = JsonField(_('约束条件,格式例子:[1,2]'))
    source_record_id = models.IntegerField(_('来源记录id'))
    standard_content_sql = models.TextField(_('标准模板sql'), blank=True, null=True)
    category_id = models.IntegerField(_('所属分类'))
    standard_content_type = models.CharField(_('标准内容类型[detaildata/indicator]'), max_length=128)
    description = models.TextField(_('标准内容描述'), blank=True, null=True)
    # window_period = models.TextField(u'窗口类型,以秒为单位,json表达定义', blank=True, null=True)
    window_period = JsonField(_('窗口类型,以秒为单位,json表达定义'))
    filter_cond = models.TextField(_('过滤条件'), blank=True, null=True)
    active = models.BooleanField(_('是否有效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'dm_standard_content_config'
        managed = False
        app_label = 'dstan'

    def capture_values(self):
        val_dict = model_to_dict(self)
        parent_id = val_dict.get('parent_id')
        window_period = val_dict.get('window_period')
        if parent_id is not None:
            val_dict['parent_id'] = json.dumps(parent_id)
        if window_period is not None:
            val_dict['window_period'] = json.dumps(window_period)
        return val_dict


class DmDetaildataFieldConfig(models.Model):
    # 明细数据标准字段详情表
    id = models.AutoField(primary_key=True)
    standard_content_id = models.IntegerField(_('关联的dm_standard_content_config的id'))
    source_record_id = models.IntegerField(_('来源记录id'))
    field_name = models.CharField(_('字段英文名'), max_length=128)
    field_alias = models.CharField(_('字段英文名'), max_length=128, blank=True, null=True)
    field_type = models.CharField(_('数据类型'), max_length=128)
    field_index = models.IntegerField(_('来源记录id'))
    unit = models.CharField(_('数据类型'), max_length=128, blank=True, null=True)
    description = models.TextField(_('备注'), blank=True, null=True)
    constraint_id = models.IntegerField(_('字段在数据集中的顺序'), blank=True, null=True)
    active = models.BooleanField(_('是否有效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'dm_detaildata_field_config'
        managed = False
        app_label = 'dstan'


class DmIndicatorFieldConfig(models.Model):
    # 原子指标字段详情表
    id = models.AutoField(primary_key=True)
    standard_content_id = models.IntegerField(_('关联的dm_standard_content_config的id'))
    source_record_id = models.IntegerField(_('来源记录id'))
    field_name = models.CharField(_('字段英文名'), max_length=128)
    field_alias = models.CharField(_('字段英文名'), max_length=128, blank=True, null=True)
    field_type = models.CharField(_('数据类型'), max_length=128)
    field_index = models.IntegerField(_('来源记录id'))
    unit = models.CharField(_('数据类型'), max_length=128, blank=True, null=True)
    is_dimension = models.BooleanField(_('是否维度:0:否;1:是;'), default=True)
    # 可加性:yes完全可加;half:部分可加;no:不可加;
    add_type = models.CharField(_('可加性'), max_length=128, blank=True, null=True)
    compute_model_id = models.IntegerField(_('关联的值约束配置表id'))
    constraint_id = models.IntegerField(_('计算方式id'), blank=True, null=True)
    description = models.TextField(_('备注'), blank=True, null=True)
    active = models.BooleanField(_('是否有效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'dm_indicator_field_config'
        managed = False
        app_label = 'dstan'


class DmConstraintConfig(MetaSyncSupport):
    # 支持的值约束配置表
    id = models.AutoField(primary_key=True)
    constraint_name = models.CharField(_('约束名称'), max_length=128)
    rule = JsonField(_('约束条件'))
    active = models.BooleanField(_('是否有效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('创建者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('创建时间'), auto_now_add=True, blank=True, null=True)

    class Meta:
        db_table = 'dm_constraint_config'
        managed = False
        app_label = 'dstan'

    def capture_values(self):
        val_dict = model_to_dict(self)
        rule = val_dict.get('rule')
        if rule is not None:
            val_dict['rule'] = json.dumps(rule)
        return val_dict


class DmDataTypeConfig(models.Model):
    # 数据类型信息表
    id = models.AutoField(primary_key=True)
    data_type_name = models.CharField(_('数据类型'), max_length=128)
    data_type_alias = models.CharField(_('数据类型名称'), max_length=128, default='')
    # 类型: numeric-数值型, string-字符型, time-时间型
    data_type_group = models.CharField(_('类型'), max_length=128, default='')
    description = models.TextField(_('标准描述'), blank=True, null=True)
    created_by = models.CharField('created by', max_length=50)
    created_at = models.DateTimeField('create time', auto_now_add=True)
    updated_by = models.CharField('updated by', max_length=50)
    updated_at = models.DateTimeField('update time', auto_now_add=True)

    class Meta:
        db_table = 'dm_data_type_config'
        managed = False
        app_label = 'dstan'


class DmUnitConfig(models.Model):
    # 度量单位信息表
    id = models.AutoField(_('标准id'), primary_key=True)
    name = models.CharField(_('单位英文名'), max_length=64)
    alias = models.CharField(_('单位中文名'), max_length=64)
    category_name = models.CharField(_('单位类目英文名'), max_length=64)
    category_alias = models.CharField(_('单位类目中文名'), max_length=64)
    description = models.TextField(_('描述'), blank=True, null=True)
    created_by = models.CharField('created by', max_length=50)
    created_at = models.DateTimeField('create time', auto_now_add=True)
    updated_by = models.CharField('updated by', max_length=50)
    updated_at = models.DateTimeField('update time', auto_now_add=True)

    class Meta:
        db_table = 'dm_unit_config'
        managed = False
        app_label = 'dstan'


class DmTaskDetailV1(MetaSyncSupport):
    id = models.AutoField(primary_key=True)
    task_id = models.IntegerField()
    task_content_id = models.IntegerField()
    standard_version_id = models.IntegerField()
    bk_biz_id = models.IntegerField()
    project_id = models.IntegerField()
    data_set_type = models.CharField(max_length=128)
    data_set_id = models.CharField(max_length=128)
    task_type = models.CharField(max_length=128)
    active = models.IntegerField(default=1)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'dm_task_detail'
        app_label = 'dstan'


meta_sync_register(DmStandardConfig)
meta_sync_register(DmStandardVersionConfig)
meta_sync_register(DmStandardContentConfig)
meta_sync_register(DmDetaildataFieldConfig)
meta_sync_register(DmIndicatorFieldConfig)
meta_sync_register(DmConstraintConfig)
meta_sync_register(DmTaskDetailV1)
