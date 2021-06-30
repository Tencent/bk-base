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

from datamanage.pro.datamodel.models.model_dict import (
    DataModelPublishStatus,
    DataModelStep,
    DataModelType,
    FieldCategory,
    DataModelActiveStatus,
    ConstraintType,
    RelatedMethod,
    DataModelObjectType,
    DataModelObjectOperationType,
    DATA_MODEL_UNIQUE_KEY_MAPPINGS,
)
from datamanage.pro.datamodel.models.json_field import JsonField
from datamanage.pro.datamodel.models.base import OverrideHashModel


class BaseManager(models.Manager):
    """
    主要处理内部操作时，自动补全必要字段，比如 created_by updated_by,
    """

    pass


class DmmModelInfoManager(BaseManager):
    def get_queryset(self):
        return super(DmmModelInfoManager, self).get_queryset().filter(active_status=DataModelActiveStatus.ACTIVE.value)


class DmmModelRelease(models.Model):
    # 数据模型发布版本
    version_id = models.CharField(_('模型版本ID'), primary_key=True, max_length=64)
    version_log = models.TextField(_('模型版本日志'))
    model_id = models.IntegerField(_('模型ID'))
    model_content = JsonField(_('模型版本内容'))
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_release'
        managed = False
        app_label = 'datamodel'


class DmmModelInfo(models.Model):
    # 数据模型主表
    model_id = models.AutoField(_('模型ID'), primary_key=True)
    model_name = models.CharField(_('模型名称'), max_length=255, unique=True)
    model_alias = models.CharField(_('模型别名'), max_length=255)
    model_type = models.CharField(_('模型类型'), max_length=32, choices=DataModelType.get_enum_alias_map_list())
    project_id = models.IntegerField(_('项目ID'))
    description = models.TextField(_('描述'), blank=True, null=True)
    publish_status = models.CharField(
        _('发布状态'),
        max_length=32,
        choices=DataModelPublishStatus.get_enum_alias_map_list(),
        default=DataModelPublishStatus.DEVELOPING.value,
    )
    active_status = models.CharField(
        _('可用状态, active/disabled/conflicting'),
        max_length=32,
        choices=DataModelActiveStatus.get_enum_alias_map_list(),
        default=DataModelActiveStatus.ACTIVE.value,
    )
    table_name = models.CharField(_('主表名称'), max_length=255)
    table_alias = models.CharField(_('主表别名'), max_length=255)
    step_id = models.IntegerField(
        _('步骤ID'), choices=DataModelStep.get_enum_alias_map_list(), default=DataModelStep.INIT.value
    )
    latest_version = models.ForeignKey(DmmModelRelease, null=True, on_delete=False)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    objects = DmmModelInfoManager()
    origin_objects = BaseManager()

    class Meta:
        db_table = 'dmm_model_info'
        managed = False
        app_label = 'datamodel'


class DmmModelTop(models.Model):
    # 数据模型用户置顶表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_top'
        managed = False
        app_label = 'datamodel'


class DmmFieldConstraintConfig(models.Model):
    # 字段约束表
    constraint_id = models.CharField(_('字段约束ID'), primary_key=True, max_length=64)
    constraint_index = models.IntegerField(_('字段约束index'))
    constraint_type = models.CharField(_('约束类型'), max_length=32, choices=ConstraintType.get_enum_alias_map_list())
    group_type = models.CharField(_('约束分组'), max_length=32)
    constraint_name = models.CharField(_('约束名称'), max_length=64)
    constraint_value = models.TextField(_('约束规则'), null=True)
    validator = JsonField(_('约束规则校验'))
    description = models.TextField(_('字段约束说明'), blank=True, null=True)
    editable = models.BooleanField(_('是否可以编辑'))
    allow_field_type = JsonField(_('允许的字段数据类型'), null=True)

    class Meta:
        db_table = 'dmm_field_constraint_config'
        managed = False
        app_label = 'datamodel'


class DmmModelField(OverrideHashModel):
    # 数据模型字段表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    field_name = models.CharField(_('字段名称'), max_length=255)
    field_alias = models.CharField(_('字段别名'), max_length=255)
    field_type = models.CharField(_('数据类型'), max_length=32)
    field_category = models.CharField(_('字段类型'), max_length=32, choices=FieldCategory.get_enum_alias_map_list())
    is_primary_key = models.BooleanField(_('是否主键'), default=False)
    description = models.TextField(_('描述'), blank=True, null=True)
    field_constraint_content = JsonField(_('字段约束'))
    field_clean_content = JsonField(_('清洗规则'))
    origin_fields = JsonField(_('计算来源字段'))
    field_index = models.IntegerField(_('字段位置'))
    source_model_id = models.IntegerField(_('来源模型ID'), null=True)
    source_field_name = models.CharField(_('来源字段'), max_length=255, null=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_field'
        unique_together = (('model_id', 'field_name'),)
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        return hash(self.field_name)


class DmmModelFieldStage(OverrideHashModel):
    # 数据模型字段草稿态临时表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    field_name = models.CharField(_('字段名称'), max_length=255)
    field_alias = models.CharField(_('字段别名'), max_length=255)
    field_type = models.CharField(_('数据类型'), max_length=32)
    field_category = models.CharField(_('字段类型'), max_length=32, choices=FieldCategory.get_enum_alias_map_list())
    is_primary_key = models.BooleanField(_('是否主键'), default=False)
    description = models.TextField(_('描述'), blank=True, null=True)
    field_constraint_content = JsonField(_('字段约束'))
    field_clean_content = JsonField(_('清洗规则'))
    origin_fields = JsonField(_('计算来源字段'))
    field_index = models.IntegerField(_('字段位置'))
    source_model_id = models.IntegerField(_('来源模型ID'), null=True)
    source_field_name = models.CharField(_('来源字段'), max_length=255, null=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_field_stage'
        unique_together = (('model_id', 'field_name'),)
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        return hash(self.field_name)


class DmmModelRelation(OverrideHashModel):
    # 数据模型关系表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    field_name = models.CharField(_('字段名称'), max_length=255)
    related_model_id = models.IntegerField(_('关联模型ID'))
    related_field_name = models.CharField(_('关联字段名称'), max_length=255)
    related_method = models.CharField(
        _('关联方式'), max_length=32, choices=RelatedMethod.get_enum_alias_map_list(),
        default=RelatedMethod.LEFT_JOIN.value
    )
    related_model_version_id = models.CharField(_('维度模型版本ID'), max_length=64, null=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_relation'
        unique_together = (('model_id', 'related_model_id'),)
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        attr_names = DATA_MODEL_UNIQUE_KEY_MAPPINGS[DataModelObjectType.MASTER_TABLE.value]
        return hash('-'.join([str(getattr(self, attr_name)) for attr_name in attr_names]))


class DmmModelRelationStage(OverrideHashModel):
    # 数据模型关系草稿态临时表
    id = models.AutoField(_('主键ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    field_name = models.CharField(_('字段名称'), max_length=255)
    related_model_id = models.IntegerField(_('关联模型ID'))
    related_field_name = models.CharField(_('关联字段名称'), max_length=255)
    related_method = models.CharField(
        _('关联方式'), max_length=32, choices=RelatedMethod.get_enum_alias_map_list(),
        default=RelatedMethod.LEFT_JOIN.value
    )
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'dmm_model_relation_stage'
        unique_together = (('model_id', 'related_model_id'),)
        managed = False
        app_label = 'datamodel'

    def __hash__(self):
        attr_names = DATA_MODEL_UNIQUE_KEY_MAPPINGS[DataModelObjectType.MASTER_TABLE.value]
        return hash('-'.join([str(getattr(self, attr_name)) for attr_name in attr_names]))


class DmmModelUserOperationLog(models.Model):
    # 数据模型用户操作流水记录
    id = models.AutoField(_('自增ID'), primary_key=True)
    model_id = models.IntegerField(_('模型ID'))
    object_type = models.CharField(
        _('操作对象类型'),
        max_length=32,
        choices=DataModelObjectType.get_enum_alias_map_list(
            [DataModelObjectType.FIELD.value, DataModelObjectType.MODEL_RELATION.value]
        ),
    )
    object_operation = models.CharField(
        _('操作类型'), max_length=32, choices=DataModelObjectOperationType.get_enum_alias_map_list()
    )
    object_id = models.CharField(_('操作对象ID'), max_length=255)
    object_name = models.CharField(_('操作对象英文名'), max_length=255)
    object_alias = models.CharField(_('操作对象中文名'), max_length=255)
    content_before_change = JsonField(_('操作前的模型内容'), default={})
    content_after_change = JsonField(_('操作后的模型内容'), default={})
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    extra = JsonField(_('附加信息'), null=True)

    class Meta:
        db_table = 'dmm_model_user_operation_log'
        managed = False
        app_label = 'datamodel'


meta_sync_register(DmmModelInfo)
meta_sync_register(DmmModelTop)
meta_sync_register(DmmFieldConstraintConfig)
meta_sync_register(DmmModelField)
meta_sync_register(DmmModelRelation)
meta_sync_register(DmmModelRelease)
