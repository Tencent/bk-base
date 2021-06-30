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


from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.model_dict import (
    DataModelType,
    FieldCategory,
    InnerField,
    TimeField,
    ExcludeFieldType,
    OperationLogQueryConditionKeys,
)
from datamanage.pro.datamodel.handlers.constraint import get_field_constraint_configs
from datamanage.pro.datamodel.handlers.field_type import get_field_type_list
from datamanage.pro.datamodel.serializers.indicator import IndicatorImportSerializer, CalculationAtomImportSerializer

MAX_TAG_NUM = 10


class BkUserNameSerializer(serializers.Serializer):
    bk_username = serializers.CharField(max_length=50, label=_('用户名'))


class DataModelNameValidateSerializer(serializers.Serializer):
    # 数据模型名称校验
    model_name = serializers.CharField(label=_('模型名称'))


class TagsSerializer(serializers.Serializer):
    # 数据模型标签校验
    tag_code = serializers.CharField(label=_('标签名称'), allow_blank=True, allow_null=True)
    tag_alias = serializers.CharField(label=_('标签别名'))


class DataModelSerializer(BkUserNameSerializer):
    def validate(self, attrs):
        # 判断标签个数
        if len(attrs['tags']) > MAX_TAG_NUM:
            raise dm_pro_errors.TagNumberLargerThanMaxError(message_kv={'max_tag_num': MAX_TAG_NUM})
        return attrs


class DataModelCreateSerializer(DataModelSerializer):
    # 模型创建参数检验
    model_name = serializers.CharField(label=_('模型名称'))
    model_alias = serializers.CharField(label=_('模型别名'))
    model_type = serializers.ChoiceField(label=_('模型类型'), choices=DataModelType.get_enum_value_list())
    project_id = serializers.IntegerField(label=_('项目ID'))
    description = serializers.CharField(
        label=_('模型描述'), required=False, allow_null=True, allow_blank=True, default=None
    )
    tags = serializers.ListField(label=_('标签'), allow_empty=False, child=TagsSerializer())


class DataModelUpdateSerializer(DataModelSerializer):
    # 模型修改参数检验
    model_alias = serializers.CharField(label=_('数据别名'), required=False)
    description = serializers.CharField(label=_('模型描述'), required=False, allow_null=True, allow_blank=True)
    tags = serializers.ListField(label=_('标签'), allow_empty=False, child=TagsSerializer(), required=False)

    def validate(self, attrs):
        # 判断标签个数
        if 'tags' in attrs:
            return super(DataModelUpdateSerializer, self).validate(attrs)
        return attrs


class DataModelListSerializer(BkUserNameSerializer):
    # 模型列表参数检验
    model_id = serializers.IntegerField(label=_('模型ID'), required=False)
    model_name = serializers.CharField(label=_('模型名称'), required=False)
    model_type = serializers.ChoiceField(label=_('模型类型'), required=False, choices=DataModelType.get_enum_value_list())
    project_id = serializers.IntegerField(label=_('项目ID'), required=False)
    keyword = serializers.CharField(label=_('搜索关键字'), required=False)


class ConditionSerializer(serializers.Serializer):
    # 操作记录搜索条件参数校验
    key = serializers.ChoiceField(label=_('搜索条件key'), choices=OperationLogQueryConditionKeys.get_enum_value_list())
    value = serializers.ListField(label=_('搜索条件value'), allow_empty=False)


class OperationLogListSerializer(BkUserNameSerializer):
    # 操作记录列表参数检验
    conditions = serializers.ListField(label=_('搜索条件参数'), child=ConditionSerializer(), required=False, default=[])
    page = serializers.IntegerField(label=_('页码'))
    page_size = serializers.IntegerField(label=_('每页条数'))
    start_time = serializers.DateTimeField(label=_('启始时间'), required=False, default=None)
    end_time = serializers.DateTimeField(label=_('结束时间'), required=False, default=None)
    order_by_created_at = serializers.ChoiceField(
        label=_('按照操作时间排序'), choices=['asc', 'desc'], required=False, default='desc'
    )


class RelatedDimensionModelListSerializer(serializers.Serializer):
    # 可以关联/已经关联的维度模型列表
    related_model_id = serializers.IntegerField(label=_('关联表模型ID, 用于前端点击关联模型设置回填'), required=False, default=None)
    published = serializers.BooleanField(label=_('是否只返回已经发布的模型'), required=False, default=False)


class DataModelInfoSerializer(serializers.Serializer):
    with_details = serializers.ListField(label=_('是否展示详情'), required=False, allow_empty=True, default=[])


class ConstraintSerializer(serializers.Serializer):
    # 字段约束内容中的content校验
    constraint_id = serializers.CharField(label=_('约束类型'))
    constraint_content = serializers.CharField(label=_('约束内容'), allow_null=True)

    def validate(self, attrs):
        constraint_list = get_field_constraint_configs()
        constraint_id_list = [constraint_dict['constraint_id'] for constraint_dict in constraint_list]
        if attrs['constraint_id'] not in constraint_id_list:
            raise ValidationError(_('约束ID错误, 请修改约束ID'))
        if attrs['constraint_id'] != 'not_null' and not attrs['constraint_content']:
            raise ValidationError(_('约束不是非空时，约束内容不能为空'))
        return attrs


class FieldConstraintGroupSerializer(serializers.Serializer):
    # 字段约束组内校验
    op = serializers.ChoiceField(label=_('逻辑运算符'), required=False, choices=['OR', 'AND'])
    items = serializers.ListField(label=_('字段约束组内内容'), child=ConstraintSerializer())


class FieldConstraintSerializer(serializers.Serializer):
    # 字段约束内容校验
    op = serializers.ChoiceField(label=_('逻辑运算符'), required=False, choices=['OR', 'AND'])
    groups = serializers.ListField(label=_('字段约束组间内容'), child=FieldConstraintGroupSerializer())


class FieldCleanContentSerializer(serializers.Serializer):
    # 字段清洗规则校验
    clean_option = serializers.ChoiceField(label=_('字段加工类型'), choices=['SQL'], required=False)
    clean_content = serializers.CharField(label=_('字段加工逻辑'), required=False)

    def validate(self, attrs):
        clean_option = attrs.get('clean_option', None)
        clean_content = attrs.get('clean_content', None)
        if bool(clean_option) ^ bool(clean_content):
            raise ValidationError(_('字段清洗规则校验不通过，请修改字段清洗规则'))
        return attrs


class FieldSerializer(serializers.Serializer):
    # 字段通用参数校验
    field_name = serializers.CharField(label=_('字段名称'))
    field_alias = serializers.CharField(label=_('字段别名'))
    field_index = serializers.IntegerField(label=_('字段位置'))
    field_type = serializers.CharField(label=_('数据类型'))
    field_category = serializers.ChoiceField(label=_('字段类型'), choices=FieldCategory.get_enum_value_list())
    is_primary_key = serializers.BooleanField(label=_('是否主键'), default=False)
    description = serializers.CharField(label=_('字段描述'), required=False, allow_null=True, allow_blank=True)
    field_constraint_content = FieldConstraintSerializer(label=_('字段约束内容'), allow_null=True, default=None)
    field_clean_content = FieldCleanContentSerializer(label=_('清洗规则'), allow_null=True, default=None)
    source_model_id = serializers.IntegerField(label=_('来源模型ID'), allow_null=True, default=None)
    source_field_name = serializers.CharField(label=_('来源字段'), allow_null=True, default=None)

    def validate_field_type(self, value):
        field_type_list = get_field_type_list()
        if value not in field_type_list:
            raise ValidationError(_('数据类型错误, 请修改数据类型'))
        return value

    def validate_field_name(self, value):
        if value.lower() in InnerField.INNER_FIELD_LIST:
            raise ValidationError(_('字段名称是系统内部字段，请修改字段名称'))
        return value

    def validate(self, attrs):
        # 维度模型扩展字段不能修改值约束和字段加工逻辑
        if attrs['source_model_id'] and attrs['source_field_name']:
            attrs['field_constraint_content'] = None
            attrs['field_clean_content'] = None
        if attrs['field_type'] == TimeField.TIME_FIELD_TYPE and attrs['field_name'] != TimeField.TIME_FIELD_NAME:
            raise ValidationError(_('数据类型timestamp校验不通过，请修改数据类型'))
        return attrs


class ModelRelationSerializer(serializers.Serializer):
    # 模型关联校验
    field_name = serializers.CharField(label=_('主表关联字段名称'))
    related_model_id = serializers.IntegerField(label=_('关联表模型ID'))
    related_field_name = serializers.CharField(label=_('关联表关联字段名称'))


class MasterTableCreateSerializer(BkUserNameSerializer):
    # 主表创建参数检验
    fields = serializers.ListField(label=_('模型主表字段列表'), child=FieldSerializer(), allow_empty=False)
    model_relation = serializers.ListField(
        label=_('模型主表关联信息'), child=ModelRelationSerializer(), required=False, allow_empty=True, default=[]
    )


class MasterTableListSerializer(serializers.Serializer):
    # 主表列表参数校验
    with_time_field = serializers.BooleanField(label=_('是否显示时间字段'), required=False, default=False)
    allow_field_type = serializers.ListField(label=_('允许的字段数据类型'), required=False, allow_empty=True, default=[])
    with_details = serializers.ListField(label=_('字段展示的详情内容'), required=False, allow_empty=True, default=[])
    latest_version = serializers.BooleanField(label=_('是否返回最新发布版本内容'), required=False, default=False)

    def validate_allow_field_type(self, value):
        field_type_list = get_field_type_list()
        if not (set(value).issubset(field_type_list)):
            raise ValidationError(_('允许的字段数据类型错误, 请修改允许的字段数据类型'))
        return value

    def validate(self, attrs):
        if attrs['with_time_field'] and attrs['allow_field_type']:
            raise ValidationError(_('在指定允许的字段数据类型时，不能返回时间字段'))
        return attrs


class DataModelColumnProcessVerifySerializer(serializers.Serializer):
    table_name = serializers.CharField(label=_('处理表名'), required=True)
    verify_fields = serializers.ListField(label=_('指定当前需要校验的字段'), required=True)
    scope_field_list = serializers.ListField(label=_('处理表可引用的表schema'), required=False, default=list())
    require_field_list = serializers.ListField(label=_('需要输出的表schema'), required=False, default=list())


class DataModelReleaseSerializer(BkUserNameSerializer):
    # 模型发布参数检验
    version_log = serializers.CharField(label=_('模型版本日志'))


class FieldTypeListSerializer(serializers.Serializer):
    # 数据类型参数校验
    include_field_type = serializers.ListField(label=_('额外返回的数据类型列表'), required=False, default=[])
    exclude_field_type = serializers.ListField(label=_('不返回的数据类型列表'), required=False, default=[])

    def validate(self, attrs):
        if not set(attrs['include_field_type']).issubset([ExcludeFieldType.TIMESTAMP_FIELD_TYPE.value]):
            raise ValidationError(_('额外返回的数据类型列表错误, 请修改额外返回的数据类型列表'))
        if not set(attrs['exclude_field_type']).issubset(get_field_type_list()):
            raise ValidationError(_('不返回的数据类型列表错误, 请修改不返回的数据类型列表'))
        return attrs


class ResultTableFieldListSerializer(serializers.Serializer):
    # rt字段列表校验
    with_time_field = serializers.BooleanField(label=_('是否显示时间字段'), required=False, default=False)


class DataModelDiffSerializer(BkUserNameSerializer):
    # 数据模型diff参数
    orig_version_id = serializers.CharField(label=_('源版本'), required=False, default=None, allow_null=True)
    new_version_id = serializers.CharField(label=_('目标版本'), required=False, default=None, allow_null=True)


class ModelDetailSerializer(serializers.Serializer):
    # 模型详情校验
    fields = serializers.ListField(label=_('模型主表字段列表'), child=FieldSerializer(), allow_empty=False)
    model_relation = serializers.ListField(
        label=_('模型主表关联信息'), child=ModelRelationSerializer(), required=False, allow_empty=True, default=[]
    )
    calculation_atoms = serializers.ListField(
        label=_('模型统计口径信息'), child=CalculationAtomImportSerializer(), required=False, allow_empty=True, default=[]
    )
    indicators = serializers.ListField(
        label=_('模型指标信息'), child=IndicatorImportSerializer(), required=False, allow_empty=True, default=[]
    )


class DataModelImportSerializer(DataModelCreateSerializer):
    # 数据模型导入参数
    model_detail = ModelDetailSerializer(label=_('模型详情'))


class DataModelOverviewSerializer(serializers.Serializer):
    # 模型预览参数校验
    latest_version = serializers.BooleanField(label=_('是否返回模型最新发布版本的预览内容'), required=False, default=False)
