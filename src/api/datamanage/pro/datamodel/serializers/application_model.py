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
from django.utils.translation import ugettext_lazy as _

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError

from datamanage.utils.drf import GeneralSerializer
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.model_dict import SchedulingType
from datamanage.pro.datamodel.models.application import DmmModelInstance


class ApplicationModelInstanceRollback(serializers.Serializer):
    """回滚接口参数"""

    rollback_id = serializers.CharField(label=_('回滚ID'))


class ApplicationModelCreateSerializer(serializers.Serializer):
    """模型应用实例创建校验器"""

    model_id = serializers.IntegerField(label=_('数据模型ID'))
    version_id = serializers.CharField(label=_('数据模型版本ID'))
    model_instance_id = serializers.IntegerField(required=False, label=_('数据模型实例ID'))
    project_id = serializers.IntegerField(label=_('项目ID'))
    bk_biz_id = serializers.IntegerField(label=_('业务ID'))
    result_table_id = serializers.CharField(label=_('主表结果表ID'))
    flow_id = serializers.IntegerField(label=_('当前应用数据流ID'))
    flow_node_id = serializers.IntegerField(label=_('模型应用所在节点ID'))
    from_result_tables = serializers.ListField(label=_('输入结果表ID列表'))
    fields = serializers.ListField(label=_('字段映射'))

    def validate(self, attrs):
        """
        自定义校验逻辑，针对字段信息进行更详细的校验，并补充model_id的信息
        """
        info = super(ApplicationModelCreateSerializer, self).validate(attrs)

        for index, from_result_table_info in enumerate(info['from_result_tables']):
            info['from_result_tables'][index] = custom_params_valid(
                ModelInstanceFromTableSerializer,
                from_result_table_info,
            )

        for index, field_info in enumerate(info['fields']):
            info['fields'][index] = custom_params_valid(ModelInstanceFieldSerializer, field_info)
            if 'model_id' in field_info and field_info['model_id'] != info['model_id']:
                raise dm_pro_errors.ModelNotTheSameError()
            info['fields'][index]['model_id'] = info['model_id']

        return info


class ModelInstanceFieldRelationSerializer(serializers.Serializer):
    """模型应用实例主表关联关系校验器"""

    related_model_id = serializers.IntegerField(label=_('维度关联模型ID'))
    input_result_table_id = serializers.CharField(label=_('维表关联输入结果表'), allow_blank=True)
    input_field_name = serializers.CharField(label=_('维表关联输入字段名'), allow_blank=True)
    input_table_field = serializers.CharField(required=False, label=_('维表关联输入结果表ID/维表关联输入字段名'), allow_blank=True)

    def validate(self, attrs):
        """如果input_result_table_id和input_field_name为空，则解析input_table_field来生成"""
        info = super(ModelInstanceFieldRelationSerializer, self).validate(attrs)

        if 'input_table_field' in info:
            if info['input_table_field']:
                input_table_field_tokens = info['input_table_field'].split('/')
                info['input_result_table_id'] = input_table_field_tokens[0]
                info['input_field_name'] = input_table_field_tokens[1]
            else:
                info['input_result_table_id'] = ''
                info['input_field_name'] = ''

        return info


class ModelInstanceFromTableSerializer(serializers.Serializer):
    """模型应用实例模型节点来源表校验器"""

    result_table_id = serializers.CharField(label=_('来源输入表ID'))
    node_type = serializers.CharField(label=_('来源输入表节点类型'))


class ApplicationCleanContentSerializer(serializers.Serializer):
    # 字段清洗规则校验
    clean_option = serializers.ChoiceField(label=_('字段加工类型'), choices=['SQL', 'constant'], default='SQL')
    clean_content = serializers.CharField(label=_('字段加工逻辑'), default='', allow_blank=True)


class ModelInstanceFieldSerializer(serializers.Serializer):
    """模型应用实例主表字段映射校验器"""

    model_id = serializers.IntegerField(required=False, label=_('实例模型ID'))  # 在实例校验器的validate函数中回填
    field_name = serializers.CharField(label=_('输出字段名'))
    input_result_table_id = serializers.CharField(label=_('输入结果表'), allow_null=True, allow_blank=True)
    input_field_name = serializers.CharField(label=_('输入字段名'), allow_null=True, allow_blank=True)
    input_table_field = serializers.CharField(required=False, label=_('输入结果表ID/输入字段名'), allow_blank=True)
    is_generated_field = serializers.BooleanField(required=False, label=_('是否生成字段'), default=False)
    # 部分字段可能没有应用清洗规则
    application_clean_content = ApplicationCleanContentSerializer(allow_null=True, label=_('应用阶段清洗规则'))
    # 非维度关联字段不需要relation信息
    relation = ModelInstanceFieldRelationSerializer(
        required=False,
        allow_null=True,
        label=('维表关联信息'),
    )
    mapping_type = serializers.ChoiceField(required=False, label=_('字段映射类型'), choices=['field', 'constant'])
    constant = serializers.CharField(default='', label=_('常数字段映射类型时常数值'), allow_blank=True)

    def validate(self, attrs):
        """如果input_table_field不为空，则解析input_table_field来生成input_result_table_id和input_field_name"""
        info = super(ModelInstanceFieldSerializer, self).validate(attrs)

        if 'input_table_field' in info:
            if info['input_table_field']:
                input_table_field_tokens = info['input_table_field'].split('/')
                info['input_result_table_id'] = input_table_field_tokens[0]
                info['input_field_name'] = input_table_field_tokens[1]
            else:
                info['input_result_table_id'] = ''
                info['input_field_name'] = ''

        if 'mapping_type' in info:
            if info['mapping_type'] == 'constant':
                info['input_table_field'] = ''
                info['input_result_table_id'] = ''
                info['input_field_name'] = ''
                info['application'] = {
                    'clean_option': 'constant',
                    'clean_content': info['constant'],
                }

        clean_content = (info['application_clean_content'] or {}).get('clean_content')

        if not info['is_generated_field'] and not info['input_field_name'] and not clean_content:
            raise ValidationError('关于字段({})的来源结果表字段和字段加工逻辑不能同时为空'.format(info['field_name']))

        return info


class ApplicationModelUpdateSerializer(serializers.Serializer):
    """模型应用实例更新校验器"""

    model_id = serializers.IntegerField(required=False, label=_('数据模型ID'))
    version_id = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_('数据模型版本ID'))
    upgrade_version = serializers.BooleanField(required=False, default=False, label=_('是否更新到最新版本'))
    from_result_tables = serializers.ListField(required=False, label=_('输入结果表ID列表'))
    fields = serializers.ListField(required=False, label=_('字段映射'))

    def validate(self, attrs):
        """
        自定义校验逻辑，针对字段信息进行更详细的校验，并补充model_id的信息
        """
        info = super(ApplicationModelUpdateSerializer, self).validate(attrs)

        # 当且仅当需要更新模型时，才需要进行下面的校验逻辑
        if info['upgrade_version']:
            if 'model_id' in info and ('version_id' not in info or 'fields' not in info):
                raise ValidationError('更新模型ID必须在请求参数中包含该模型版本ID(version_id)和字段映射关系(fields)')

            if 'version_id' not in info:
                raise ValidationError('若需要更新模型版本(upgrade_version为true)，version_id为必选项')

            if 'version_id' in info and 'fields' not in info:
                raise ValidationError('更新模型版本必须请求参数中包含字段映射关系(fields)')

        if 'from_result_tables' in info:
            for index, from_result_table_info in enumerate(info['from_result_tables']):
                info['from_result_tables'][index] = custom_params_valid(
                    ModelInstanceFromTableSerializer,
                    from_result_table_info,
                )

        if 'fields' in info:
            for index, field_info in enumerate(info['fields']):
                info['fields'][index] = custom_params_valid(ModelInstanceFieldSerializer, field_info)
                if 'model_id' in info:
                    if 'model_id' in field_info and field_info['model_id'] != info['model_id']:
                        raise dm_pro_errors.ModelNotTheSameError()
                    info['fields'][index]['model_id'] = info['model_id']

        return info


class ApplicationIndicatorCreateSerializer(serializers.Serializer):
    """模型应用实例指标创建校验器"""

    bk_biz_id = serializers.IntegerField(label=_('业务ID'))
    result_table_id = serializers.CharField(label=_('结果表ID'))
    parent_result_table_id = serializers.CharField(label=_('来源结果表ID'))
    flow_node_id = serializers.IntegerField(label=_('原DataFlow节点ID'))
    calculation_atom_name = serializers.CharField(label=_('统计口径名称'))
    aggregation_fields = serializers.ListField(allow_empty=True, label=_('聚合字段列表，允许为空'))
    filter_formula = serializers.CharField(label=_('过滤SQL'), allow_blank=True)
    scheduling_type = serializers.ChoiceField(choices=SchedulingType.get_enum_value_list(), label=_('计算类型'))
    scheduling_content = serializers.DictField(label=_('调度内容'))


class ApplicationIndicatorUpdateSerializer(serializers.Serializer):
    """模型应用实例指标更新校验器"""

    parent_result_table_id = serializers.CharField(required=False, label=_('来源结果表ID'))
    calculation_atom_name = serializers.CharField(required=False, label=_('统计口径名称'))
    aggregation_fields = serializers.ListField(required=False, allow_empty=True, label=_('聚合字段列表，允许为空'))
    filter_formula = serializers.CharField(required=False, label=_('过滤SQL'), allow_blank=True)
    scheduling_type = serializers.ChoiceField(
        required=False, choices=SchedulingType.get_enum_value_list(), label=_('计算类型')
    )
    scheduling_content = serializers.DictField(required=False, label=_('调度内容'))


class ApplicationReleasedModelListSerializer(serializers.Serializer):
    """模型应用节点模型列表接口参数"""

    project_id = serializers.IntegerField(required=False, label=_('项目ID'))
    is_open = serializers.BooleanField(required=False, label=_('是否公开'))


class ApplicationFieldsMappingsSerializer(serializers.Serializer):
    """模型应用初始化字段映射接口参数"""

    model_id = serializers.IntegerField(label=_('数据模型ID'))
    version_id = serializers.CharField(label=_('数据模型版本ID'))
    from_result_tables = serializers.JSONField(label=_('输入节点列表'), required=False, binary=True)

    def validate(self, attrs):
        """
        自定义校验from_result_tables里面的结构
        """
        info = super(ApplicationFieldsMappingsSerializer, self).validate(attrs)

        if 'from_result_tables' in info:
            for index, from_result_table_info in enumerate(info['from_result_tables']):
                info['from_result_tables'][index] = custom_params_valid(
                    ModelInstanceFromTableSerializer,
                    from_result_table_info,
                )

        return info


class ApplicationInstanceByTableSerializer(serializers.Serializer):
    """根据结果表获取数据模型应用实例信息接口参数"""

    result_table_id = serializers.CharField(label=_('结果表ID'))


class ApplicationInstanceDataflowInputSerializer(serializers.Serializer):
    """数据模型应用任务输入参数"""

    main_table = serializers.CharField(label=_('来源主表'))
    dimension_tables = serializers.ListField(label=_('来源维度表'), allow_empty=True)


class ApplicationInstanceDataflowStorageSerializer(serializers.Serializer):
    """数据模型应用任务存储配置"""

    cluster_type = serializers.CharField(label=_('存储类型'))
    cluster_name = serializers.CharField(label=_('存储集群名称'))
    expires = serializers.IntegerField(label=_('过期时间'))

    specific_params = serializers.DictField(required=False, label=_('具体存储字段'))


class ApplicationInstanceDataflowTableSerializer(serializers.Serializer):
    """数据模型应用任务主表参数"""

    table_name = serializers.CharField(required=False, label=_('主表结果表英文名'))
    table_alias = serializers.CharField(required=False, label=_('主表结果表中文名'))
    fields = serializers.ListField(required=False, label=_('字段映射'))
    storages = serializers.ListField(required=False, label=_('主表存储配置'), allow_empty=True)

    def validate(self, attrs):
        """
        自定义校验逻辑，针对字段信息进行更详细的校验，并补充model_id的信息
        """
        info = super(ApplicationInstanceDataflowTableSerializer, self).validate(attrs)

        if 'fields' in info:
            for index, field_info in enumerate(info['fields']):
                info['fields'][index] = custom_params_valid(ModelInstanceFieldSerializer, field_info)

        if 'storages' in info:
            for index, storage_info in enumerate(info['storages']):
                info['storages'][index] = custom_params_valid(
                    ApplicationInstanceDataflowStorageSerializer,
                    storage_info,
                )

        return info


class ApplicationInstanceDataflowIndicatorSerializer(serializers.Serializer):
    """数据模型应用任务指标信息"""

    indicator_name = serializers.CharField(label=_('指标名称'))
    table_custom_name = serializers.CharField(required=False, label=_('指标表名自定义部分'))
    table_alias = serializers.CharField(required=False, label=_('指标表中文名'))
    storages = serializers.ListField(required=False, label=_('指标存储配置'), allow_empty=True)

    def validate(self, attrs):
        """
        自定义校验逻辑，针对字段信息进行更详细的校验，并补充model_id的信息
        """
        info = super(ApplicationInstanceDataflowIndicatorSerializer, self).validate(attrs)

        if 'storages' in info:
            for index, storage_info in enumerate(info['storages']):
                info['storages'][index] = custom_params_valid(
                    ApplicationInstanceDataflowStorageSerializer,
                    storage_info,
                )

        return info


class ApplicationInstanceDataflowSerializer(serializers.Serializer):
    """生成数据模型应用任务接口参数"""

    model_id = serializers.IntegerField(label=_('数据模型ID'))
    version_id = serializers.CharField(required=False, label=_('数据模型版本ID'))
    project_id = serializers.IntegerField(label=_('项目ID'))
    bk_biz_id = serializers.IntegerField(label=_('业务ID'))
    input = ApplicationInstanceDataflowInputSerializer(label=_('输入表信息'))
    main_table = ApplicationInstanceDataflowTableSerializer(required=False, label=_('主表信息'))
    default_indicators_storages = serializers.ListField(required=False, label=_('默认指标存储'), allow_empty=True)
    indicators = serializers.ListField(required=False, label=_('模型指标信息'))

    def validate(self, attrs):
        """
        自定义校验逻辑，针对字段信息进行更详细的校验，并补充model_id的信息
        """
        info = super(ApplicationInstanceDataflowSerializer, self).validate(attrs)

        if 'default_indicators_storages' in info:
            for index, storage_info in enumerate(info['default_indicators_storages']):
                info['default_indicators_storages'][index] = custom_params_valid(
                    ApplicationInstanceDataflowStorageSerializer,
                    storage_info,
                )

        if 'indicators' in info:
            for index, indicator_info in enumerate(info['indicators']):
                info['indicators'][index] = custom_params_valid(
                    ApplicationInstanceDataflowIndicatorSerializer,
                    indicator_info,
                )

        return info


class ApplicationIndicatorFieldsSerializer(serializers.Serializer):
    """获取模型实例指标可用字段接口参数"""

    parent_result_table_id = serializers.CharField(label=_('上游结果表ID'))
    field_category = serializers.CharField(required=False, label=_('字段类型'))


class ApplicationIndicatorMenuSerializer(serializers.Serializer):
    """根据数据模型实例ID或者节点ID获取当前模型应用的指标目录"""

    model_instance_id = serializers.IntegerField(required=False, label=_('数据模型实例ID'))
    node_id = serializers.IntegerField(required=False, label=_('数据模型节点ID'))

    def validate(self, attrs):
        info = super(ApplicationIndicatorMenuSerializer, self).validate(attrs)

        if 'model_instance_id' not in info and 'node_id' not in info:
            raise ValidationError('需要提供参数model_instance_id和node_id的其中一个')

        return info


class ApplicationInstanceInitSerializer(ApplicationIndicatorMenuSerializer):
    """保存模型节点后初始化指标节点接口参数"""

    indicator_names = serializers.ListField(required=False, label=_('指定初始化的指标名称'), allow_empty=True)


class ApplicationDataflowCheckSerializer(serializers.Serializer):
    """检查数据模型应用任务是否可以正常启动"""

    flow_id = serializers.IntegerField(label=_('数据流ID'))


class ModelInstanceSerializer(GeneralSerializer):
    class Meta:
        model = DmmModelInstance
