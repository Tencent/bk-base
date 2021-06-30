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

from common.base_utils import custom_params_valid

from datamanage.utils.api.dataflow import DataflowApi
from datamanage.pro.datamodel.models.model_dict import (
    AggregateFunction,
    CalculationContentType,
    SchedulingType,
    BatchWindowType,
    StreamWindowType,
    BatchSchedulePeriod,
    DependencyRule,
    RecoveryConfig,
    DependencyConfigType,
    ScheduleContentUnit,
    CountFreq,
    SchedulingContentValidateRange,
    CalculationAtomType,
)
from datamanage.pro.datamodel.dmm.indicator_manager import IndicatorManager


def set_format_window_size_and_unit(attrs):
    """
    设计指标格式化窗口长度和窗口长度单位
    :param attrs: {dict} 指标详情
    :return:
    """
    # 1)离线计算
    scheduling_content = attrs['scheduling_content']
    if attrs['scheduling_type'] == SchedulingType.BATCH.value:
        # a)固定窗口
        if scheduling_content['window_type'] == BatchWindowType.FIXED.value:
            scheduling_content['format_window_size'] = scheduling_content['unified_config']['window_size']
            scheduling_content['format_window_size_unit'] = ScheduleContentUnit.FORMAT_UNIT_DICT[
                scheduling_content['unified_config']['window_size_period']
            ]
        # b)按小时累加窗口
        elif scheduling_content['window_type'] == BatchWindowType.ACCUMULATE_BY_HOUR.value:
            scheduling_content['format_window_size'] = (
                scheduling_content['data_end'] - scheduling_content['data_start'] + 1
            )
            scheduling_content['format_window_size_unit'] = ScheduleContentUnit.HOUR
    # 2)实时计算
    if attrs['scheduling_type'] == SchedulingType.STREAM.value:
        # a)滑动窗口和累加窗口
        if scheduling_content['window_type'] in [StreamWindowType.SLIDE.value, StreamWindowType.ACCUMULATE.value]:
            scheduling_content['format_window_size'] = scheduling_content['window_time']
            scheduling_content['format_window_size_unit'] = ScheduleContentUnit.MINUTE
        # b)滚动窗口
        else:
            scheduling_content['format_window_size'] = scheduling_content['count_freq']
            scheduling_content['format_window_size_unit'] = ScheduleContentUnit.SECOND


class CalculationAtomSerializer(serializers.Serializer):
    model_id = serializers.IntegerField(label=_('模型ID'))


class CalculationAtomQuoteSerializer(CalculationAtomSerializer):
    # 统计口径引用参数校验
    calculation_atom_names = serializers.ListField(label=_('统计口径名称'), allow_empty=False)


class CalculationAtomInfoSerializer(serializers.Serializer):
    # 统计口径详情参数校验
    with_indicators = serializers.BooleanField(label=_('是否展示指标'), required=False, default=False)


class CalculationAtomListSerializer(CalculationAtomInfoSerializer):
    # 统计口径列表参数校验
    model_id = serializers.IntegerField(label=_('模型ID'))


class TableContentSerializer(serializers.Serializer):
    # 通过表单提交的统计方式内容校验
    calculation_field = serializers.CharField(label=_('统计字段'))
    calculation_function = serializers.CharField(label=_('聚合函数'))

    def validate_calculation_function(self, value):
        aggregate_functions = AggregateFunction.get_enum_value_list()
        if value not in aggregate_functions:
            raise ValidationError(_('聚合函数错误, 请修改聚合函数'))
        return value


class SqlContentSerializer(serializers.Serializer):
    # 通过SQL编辑器提交的统计方式内容校验
    calculation_formula = serializers.CharField(label=_('统计SQL'))


class UncertainField(serializers.Serializer):
    def run_validation(self, data=serializers.empty):
        if data == serializers.empty:
            return self.default
        return data

    def to_internal_value(self, data):
        return data

    def to_representation(self, value):
        return value


class CalculationContentSerializer(serializers.Serializer):
    # 统计方式参数校验
    option = serializers.ChoiceField(label=_('统计方式类型'), choices=CalculationContentType.get_enum_value_list())
    content = UncertainField(label=_('统计方式内容'))

    def validate(self, attrs):
        if attrs['option'] == CalculationContentType.TABLE.value:
            attrs['content'] = custom_params_valid(TableContentSerializer, attrs['content'])
        else:
            attrs['content'] = custom_params_valid(SqlContentSerializer, attrs['content'])
        return attrs


class CalculationAtomCreateSerializer(CalculationAtomSerializer):
    # 创建统计口径创建参数检验
    calculation_atom_name = serializers.CharField(label=_('统计口径名称'))
    calculation_atom_alias = serializers.CharField(label=_('统计口径别名'))
    description = serializers.CharField(label=_('统计口径描述'), required=False, allow_null=True, allow_blank=True)
    field_type = serializers.CharField(label=_('数据类型'))
    calculation_content = CalculationContentSerializer(label=_('统计方式'))


class CalculationAtomImportSerializer(CalculationAtomCreateSerializer):
    model_id = serializers.IntegerField(label=_('模型ID'), required=False)
    calculation_atom_type = serializers.ChoiceField(
        label=_('统计口径类型'),
        choices=[CalculationAtomType.CREATE, CalculationAtomType.QUOTE],
        required=False,
        default=CalculationAtomType.CREATE,
    )


class CalculationAtomUpdateSerializer(CalculationAtomSerializer):
    # 统计口径修改参数检验
    calculation_atom_alias = serializers.CharField(label=_('统计口径别名'), required=False)
    description = serializers.CharField(label=_('统计口径描述'), required=False, allow_null=True, allow_blank=True)
    field_type = serializers.CharField(label=_('数据类型'), required=False)
    calculation_content = CalculationContentSerializer(label=_('统计方式'), required=False)


class IndicatorSerializer(serializers.Serializer):
    # 指标创建参数校验
    model_id = serializers.IntegerField(label=_('模型ID'))
    indicator_alias = serializers.CharField(label=_('指标别名'))
    description = serializers.CharField(
        label=_('指标描述'), required=False, allow_null=True, allow_blank=True, default=None
    )
    calculation_atom_name = serializers.CharField(label=_('统计口径名称'))
    aggregation_fields = serializers.ListField(label=_('聚合字段'))
    filter_formula = serializers.CharField(
        label=_('过滤SQL'), required=False, allow_null=True, allow_blank=True, default=None
    )
    scheduling_type = serializers.ChoiceField(label=_('计算类型'), choices=SchedulingType.get_enum_value_list())
    scheduling_content = serializers.JSONField(label=_('调度内容'))
    parent_indicator_name = serializers.CharField(label=_('父指标名称'), allow_null=True, required=False, default=None)

    def validate(self, attrs):
        IndicatorManager.validate_ind_sche_cont_in_cur_model(
            attrs['model_id'], attrs, validate_range=SchedulingContentValidateRange.CURRENT
        )
        # 格式化窗口长度和窗口长度单位
        set_format_window_size_and_unit(attrs)
        return attrs


class IndicatorCreateSerializer(IndicatorSerializer):
    # 指标创建参数校验
    indicator_name = serializers.CharField(label=_('指标名称'))


class IndicatorImportSerializer(IndicatorCreateSerializer):
    # 指标创建参数校验
    model_id = serializers.IntegerField(label=_('模型ID'), required=False)

    def validate(self, attrs):
        query_ret = DataflowApi.param_verify(
            {'scheduling_content': attrs['scheduling_content'], 'scheduling_type': attrs['scheduling_type']}, raw=True
        )
        ret = query_ret['result']
        if not ret:
            raise ValidationError(_('调度内容校验不通过'))

        # 格式化窗口长度和窗口长度单位
        set_format_window_size_and_unit(attrs)
        return attrs


class IndicatorUpdateSerializer(IndicatorSerializer):
    # 指标创建参数校验
    indicator_id = serializers.IntegerField(label=_('指标ID'))
    indicator_name = serializers.CharField(label=_('指标名称'))
    indicator_alias = serializers.CharField(label=_('指标别名'), required=False)
    description = serializers.CharField(label=_('指标描述'), required=False, allow_null=True, allow_blank=True)
    calculation_atom_name = serializers.CharField(label=_('统计口径名称'), required=False)
    aggregation_fields = serializers.ListField(label=_('聚合字段'), required=False)
    filter_formula = serializers.CharField(label=_('过滤SQL'), required=False, allow_null=True, allow_blank=True)
    scheduling_type = serializers.ChoiceField(
        label=_('计算类型'), choices=SchedulingType.get_enum_value_list(), required=False
    )
    scheduling_content = serializers.JSONField(label=_('调度内容'), required=False)
    parent_indicator_name = serializers.CharField(label=_('父指标名称'), allow_null=True)

    def validate(self, attrs):
        if attrs.get('scheduling_content') and attrs.get('scheduling_type'):
            # 校验指标及其下游节点调度内容，只校验当前节点无法保证下游节点的调度内容
            IndicatorManager.validate_ind_sche_cont_in_cur_model(
                attrs['model_id'], attrs, validate_range=SchedulingContentValidateRange.OUTPUT
            )
            # 格式化窗口长度和窗口长度单位
            set_format_window_size_and_unit(attrs)
        return attrs


class UnifiedConfigSerializer(serializers.Serializer):
    # 统一配置内容校验 (窗口长度 & 依赖策略)
    window_size = serializers.IntegerField(label=_('窗口长度'), required=False, allow_null=True, default=None)
    window_size_period = serializers.ChoiceField(
        label=_('窗口长度单位'),
        required=False,
        allow_null=True,
        default=None,
        choices=BatchSchedulePeriod.get_enum_value_list(),
    )
    dependency_rule = serializers.ChoiceField(label=_('依赖策略'), choices=DependencyRule.get_enum_value_list())


class AdvancedSerializer(serializers.Serializer):
    # 高级配置 (是否启用失败重试 & 重试次数 & 重试间隔)
    recovery_enable = serializers.BooleanField(label=_('是否启用失败重试'), default=False)
    recovery_times = serializers.ChoiceField(
        label=_('重试次数'), choices=RecoveryConfig.RECOVERY_TIMES_LIST, required=False
    )
    recovery_interval = serializers.ChoiceField(
        label=_('重试间隔'), choices=RecoveryConfig.RECOVERY_INTERVAL_LIST, required=False
    )

    def validate(self, attrs):
        if attrs['recovery_enable']:
            if 'recovery_times' not in attrs or 'recovery_interval' not in attrs:
                raise ValidationError(_('启用失败重试时,必传重试次数&重试间隔'))
        return attrs


class BatchCommonSchedulingContentSerializer(serializers.Serializer):
    # 离线计算调度内容校验
    window_type = serializers.ChoiceField(label=_('窗口类型'), choices=BatchWindowType.get_enum_value_list())
    count_freq = serializers.IntegerField(label=_('统计频率'))
    schedule_period = serializers.ChoiceField(label=_('调度单位'), choices=BatchSchedulePeriod.get_enum_value_list())
    advanced = AdvancedSerializer(label=_('高级配置'))
    format_window_size = serializers.IntegerField(label=_('格式化窗口长度'), required=False)
    format_window_size_unit = serializers.CharField(label=_('格式化窗口长度单位'), required=False)
    dependency_config_type = serializers.ChoiceField(label=_('依赖配置'), choices=[DependencyConfigType.UNIFIED.value])
    unified_config = UnifiedConfigSerializer(label=_('统一配置内容校验'))

    def validate(self, attrs):
        # 离线计算频率校验和dataflow保持一致
        if attrs['schedule_period'] == BatchSchedulePeriod.HOUR.value:
            if attrs['count_freq'] not in CountFreq.batch_hour_count_freq_list:
                raise ValidationError(_('离线计算频率单位为小时，目前统计频率仅支持{}'.format(CountFreq.batch_hour_count_freq_list)))


class BatchSchedulingContentSerializer(BatchCommonSchedulingContentSerializer):
    # 离线计算调度内容校验
    fixed_delay = serializers.IntegerField(label=_('统计延迟'), required=False, allow_null=True)
    dependency_config_type = serializers.ChoiceField(
        label=_('依赖配置'), choices=[DependencyConfigType.UNIFIED.value], required=False, allow_null=True
    )
    delay = serializers.IntegerField(label=_('延迟时间'), required=False, allow_null=True)
    delay_period = serializers.ChoiceField(
        label=_('延迟时间单位'), choices=[BatchSchedulePeriod.HOUR.value], required=False, allow_null=True
    )
    data_start = serializers.IntegerField(label=_('数据起点'), required=False, allow_null=True)
    data_end = serializers.IntegerField(label=_('数据终点'), required=False, allow_null=True)
    unified_config = UnifiedConfigSerializer(label=_('统一配置内容校验'), required=False, allow_null=True)

    def validate(self, attrs):
        if attrs['window_type'] == BatchWindowType.FIXED.value:
            attrs = custom_params_valid(BatchFixedSchedulingContentSerializer, attrs)
        else:
            attrs = custom_params_valid(BatchAccumulateSchedulingContentSerializer, attrs)
        return attrs


class BatchFixedSchedulingContentSerializer(BatchCommonSchedulingContentSerializer):
    # 离线计算固定窗口内容校验
    fixed_delay = serializers.IntegerField(label=_('统计延迟'))

    def validate(self, attrs):
        attrs['format_window_size'] = attrs['unified_config']['window_size']
        attrs['format_window_size_unit'] = ScheduleContentUnit.FORMAT_UNIT_DICT[
            attrs['unified_config']['window_size_period']
        ]
        if not attrs['unified_config']['window_size'] or not attrs['unified_config']['window_size_period']:
            raise ValidationError(_('离线计算固定窗口统一配置必须传窗口长度 & 窗口长度单位'))
        if attrs['schedule_period'] != attrs['unified_config']['window_size_period']:
            raise ValidationError(_('离线计算固定窗口统一配置的窗口长度单位必须和统计频率的调度单位保持一致'))
        if attrs['count_freq'] > attrs['unified_config']['window_size']:
            raise ValidationError(_('离线计算固定窗口统一配置的窗口长度必须大于等于统计频率'))
        return attrs


class BatchAccumulateSchedulingContentSerializer(BatchCommonSchedulingContentSerializer):
    # 离线计算按小时窗口内容校验
    delay = serializers.IntegerField(label=_('延迟时间'))
    delay_period = serializers.ChoiceField(label=_('延迟时间单位'), choices=[BatchSchedulePeriod.HOUR.value])
    data_start = serializers.IntegerField(label=_('数据起点'))
    data_end = serializers.IntegerField(label=_('数据终点'))

    def validate(self, attrs):
        attrs['format_window_size'] = attrs['data_end'] - attrs['data_start'] + 1
        attrs['format_window_size_unit'] = ScheduleContentUnit.HOUR
        return attrs


class WindowLatenessSerializer(serializers.Serializer):
    # 实时计算是否计算延迟数据校验
    allowed_lateness = serializers.BooleanField(label=_('是否计算延迟数据'))
    lateness_time = serializers.IntegerField(label=_('延迟时间'), required=False)
    lateness_count_freq = serializers.IntegerField(label=_('延迟数据统计频率'), required=False)

    def validate(self, attrs):
        if attrs['allowed_lateness']:
            if 'lateness_time' not in attrs or 'lateness_count_freq' not in attrs:
                raise ValidationError(_('计算延迟数据,必传延迟时间&延迟数据统计频率'))
        return attrs


class StreamSchedulingContentSerializer(serializers.Serializer):
    # 实时计算调度内容校验
    window_type = serializers.ChoiceField(label=_('窗口类型'), choices=StreamWindowType.get_enum_value_list())
    count_freq = serializers.IntegerField(label=_('统计频率'))
    window_time = serializers.IntegerField(label=_('窗口长度'), required=False)
    waiting_time = serializers.IntegerField(label=_('等待时间'))
    window_lateness = WindowLatenessSerializer(label=_('是否计算延迟数据相关配置'))
    format_window_size = serializers.IntegerField(label=_('格式化窗口长度'), required=False)
    format_window_size_unit = serializers.CharField(label=_('格式化窗口长度单位'), required=False)

    def validate(self, attrs):
        if attrs['window_type'] in [StreamWindowType.SLIDE.value, StreamWindowType.ACCUMULATE.value]:
            if 'window_time' not in attrs:
                raise ValidationError(_('实时滑动窗口和累加窗口必传窗口长度'))
            attrs['format_window_size'] = attrs['window_time']
            attrs['format_window_size_unit'] = ScheduleContentUnit.MINUTE
        else:
            attrs['format_window_size'] = attrs['count_freq']
            attrs['format_window_size_unit'] = ScheduleContentUnit.SECOND
        return attrs


class IndicatorListSerializer(CalculationAtomSerializer):
    # 指标列表参数检验
    calculation_atom_name = serializers.CharField(label=_('统计口径名称'), required=False)
    parent_indicator_name = serializers.CharField(label=_('父指标名称'), required=False)
    with_sub_indicators = serializers.BooleanField(label=_('是否展示子指标'), required=False, default=False)
    with_details = serializers.ListField(label=_('是否展示指标字段'), required=False, allow_empty=True, default=[])

    def validate(self, attrs):
        if attrs.get('calculation_atom_name', None) and attrs.get('parent_indicator_name', None):
            raise ValidationError(_('获取指标列表不能同时指定统计口径名称&父指标名称'))
        return attrs


class IndicatorModelIdSerializer(serializers.Serializer):
    model_id = serializers.IntegerField(label=_('模型ID'))


class IndicatorInfoSerializer(IndicatorModelIdSerializer):
    # 指标详情参数检验
    with_sub_indicators = serializers.BooleanField(label=_('是否展示子指标'), required=False, default=False)
    model_id = serializers.IntegerField(label=_('模型ID'))
