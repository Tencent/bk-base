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
import time

from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError

from datamanage.utils.metric_configs import ParamType, ConstantType, FunctionType
from datamanage.lite.dmonitor.serializers import NumberSerializerMixin
from datamanage.pro.dataquality.models.audit import (
    DataQualityAuditRuleTemplate,
    DataQualityAuditFunction,
    DataQualityAuditTask,
)


class RuleConfigTimerSerializer(serializers.DictField):
    timer_type = serializers.ChoiceField(
        label=_('定时器类型'), choices=(('crontab', _('Crontab调度配置')), ('fix', _('固定时间触发配置')), ('none', _('一次性执行')))
    )
    timer_value = serializers.CharField(label=_('定时器值'), default='')


class RuleDetailInputSerializer(serializers.ListField):
    param_type = serializers.CharField(label=_('输入参数类型'))
    param_name = serializers.CharField(label=_('输入参数名称'))


class RuleDetailParamSerializer(serializers.ListField):
    param_type = serializers.CharField(label=_('参数类型'))
    metric_name = serializers.CharField(required=False, label=_('指标名称'))
    event_id = serializers.CharField(required=False, label=_('事件ID'))
    constant_type = serializers.CharField(required=False, label=_('常数类型'))
    constant_value = serializers.CharField(required=False, label=_('常数值'))
    function = serializers.DictField(required=False, label=_('子操作'))
    expression = serializers.CharField(required=False, label=_('表达式'))

    def validate(self, attr):
        info = super(RuleDetailParamSerializer, self).validate(attr)

        if info['param_type'] not in ParamType.ALL_TYPES:
            raise ValidationError(_('非法参数类型: {}').format(info['param_type']))

        if info['param_type'] == ParamType.METRIC:
            if 'metric_name' not in info or not info['metric_name']:
                raise ValidationError(_('指标类型参数需要提供有效的"指标名称(metric_name)"'))
        elif info['param_type'] == ParamType.CONSTANT:
            if 'constant_type' not in info or not info['constant_type']:
                raise ValidationError(_('常数类型参数需要提供有效的"常数类型(constant_type)"'))
            if info['constant_type'] not in ConstantType.ALL_TYPES:
                raise ValidationError(_('非法的常数类型: {}').format(info['constant_type']))

            # 常数值允许为空，所以不需要非空校验
            if 'constant_value' not in info:
                raise ValidationError(_('常数类型参数需要提供有效的"常数值(constant_value)"'))
        elif info['param_type'] == ParamType.EVENT:
            if 'event_id' not in info or not info['event_id']:
                raise ValidationError(_('事件类型参数需要提供有效的"事件ID(event_id)"'))
        elif info['param_type'] == ParamType.FUNCTION:
            if 'function' not in info or not info['function']:
                raise ValidationError(_('操作类型参数需要提供有效的"操作逻辑(function)"'))
            info['function'] = custom_params_valid(RuleDetailFunctionSerializer, info['function'])
        elif info['param_type'] == ParamType.EXPRESSION:
            if 'expression' not in info or not info['expression']:
                raise ValidationError(_('表达式类型参数需要提供有效的"表达式(expression)"'))

        return info


class RuleDetailFunctionSerializer(serializers.DictField):
    function_type = serializers.CharField(label=_('规则操作类型'))
    function_name = serializers.CharField(label=_('规则操作名称'))
    function_params = RuleDetailParamSerializer(label=_('规则操作参数'))

    def validate(self, attr):
        info = super(RuleDetailFunctionSerializer, self).validate(attr)

        if info['function_type'] not in FunctionType.ALL_TYPES:
            raise ValidationError(_('非法操作类型: {}').format(info['function_type']))

        return info


class RuleDetailOutputSerializer(serializers.DictField):
    event_id = serializers.CharField(label=_('输出事件ID'))
    event_name = serializers.CharField(label=_('输出事件名称'))
    event_alias = serializers.CharField(label=_('输出事件中文名'))
    event_type = serializers.CharField(label=_('输出事件类型'))
    event_sub_type = serializers.CharField(label=_('输出事件子类型'))
    event_currency = serializers.IntegerField(label=_('事件时效(秒)'))
    tags = serializers.ListField(required=False, label=_('事件标签'))


class RuleConfigSerializer(serializers.DictField):
    input = RuleDetailInputSerializer(required=False, label=_('规则输入配置'))
    rule = serializers.DictField(label=_('规则操作配置'))
    output = RuleDetailOutputSerializer(required=False, label=_('规则输出配置'))

    def validate(self, attr):
        info = super(RuleConfigSerializer, self).validate(attr)

        if 'function' in info['rule']:
            info['rule']['function'] = custom_params_valid(RuleDetailFunctionSerializer, info['rule']['function'])

        return info


class EventConvergenceConfigSerializer(serializers.DictField, NumberSerializerMixin):
    duration = serializers.IntegerField(default=1, label=_('触发时间检测范围'))
    alert_threshold = serializers.IntegerField(default=1, label=_('触发次数阈值'))
    mask_time = serializers.IntegerField(default=60, label=_('告警屏蔽时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['duration'], 10080):
            raise ValidationError(_('触发周期必须是1~10080的正整数(分钟)'))

        if not self.valid_time_number(attr['alert_threshold'], 10000):
            raise ValidationError(_('触发次数必须是1~10000的正整数'))

        if not self.valid_time_number(attr['mask_time'], 10080):
            raise ValidationError(_('告警屏蔽时间必须是1~10080的正整数(分钟)'))
        return attr


class DataQualityRuleSerializer(serializers.Serializer):
    data_set_id = serializers.CharField(label=_('数据集ID'))
    bk_biz_id = serializers.IntegerField(required=False, label=_('业务ID'))
    rule_name = serializers.CharField(label=_('规则名称'))
    rule_description = serializers.CharField(label=_('规则描述'))
    rule_template_id = serializers.IntegerField(label=_('规则模板ID'))
    rule_config = serializers.ListField(required=True, label=_('规则配置'))
    rule_config_alias = serializers.CharField(label=_('规则配置别名'))
    event_name = serializers.CharField(label=_('事件名称'))
    event_alias = serializers.CharField(label=_('事件别名'))
    event_description = serializers.CharField(label=_('事件描述'))
    event_type = serializers.CharField(label=_('事件类型'))
    event_sub_type = serializers.CharField(label=_('事件子类型'), required=False)
    event_polarity = serializers.CharField(label=_('事件极性'))
    event_currency = serializers.IntegerField(label=_('事件时效性'))
    sensitivity = serializers.CharField(required=False, label=_('敏感度'))
    event_detail_template = serializers.CharField(label=_('事件详情模板'))
    notify_ways = serializers.ListField(required=False, label=_('事件通知方式'))
    receivers = serializers.ListField(required=False, label=_('事件接收人'))
    convergence_config = EventConvergenceConfigSerializer(required=False, label=_('收敛配置'))

    def validate(self, attr):
        info = super(DataQualityRuleSerializer, self).validate(attr)

        data_set_tokens = info['data_set_id'].split('_')
        if 'bk_biz_id' not in info:
            info['bk_biz_id'] = data_set_tokens[0]

        return info


class DataQualityRuleTemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataQualityAuditRuleTemplate

    def to_representation(self, instance):
        info = super(DataQualityRuleTemplateSerializer, self).to_representation(instance)
        try:
            info['template_config'] = json.loads(info['template_config'])
        except Exception:
            info['template_config'] = {}
        return info


class DataQualityAuditTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataQualityAuditTask


class DataQualityAuditFunctionSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataQualityAuditFunction

    def to_representation(self, instance):
        info = super(DataQualityAuditFunctionSerializer, self).to_representation(instance)
        try:
            info['function_configs'] = json.loads(info['function_configs'])
        except Exception:
            info['function_configs'] = {}
        return info


class DataQualityRuleMetricsSerializer(serializers.Serializer):
    start_time = serializers.IntegerField(required=False, label=_('开始时间'), default=int(time.time()) - 86400)
    end_time = serializers.IntegerField(required=False, label=_('结束时间'), default=int(time.time()))
