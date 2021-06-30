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

from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError

from datamanage.utils.drf import GeneralSerializer
from datamanage.pro.dataquality.models.correction import (
    DataQualityCorrectConditionTemplate,
    DataQualityCorrectHandlerTemplate,
)


class CorrectConfigItemSerializer(serializers.Serializer):
    correct_config_item_id = serializers.IntegerField(required=False, allow_null=True, label=_('修正配置项ID'))
    field = serializers.CharField(label=_('数据集字段'))
    correct_config_detail = serializers.DictField(label=_('修正配置'))
    correct_config_alias = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_('修正配置别名'))
    description = serializers.CharField(required=False, default='', allow_null=True, allow_blank=True, label=_('描述'))

    def validate(self, attr):
        info = super(CorrectConfigItemSerializer, self).validate(attr)
        return info


class CorrectConfigCreateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(label=_('用户名'))
    data_set_id = serializers.CharField(label=_('数据集ID'))
    bk_biz_id = serializers.IntegerField(required=False, label=_('业务ID'))
    flow_id = serializers.IntegerField(label=_('数据流ID'))
    node_id = serializers.IntegerField(label=_('数据流节点ID'))
    source_sql = serializers.CharField(label=_('原始SQL'))
    correct_configs = serializers.ListField(label=_('修正配置列表'))
    generate_type = serializers.CharField(label=_('生成类型'), default='user')
    description = serializers.CharField(required=False, default='', allow_null=True, allow_blank=True, label=_('描述'))

    def validate(self, attr):
        info = super(CorrectConfigCreateSerializer, self).validate(attr)

        info['correct_configs'] = custom_params_valid(CorrectConfigItemSerializer, info['correct_configs'], many=True)
        if len(info['correct_configs']) == 0:
            raise ValidationError(_('修正配置不能为空'))

        data_set_tokens = info['data_set_id'].split('_')
        if 'bk_biz_id' not in info:
            info['bk_biz_id'] = data_set_tokens[0]

        return info


class CorrectConfigUpdateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(label=_('用户名'))
    source_sql = serializers.CharField(label=_('原始SQL'))
    correct_configs = serializers.ListField(label=_('修正配置列表'))

    def validate(self, attr):
        info = super(CorrectConfigUpdateSerializer, self).validate(attr)

        info['correct_configs'] = custom_params_valid(CorrectConfigItemSerializer, info['correct_configs'], many=True)
        if len(info['correct_configs']) == 0:
            raise ValidationError(_('修正配置不能为空'))

        return info


class CorrectSqlSerializer(serializers.Serializer):
    data_set_id = serializers.CharField(label=_('数据集ID'))
    source_sql = serializers.CharField(label=_('原始SQL'))
    correct_configs = serializers.ListField(label=_('修正配置列表'))

    def validate(self, attr):
        info = super(CorrectSqlSerializer, self).validate(attr)

        info['correct_configs'] = custom_params_valid(CorrectConfigItemSerializer, info['correct_configs'], many=True)
        if len(info['correct_configs']) == 0:
            raise ValidationError(_('修正配置不能为空'))

        return info


class CorrectDebugSubmitSerializer(serializers.Serializer):
    source_data_set_id = serializers.CharField(label=_('数据集ID'))
    data_set_id = serializers.CharField(label=_('数据集ID'))
    source_sql = serializers.CharField(label=_('原始SQL'))
    correct_configs = serializers.ListField(label=_('修正配置列表'))

    def validate(self, attr):
        info = super(CorrectDebugSubmitSerializer, self).validate(attr)

        info['correct_configs'] = custom_params_valid(CorrectConfigItemSerializer, info['correct_configs'], many=True)
        if len(info['correct_configs']) == 0:
            raise ValidationError(_('修正配置不能为空'))

        return info


class CorrectDebugResultSerializer(serializers.Serializer):
    debug_request_id = serializers.CharField(label=_('数据集ID'))


class CorrectConditionTemplateSerializer(GeneralSerializer):
    class Meta:
        model = DataQualityCorrectConditionTemplate

    def to_representation(self, instance):
        info = super(CorrectConditionTemplateSerializer, self).to_representation(instance)
        try:
            info['condition_template_config'] = json.loads(info['condition_template_config'])
        except Exception:
            info['condition_template_config'] = {}
        return info


class CorrectHandlerTemplateSerializer(GeneralSerializer):
    class Meta:
        model = DataQualityCorrectHandlerTemplate

    def to_representation(self, instance):
        info = super(CorrectHandlerTemplateSerializer, self).to_representation(instance)
        try:
            info['handler_template_config'] = json.loads(info['handler_template_config'])
        except Exception:
            info['handler_template_config'] = {}
        return info
