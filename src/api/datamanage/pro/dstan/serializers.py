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

from datamanage.pro.dstan import models
from datamanage.pro.dstan.constants import DataSet, StandardType


class DmStandardConfigSerializer(serializers.ModelSerializer):
    # 数据标准总表Serializer
    class Meta:
        model = models.DmStandardConfig
        fields = '__all__'


class DmStandardVersionConfigSerializer(serializers.ModelSerializer):
    # 数据标准版本表Serializer
    class Meta:
        model = models.DmStandardVersionConfig
        fields = '__all__'


class DmStandardContentConfigSerializer(serializers.ModelSerializer):
    # 数据标准内容表Serializer
    parent_id = serializers.JSONField(required=True)
    window_period = serializers.JSONField(required=True)

    class Meta:
        model = models.DmStandardContentConfig
        fields = '__all__'


class DmDetaildataFieldConfigSerializer(serializers.ModelSerializer):
    # 明细数据标准字段详情表 Serializer
    class Meta:
        model = models.DmDetaildataFieldConfig
        fields = '__all__'


class DmIndicatorFieldConfigSerializer(serializers.ModelSerializer):
    # 原子指标字段详情表 Serializer
    class Meta:
        model = models.DmIndicatorFieldConfig
        fields = '__all__'


class DmConstraintConfigSerializer(serializers.ModelSerializer):
    # 支持的值约束配置表 Serializer
    rule = serializers.JSONField(required=True)

    class Meta:
        model = models.DmConstraintConfig
        fields = '__all__'


class DmDataTypeConfigSerializer(serializers.ModelSerializer):
    # 数据类型信息表 Serializer
    class Meta:
        model = models.DmDataTypeConfig
        fields = ('data_type_name', 'data_type_alias')


class BindDatasetStandardSerializer(serializers.Serializer):
    data_set_id = serializers.CharField(required=True)
    data_set_type = serializers.ChoiceField(required=True, allow_blank=False, choices=DataSet.VALID_DATA_SET_TYPES)
    standard_id = serializers.IntegerField(required=True)
    standard_type = serializers.ChoiceField(
        required=True, allow_blank=False, choices=[item.value for item in StandardType]
    )


class StandardPublicityListSerializer(serializers.Serializer):
    """
    标准公示Serializer
    """

    tag_ids = serializers.ListField(required=True)
    keyword = serializers.CharField(required=True, allow_blank=True)
    page = serializers.IntegerField(required=True)
    page_size = serializers.IntegerField(required=True)
    order_linked_data_count = serializers.CharField(required=False, allow_null=True)


class StandardPublicityInfoSerializer(serializers.Serializer):
    """
    标准公示Serializer
    """

    online = serializers.ChoiceField(required=True, choices=(('true', 'True'), ('false', 'False')))
    dm_standard_config_id = serializers.IntegerField(required=True)
