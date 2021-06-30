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

ADVANCED_PARAMS = [
    'tag_ids',
    'keyword',
    'storage_type',
    'created_by',
    'created_at_start',
    'created_at_end',
    'range_score',
    'range_operate',
    'heat_score',
    'heat_operate',
    'importance_score',
    'importance_operate',
    'asset_value_operate',
    'asset_value_score',
    'assetvalue_to_cost_operate',
    'assetvalue_to_cost',
    'storage_capacity_operate',
    'storage_capacity',
]


class SearchSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=True, allow_null=True)
    project_id = serializers.IntegerField(required=True, allow_null=True)
    tag_ids = serializers.ListField(required=True)
    keyword = serializers.CharField(required=True, allow_blank=True)
    cal_type = serializers.ListField(required=False, allow_empty=True)
    platform = serializers.ChoiceField(
        required=False,
        choices=(
            ('all', _('全部')),
            ('bk_data', _('数据平台')),
            ('tdw', _('TDW平台')),
        ),
        default='all',
    )
    data_set_type = serializers.ChoiceField(
        required=False,
        choices=(
            ('all', _('全部')),
            ('result_table', _('结果数据')),
            ('raw_data', _('数据源')),
        ),
        default='all',
    )
    page = serializers.IntegerField(required=False, min_value=1, default=1)
    page_size = serializers.IntegerField(required=False, min_value=1, default=10)
    tag_code = serializers.CharField(required=False)
    me_type = serializers.CharField(required=False)
    has_standard = serializers.IntegerField(required=False)
    token_pkey = serializers.CharField(required=False, allow_null=True)


class BasicInfoSerializer(SearchSerializer):
    tag_code = serializers.CharField(required=True)
    parent_code = serializers.CharField(required=False)


class AdvancedFilterSLZ(BasicInfoSerializer):
    created_by = serializers.CharField(required=False, allow_null=True)
    created_at_start = serializers.DateTimeField(required=False, allow_null=True)
    created_at_end = serializers.DateTimeField(required=False, allow_null=True)
    storage_type = serializers.CharField(required=False, allow_null=True)
    range_operate = serializers.CharField(required=False, allow_null=True)
    range_score = serializers.FloatField(required=False, allow_null=True)
    heat_operate = serializers.CharField(required=False, allow_null=True)
    heat_score = serializers.FloatField(required=False, allow_null=True)
    importance_operate = serializers.CharField(required=False, allow_null=True)
    importance_score = serializers.FloatField(required=False, allow_null=True)
    asset_value_operate = serializers.CharField(required=False, allow_null=True)
    asset_value_score = serializers.FloatField(required=False, allow_null=True)
    assetvalue_to_cost_operate = serializers.CharField(required=False, allow_null=True)
    assetvalue_to_cost = serializers.FloatField(required=False, allow_null=True)
    storage_capacity_operate = serializers.CharField(required=False, allow_null=True)
    storage_capacity = serializers.FloatField(required=False, allow_null=True)
    top = serializers.IntegerField(required=False, allow_null=True)
    extra_retrieve = serializers.DictField(required=False)
    order_time = serializers.CharField(required=False, allow_null=True)
    order_range = serializers.CharField(required=False, allow_null=True)
    order_heat = serializers.CharField(required=False, allow_null=True)
    parent_tag_code = serializers.CharField(required=False, allow_null=True)
    level = serializers.IntegerField(required=False, allow_null=False, default=4)


class BasicListSerializer(AdvancedFilterSLZ):
    order_asset_value = serializers.CharField(required=False, allow_null=True)
    order_importance = serializers.CharField(required=False, allow_null=True)
    order_storage_capacity = serializers.CharField(required=False, allow_null=True)
    order_assetvalue_to_cost = serializers.CharField(required=False, allow_null=True)
    standard_content_id = serializers.IntegerField(required=False, allow_null=True)
    token_pkey = serializers.CharField(required=False, allow_null=True)
    token_msg = serializers.CharField(required=False, allow_null=True)
    has_advanced_params = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        advanced_param_list = [v for k, v in list(attrs.items()) if k in ADVANCED_PARAMS]
        attrs['has_advanced_params'] = any(advanced_param_list) or (len(attrs.get('cal_type', [])) > 1)
        return attrs


class DataValueSerializer(AdvancedFilterSLZ):
    bk_biz_id = serializers.IntegerField(required=False, allow_null=True)
    project_id = serializers.IntegerField(required=False, allow_null=True)
    keyword = serializers.CharField(required=False, allow_blank=True, default='')
