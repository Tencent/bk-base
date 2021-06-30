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

import time
import json

from common.base_utils import custom_params_valid

from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _

from datamanage.pro.lifecycle.utils import timeformat_to_timestamp
from datamanage.pro.lifecycle.models_dict import DataTraceShowType


class DataSetSerializer(serializers.Serializer):
    dataset_id = serializers.CharField(required=True, label='rt_id/data_id')
    dataset_type = serializers.ChoiceField(
        choices=(
            ('result_table', _('结果表')),
            ('raw_data', _('数据源')),
            ('tdw_table', _('tdw表')),
        )
    )


class DataTraceEventOprInfo(serializers.Serializer):
    alias = serializers.CharField(label=_('父事件中文名'))
    sub_type_alias = serializers.CharField(label=_('子事件中文名'))
    jump_to = serializers.JSONField(label=_('跳转链接详情，用于拼接跳转链接'))
    desc_params = serializers.ListField(label=_('参数值'), allow_empty=False, child=serializers.JSONField())
    desc_tpl = serializers.CharField(label=_('带change_content参数的事件描述'))
    kv_tpl = serializers.CharField(label=_('参数'))
    show_type = serializers.ChoiceField(label=_('展示方式'), choices=DataTraceShowType.get_enum_value_list())


class DataTraceEventContentSerializer(serializers.Serializer):
    opr_type = serializers.CharField(label=_('父事件类型'))
    dispatch_id = serializers.CharField(label=_('调度ID'))
    opr_sub_type = serializers.CharField(label=_('子事件类型'))
    created_at = serializers.DateTimeField(label=_('时间'), required=False, default=None)
    created_by = serializers.CharField(max_length=50, label=_('用户名'))
    data_set_id = serializers.CharField(required=True, label='rt_id')
    opr_info = serializers.JSONField(label=_('事件详情'))
    description = serializers.CharField(label=_('事件描述'))

    def validate_opr_info(self, value):
        value = json.loads(value)
        value = custom_params_valid(DataTraceEventOprInfo, value)
        value = json.dumps(value)
        return value


class DataTraceEventReportSerializer(serializers.Serializer):
    contents = serializers.ListField(label=_('上报内容'), allow_empty=False, child=DataTraceEventContentSerializer())


class LifeCycleTrendSerializer(serializers.Serializer):
    frequency = serializers.CharField(required=False, label=_('频率'))
    start_time = serializers.DateTimeField(required=False, label=_('开始时间'))
    end_time = serializers.DateTimeField(required=False, label=_('结束时间'))
    dataset_type = serializers.ChoiceField(
        required=False,
        choices=(
            ('result_table', _('结果表')),
            ('raw_data', _('数据源')),
        ),
    )

    DAY = 86400
    WEEK = 604800

    def to_representation(self, instance):
        form_data = super(LifeCycleTrendSerializer, self).to_representation(instance)
        try:
            form_data['start_timestamp'] = timeformat_to_timestamp(form_data['start_time'])
            form_data['end_timestamp'] = timeformat_to_timestamp(form_data['end_time'])
        except Exception:
            form_data['start_timestamp'] = form_data['end_timestamp'] = None

        if not form_data['start_timestamp']:
            if form_data.get('frequency', '1d') == '3m':
                form_data['start_timestamp'] = time.time() - self.DAY
            else:
                form_data['start_timestamp'] = time.time() - self.WEEK
                if (form_data['start_timestamp'] - time.timezone) % 86400 != 0:
                    form_data['start_timestamp'] = (
                        form_data['start_timestamp'] - form_data['start_timestamp'] % 86400 + time.timezone
                    )

        if not form_data['end_timestamp']:
            form_data['end_timestamp'] = time.time()
        return form_data
