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


import datetime

import six
import django_filters.rest_framework as filters
from django_filters.constants import EMPTY_VALUES
from django_filters.fields import Lookup

from datamanage.utils.time_tools import tznow
from datamanage.lite.dmonitor.models import AlertLog, DatamonitorAlertConfig


class RecentFilter(filters.NumberFilter):
    def filter(self, qs, value):
        if isinstance(value, Lookup):
            lookup = six.text_type(value.lookup_type)
            value = value.value
        else:
            lookup = self.lookup_expr

        if not value or value in EMPTY_VALUES:
            return qs

        recent_datetime = tznow() - datetime.timedelta(seconds=int(value))

        qs = self.get_method(qs)(**{'%s__%s' % (self.name, lookup): recent_datetime})
        return qs


class AlertsFilter(filters.FilterSet):
    start_time = filters.DateTimeFilter(name='alert_time', lookup_expr='gte')
    end_time = filters.DateTimeFilter(name='alert_time', lookup_expr='lte')

    class Meta:
        model = AlertLog
        fields = ['receiver', 'notify_way', 'start_time', 'end_time']


class AlertConfigsFilter(filters.FilterSet):
    recent_updated = RecentFilter(name='updated_at', lookup_expr='gte')

    class Meta:
        model = DatamonitorAlertConfig
        fields = ['active', 'generate_type', 'recent_updated']
