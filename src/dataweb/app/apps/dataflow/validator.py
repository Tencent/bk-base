# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


from django import forms
from django.utils.translation import ugettext_lazy as _

from apps.forms import BaseForm
from apps.utils.time_handler import get_pizza_timestamp, timeformat_to_timestamp


class DataCountForm(BaseForm):
    frequency = forms.CharField(label=_("频率"), required=False)
    start_time = forms.DateTimeField(label=_("开始时间"), required=False)
    end_time = forms.DateTimeField(label=_("结束时间"), required=False)

    DAY = 86400
    WEEK = 604800

    def clean(self):
        form_data = super(DataCountForm, self).clean()
        try:
            form_data["start_timestamp"] = timeformat_to_timestamp(form_data["start_time"])
            form_data["end_timestamp"] = timeformat_to_timestamp(form_data["end_time"])
        except Exception:
            form_data["start_timestamp"] = form_data["end_timestamp"] = None

        if not form_data["start_timestamp"]:
            if form_data["frequency"] == "3m":
                form_data["start_timestamp"] = get_pizza_timestamp() - self.DAY
            else:
                form_data["start_timestamp"] = get_pizza_timestamp() - self.WEEK

        if not form_data["end_timestamp"]:
            form_data["end_timestamp"] = get_pizza_timestamp()
        return form_data
