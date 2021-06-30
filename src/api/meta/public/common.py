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


import re

from common.bklanguage import bktranslates
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers


class FieldSerializerSupport(object):
    RESERVED_WORDS = [
        "__time",
        "dteventtimestamp",
        "dteventtime",
        "localtime",
        "thedate",
        "now",
        "dt_year",
        "dt_month",
        "dt_day",
        "dt_hour",
    ]
    pattern_1 = re.compile(r"^minute(\d)+")
    pattern_2 = re.compile(r"^\$f\d+$")
    pattern_3 = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    field_index = serializers.IntegerField(required=False, label=_("序号"))
    field_name = serializers.CharField(max_length=255, label=_("字段名"))

    def run_field_name_validation(self, value, enable_reserved_words=False):
        value_to_validate = value.lower()
        if not enable_reserved_words and value_to_validate in self.RESERVED_WORDS:
            raise serializers.ValidationError(_("字段{}在黑名单内。").format(value))
        elif self.pattern_1.match(value_to_validate):
            raise serializers.ValidationError(_("字段{}在黑名单内。").format(value))
        if self.pattern_3.match(value_to_validate) or self.pattern_2.match(value_to_validate):
            return value
        else:
            raise serializers.ValidationError(_("字段{}不合规。").format(repr(value)))


def translate_project_name(item):
    if "project_name" in item:
        item["project_name"] = bktranslates(item["project_name"])


field_allowed_roles = {"event_time"}
