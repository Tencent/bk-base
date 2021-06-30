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

from __future__ import absolute_import, unicode_literals

from common.exceptions import ValidationError
from datahub.common.const import LATEST
from django.utils.translation import ugettext as _
from rest_framework import serializers


class KafkaScopeSerializer(serializers.Serializer):
    master = serializers.CharField(required=True, label=_("master地址"))
    topic = serializers.CharField(required=True, label=_("消费Topic"))
    group = serializers.CharField(required=True, label=_("消费组"))
    tasks = serializers.IntegerField(required=False, label=_("最大并发度"))
    use_sasl = serializers.BooleanField(required=False, label=_("是否加密"))
    security_protocol = serializers.CharField(required=False, allow_blank=True, label=_("security_protocol"))
    sasl_mechanism = serializers.CharField(required=False, allow_blank=True, label=_("sasl_mechanism"))
    user = serializers.CharField(required=False, allow_blank=True, label=_("用户名"))
    password = serializers.CharField(required=False, allow_blank=True, label=_("密码"))
    auto_offset_reset = serializers.CharField(required=False, label=_("auto.offse.reset"), default=LATEST)

    def validate(self, attrs):
        attrs = super(KafkaScopeSerializer, self).validate(attrs)
        use_sasl = attrs.get("use_sasl", False)
        if use_sasl:
            if not attrs.get("security_protocol"):
                raise ValidationError(message=_("security_protocol不能为空"))
            if not attrs.get("sasl_mechanism"):
                raise ValidationError(message=_("sasl_mechanism不能为空"))
            if not attrs.get("user"):
                raise ValidationError(message=_("user不能为空"))
            if not attrs.get("password"):
                raise ValidationError(message=_("password不能为空"))
        return attrs
