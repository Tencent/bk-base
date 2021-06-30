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

from datahub.access.serializers import BaseSerializer
from django.utils.translation import ugettext as _
from rest_framework import serializers

from .kafka.serializers import KafkaScopeSerializer


class QueueResourceSerializer(BaseSerializer):
    class AccessConfSerializer(serializers.Serializer):
        class ResourceScopeSerializer(serializers.Serializer):
            scope = KafkaScopeSerializer(required=True, many=True, label=_("接入对象"))
            type = serializers.CharField(required=True, label=_("所属消息队列"))

        collection_model = serializers.DictField(required=False, label=_("采集方式"))
        resource = ResourceScopeSerializer(required=True, many=False, label=_("接入信息"))
        filters = serializers.DictField(required=False, label=_("过滤条件"))
        deploy_plan_id = serializers.IntegerField(required=False, label=_("部署计划id"))

    access_conf_info = AccessConfSerializer(required=True, many=False, label=_("接入配置信息"))

    def validate(self, attrs):
        attrs = super(QueueResourceSerializer, self).validate(attrs)
        return attrs
