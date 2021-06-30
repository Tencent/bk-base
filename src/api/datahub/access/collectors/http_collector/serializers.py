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
from datahub.access.serializers import BaseSerializer, check_perm_by_scenario
from django.utils.translation import ugettext as _
from rest_framework import serializers


class HttpResourceSerializer(BaseSerializer):
    class AccessConfSerializer(serializers.Serializer):
        class CollectionModelSerializer(serializers.Serializer):
            collection_type = serializers.ChoiceField(
                required=True,
                label=_("采集方式"),
                choices=(
                    ("pull", "拉取"),
                    ("push", "推送"),
                ),
            )
            time_format = serializers.CharField(required=False, allow_blank=True, label=_("时间格式"))
            period = serializers.IntegerField(required=True, label=_("采集周期"))
            increment_field = serializers.CharField(required=False, allow_blank=True, label=_("时间字段"))

            def validate(self, attrs):
                period = attrs["period"]
                increment_field = attrs.get("increment_field")
                time_format = attrs.get("time_format")
                if period <= 0:
                    raise ValidationError(message=_("period采集周期不小于0"))
                if period > 30 * 24 * 60:
                    raise ValidationError(message=_("period采集周期不能高于30天"))

                increment_field = increment_field.replace(",", "") if increment_field else ""
                if increment_field:
                    if not time_format:
                        raise ValidationError(message=_("increment_field时间参数存在时,time_format不能为空"))

                return attrs

        class ResourceScopeSerializer(serializers.Serializer):
            class ScopeSerializer(serializers.Serializer):
                deploy_plan_id = serializers.IntegerField(required=False, label=_("部署计划id"))
                url = serializers.CharField(required=True, label=_("接入url"))
                method = serializers.CharField(required=True, label=_("接入方法"))
                body = serializers.DictField(required=False, label=_("接入body"))

                def validate(self, attrs):
                    method = attrs["method"]
                    if method.lower() == "post":
                        body = attrs.get("body")
                        if not body:
                            raise ValidationError(message=_("请求方式为post,body必须填写"))

                    return attrs

            scope = ScopeSerializer(required=True, many=True, label=_("接入对象"))

        collection_model = CollectionModelSerializer(required=True, many=False, label=_("采集方式"))
        resource = ResourceScopeSerializer(required=True, many=False, label=_("接入信息"))
        filters = serializers.DictField(required=False, label=_("过滤条件"))

    access_conf_info = AccessConfSerializer(required=True, many=False, label=_("接入配置信息"))

    def validate(self, attrs):
        attrs = super(HttpResourceSerializer, self).validate(attrs)
        return attrs

    @classmethod
    def scenario_scope_check(cls, attrs):
        access_conf_info = attrs.get("access_conf_info", {})
        period = access_conf_info.get("collection_model", {}).get("period")
        before_time = access_conf_info.get("collection_model", {}).get("before_time")

        if before_time:
            if int(before_time) < 0:
                raise ValidationError(message=_("before_time:延时时间不能小于0"))

        if period:
            if int(period) <= 0:
                raise ValidationError(message=_("period采集周期目前只支持周期性拉取"))

            if int(period) > 30 * 24 * 60:
                raise ValidationError(message=_("period采集周期不能高于30天"))


class HTTPCheckSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("业务id"))
    bk_username = serializers.CharField(label=_("用户名"))
    url = serializers.CharField(label=_("url"))
    time_format = serializers.CharField(required=False, label=_("时间格式"))
    method = serializers.CharField(label=_("method"))
    body = serializers.CharField(required=False, label=_("body"))

    def validate(self, attrs):
        # HTTP 场景请求：biz.common_access 权限
        check_perm_by_scenario("http", attrs["bk_biz_id"], attrs["bk_username"])
        method = attrs["method"]

        if method.lower() == "post":
            body = attrs.get("body")
            if not body:
                raise ValidationError(message=_("请求方式为post,body必须填写"))

        return attrs
