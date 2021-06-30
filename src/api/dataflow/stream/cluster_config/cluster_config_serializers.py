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

from common.exceptions import ValidationError
from django.utils.translation import ugettext as _
from rest_framework import serializers


class CreateStormCluster(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "component_type": "storm"
    }
    """

    component_type = serializers.CharField(label=_("component_type"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    cluster_group = serializers.CharField(required=True, label=_("集群组信息"))
    cluster_name = serializers.RegexField(
        required=True,
        regex=r"^[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*$",
        label=_("集群名称"),
        error_messages={"invalid": _("由英文字母、数字、下划线、小数点组成，且需以字母开头")},
    )
    cluster_type = serializers.CharField(required=False, label=_("cluster_type"))

    def validate(self, attrs):
        if attrs.get("component_type", "") == "flink":
            if attrs.get("cluster_type", "yarn-session") not in [
                "yarn-session",
                "yarn-cluster",
            ]:
                raise ValidationError(_('当component_type是flink时，cluster_type 只能是 "yarn-cluster" 或 "yarn-session".'))
        return attrs


class UpdateStormCluster(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "component_type": "storm"
    }
    """

    component_type = serializers.CharField(label=_("component_type"))


class ElectionStormCluster(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "cluster_group": "xxx"
    }
    """

    cluster_group = serializers.CharField(label="集群组")


class DestroyStormCluster(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    component_type = serializers.CharField(label=_("component_type"))


class ElectionFlinkCluster(serializers.Serializer):
    cluster_group = serializers.CharField(required=False, label="集群组")


class ListFlinkCluster(serializers.Serializer):
    related = serializers.CharField(required=False, label="related")


class BlacklistFlinkCluster(serializers.Serializer):
    geog_area_code = serializers.CharField(label=_("geog_area_code"), required=True)
    cluster_name = serializers.RegexField(
        required=True,
        regex=r"^[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*$",
        label=_("集群名称"),
        error_messages={"invalid": _("由英文字母、数字、下划线、小数点组成，且需以字母开头")},
    )


class ListSessionCluster(serializers.Serializer):
    session_cluster = serializers.ListField(required=False, label="session集群")
    geog_area_code = serializers.CharField(required=False, label=_("地域标签信息"))
    concurrency = serializers.IntegerField(required=False, label=_("恢复session的并发"))
