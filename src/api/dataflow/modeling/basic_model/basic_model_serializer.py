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

from dataflow.flow.exceptions import ValidError
from dataflow.flow.handlers.generic import ModelingModelSerializer
from dataflow.modeling.models import MLSqlModelInfo


class BasicModelSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=False, label="user name")
    model_name = serializers.ListField(required=False, label="model name")

    def validate(self, attrs):
        info = super(BasicModelSerializer, self).validate(attrs)
        if not info["bk_username"] and not info["model_name"]:
            raise ValidationError("The parameter must contain bk_username or model_name")
        return info


class CreateBasicModelSerializer(serializers.Serializer):
    model_name = serializers.RegexField(
        required=True,
        label=_("模型名称"),
        regex=r"^[0-9]+(_[a-zA-Z0-9]+)*$",
        max_length=255,
    )
    algorithm_name = serializers.CharField(required=True, label=_("算法名称"))
    project_id = serializers.IntegerField(required=True, label=_("项目ID"))
    storage_cluster_config_id = serializers.IntegerField(required=True, label=_("存储ID"))
    train_mode = serializers.CharField(required=True, label=_("训练模式"))
    model_type = serializers.CharField(required=False, label=_("模型类型"))
    status = serializers.CharField(required=True, label=_("模型类型"))
    # 自定义错误信息

    def validate_train_mode(self, train_mode):
        modes = ["supervised", "unsupervised", "half_supervised"]
        if train_mode not in modes:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(modes))
        return train_mode

    def validate_status(self, status):
        statuses = ["developing", "trained", "published"]
        if status not in statuses:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(statuses))
        return status


class UpdateBasicModelSerializer(serializers.Serializer):
    model_name = serializers.RegexField(
        required=True,
        label=_("模型名称"),
        regex=r"^[0-9]+(_[a-zA-Z0-9]+)*$",
        max_length=255,
    )
    status = serializers.CharField(required=False, label=_("模型类型"))
    active = serializers.IntegerField(required=False, label=_("是否活跃"))

    # 自定义错误信息
    def validate_active(self, active):
        modes = [0, 1]
        if active not in modes:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(modes))
        return active

    def validate_status(self, status):
        statuses = ["developing", "trained", "published"]
        if status not in statuses:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(statuses))
        return status


class ModelSerializer(ModelingModelSerializer):
    created_at = serializers.DateTimeField(label="创建时间", read_only=True, format="%Y-%m-%d %H:%M:%S")
    modified_at = serializers.DateTimeField(label="更新时间", read_only=True, format="%Y-%m-%d %H:%M:%S")

    def validate(self, data):
        return data

    class Meta:
        model = MLSqlModelInfo
        fields = "__all__"
