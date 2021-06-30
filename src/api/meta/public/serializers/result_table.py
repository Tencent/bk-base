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
from django.conf import settings
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers

from meta.public.common import FieldSerializerSupport
from meta.public.models.result_table import ResultTable, ResultTableField
from meta.utils.validation import tags_validate


class FieldSerializer(FieldSerializerSupport, serializers.ModelSerializer):
    roles = serializers.DictField(required=False, default={})

    def validate_field_name(self, value):
        if self.root and not self.root.fields.get("platform", None):
            return value
        if (
            self.root
            and self.root.fields.get("platform", None)
            and (self.root.fields.get("platform").get_value(self.root.initial_data) == "tdw")
        ):
            return value
        # queryset、snapshot类型的rt允许用户创建保留字段
        if (
            self.root
            and self.root.fields.get("processing_type", None)
            and (self.root.fields.get("processing_type").get_value(self.root.initial_data) in ("queryset", "snapshot"))
        ):
            return self.run_field_name_validation(value, enable_reserved_words=True)
        return self.run_field_name_validation(value)

    class Meta:
        model = ResultTableField
        exclude = ("result_table_id",)


class ResultTableIdMixin(object):
    def validate(self, attrs):
        valid_result_table_id = "{}_{}".format(attrs["bk_biz_id"], attrs["result_table_name"])
        if valid_result_table_id != attrs["result_table_id"]:
            raise ValidationError(_("结果表ID并非由业务ID与结果表名称结合生成"))

        if settings.ENABLED_TDW:
            from meta.extend.tencent.tdw.support import TDWRTSupport

            TDWRTSupport.set_tdw_extra_info(attrs, ["is_managed", "usability"])
        return attrs

    @staticmethod
    def validate_result_table_name(value):
        if ResultTable.objects.filter(result_table_id__iexact=value).exists():
            raise ValidationError(_("结果表名称已存在"))
        return value


class ResultTableSerializer(ResultTableIdMixin, serializers.ModelSerializer):
    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    result_table_name = serializers.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$", max_length=255, label=_("结果表名称")
    )
    result_table_type = serializers.CharField(required=False, allow_null=True, label=_("结果表类型"))
    fields = FieldSerializer(required=True, many=True, label=_("结果表字段"))
    is_managed = serializers.BooleanField(default=True, label=_("是否受控"))
    platform = serializers.CharField(label=_("所属平台"), max_length=32, default="bk_data")
    generate_type = serializers.CharField(label=_("生成途径"), max_length=32, default="user")
    tags = serializers.ListField(label=_("标签列表"), required=False, allow_empty=False)
    sensitivity = serializers.ChoiceField(
        required=False, label=_("敏感性"), default="private", choices=("private", "public", "confidential", "topsecret")
    )
    processing_type = serializers.CharField(label=_("数据处理类型"), max_length=32, default="batch")
    extra = serializers.DictField(label=_("额外平台信息"), required=False)

    def validate(self, attrs):
        attrs = super(ResultTableSerializer, self).validate(attrs)
        tags_validate(attrs)
        if attrs["platform"] in ("bk_data",) and not attrs.get("fields", None):
            raise ValidationError(_("BKData平台表必填fields。"))
        result_table_name_length = 128 if attrs["generate_type"] == "system" else 50
        if len(attrs["result_table_name"]) > result_table_name_length:
            raise ValidationError(_("RT名称过长。"))

        tdw_extra = attrs.get("extra", {}).get("tdw", {})
        if tdw_extra and settings.ENABLED_TDW:
            from meta.extend.tencent.tdw.serializers import TdwTableSerializer

            child_serializer = TdwTableSerializer(data=tdw_extra)
            child_serializer.is_valid(raise_exception=True)
            attrs["extra"]["tdw"] = child_serializer.validated_data
        return attrs

    class Meta:
        model = ResultTable
        fields = "__all__"


class ResultTableUpdateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    result_table_name_alias = serializers.CharField(required=False, label=_("别名"))
    sensitivity = serializers.ChoiceField(
        required=False, label=_("敏感性"), default="private", choices=("private", "public", "confidential", "topsecret")
    )
    generate_type = serializers.ChoiceField(
        required=False,
        label=_("生成类型"),
        choices=(
            ("user", _("用户生成")),
            ("system", _("系统生成")),
        ),
    )
    count_freq = serializers.IntegerField(required=False, label=_("统计频率"))
    count_freq_unit = serializers.CharField(
        max_length=32,
        required=False,
    )
    description = serializers.CharField(required=False, label=_("结果表描述"))
    fields = FieldSerializer(required=False, many=True, label=_("结果表字段"))
    is_managed = serializers.BooleanField(label=_("是否受控"), required=False)
    platform = serializers.CharField(label=_("所属平台"), max_length=32, required=False)
    extra = serializers.DictField(label=_("额外平台信息"), required=False)

    def validate(self, attrs):

        tdw_extra = attrs.get("extra", {}).get("tdw", {})
        if tdw_extra and settings.ENABLED_TDW:
            from meta.extend.tencent.tdw.serializers import TdwTableUpdateSerializer

            child_serializer = TdwTableUpdateSerializer(data=tdw_extra)
            child_serializer.is_valid(raise_exception=True)
            attrs["extra"]["tdw"] = child_serializer.validated_data
        return attrs


class ResultTableScenesSerializer(serializers.Serializer):
    erp = serializers.CharField(label=_("erp查询条件"))
    scene_name = serializers.CharField(max_length=16, label=_("场景名称"))
    token = serializers.CharField(max_length=32, label=_("场景token"))
