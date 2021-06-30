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
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers

from meta.exceptions import DataProcessingOutputConflictError
from meta.public.models import DbOperateLog, Project
from meta.public.models.data_processing import DataProcessing
from meta.public.models.data_transferring import DataTransferring
from meta.public.models.result_table import ResultTable
from meta.public.serializers.result_table import FieldSerializer, ResultTableIdMixin
from meta.utils.validation import tags_validate


class ProjectSerializer(serializers.ModelSerializer):
    bk_username = serializers.CharField(max_length=32, label=_("项目用户名"))
    tdw_app_groups = serializers.JSONField(label=_("Tdw应用组"), required=False)
    tags = serializers.ListField(label=_("标签列表"), required=False, allow_empty=False)

    def validate(self, attrs):
        tags_validate(attrs)
        for key in ["deleted_by", "deleted_at"]:
            attrs.pop(key, None)
        return attrs

    class Meta:
        model = Project
        fields = "__all__"


class ProjectUpdateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(max_length=32, label=_("项目用户名"))
    project_name = serializers.CharField(required=False, max_length=255, label=_("项目名称"))
    bk_app_code = serializers.CharField(required=False, max_length=255, label=_("项目来源"))
    description = serializers.CharField(required=False, label=_("项目描述"))
    tdw_app_groups = serializers.JSONField(label=_("Tdw应用组"), required=False)

    def validate(self, attrs):
        for key in ["active", "deleted_by", "deleted_at"]:
            attrs.pop(key, None)
        return attrs


class ResultTableOverrideSerializer(ResultTableIdMixin, serializers.ModelSerializer):
    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    result_table_name_alias = serializers.CharField(required=False, label=_("结果表别名"))
    result_table_type = serializers.CharField(required=False, allow_null=True, label=_("结果表类型"))
    sensitivity = serializers.HiddenField(required=False, label=_("敏感性"), default="private")
    generate_type = serializers.ChoiceField(
        required=False,
        label=_("生成类型"),
        choices=(
            ("user", _("用户生成")),
            ("system", _("系统生成")),
        ),
    )
    count_freq = serializers.IntegerField(required=False, label=_("统计频率"))
    description = serializers.CharField(required=False, label=_("描述"))
    fields = FieldSerializer(required=False, many=True, label=_("结果表字段"))

    class Meta:
        model = ResultTable
        exclude = ("project_id", "processing_type")


class DataDirectingSerializer(serializers.Serializer):
    data_set_type = serializers.ChoiceField(
        label=_("数据类型"),
        choices=(
            ("raw_data", _("源数据")),
            ("result_table", _("结果表")),
        ),
    )
    data_set_id = serializers.CharField(max_length=255, label=_("数据ID"))
    storage_cluster_config_id = serializers.IntegerField(allow_null=True, default=None, label=_("存储类型ID"))
    channel_cluster_config_id = serializers.IntegerField(allow_null=True, default=None, label=_("channel ID"))
    storage_type = serializers.ChoiceField(
        label=_("存储类型"),
        allow_null=True,
        default=None,
        choices=(
            ("channel", _("管道")),
            ("storage", _("存储")),
        ),
    )
    tags = serializers.ListField(label=_("标签列表"), required=False)

    def validate(self, attrs):
        if attrs["storage_type"] == "storage" and attrs["storage_cluster_config_id"] is None:
            raise ValidationError(_("落地存储集群ID为null"))
        if attrs["storage_type"] == "channel" and attrs["channel_cluster_config_id"] is None:
            raise ValidationError(_("管道存储集群ID为null"))
        if attrs["storage_cluster_config_id"] is not None and attrs["channel_cluster_config_id"] is not None:
            raise ValidationError(_("落地存储集群ID和管道存储ID只有一个允许存在有效值，另一个需为null"))
        return attrs


class DataProcessingMixinSerializer(object):
    def validate(self, attrs):
        if "inputs" in attrs:
            input_length = len(attrs["inputs"])
            if input_length < 1:
                raise ValidationError(_("数据处理至少需要有一个输入源"))
        # 多业务数据切分的时候存在多个输出
        # if output_length > 1:
        #     raise ValidationError(_(u'数据处理最多允许输出一个结果表，当前参数输出有{}个'.format(output_length)))
        if "result_tables" in attrs and "outputs" in attrs:
            result_table_ids = {item["result_table_id"] for item in attrs["result_tables"]}
            if len(result_table_ids) != len(attrs["result_tables"]):
                raise DataProcessingOutputConflictError()
            for output_config in attrs["outputs"]:
                if output_config["data_set_type"] == "raw_data":
                    raise ValidationError(_("数据处理输出不允许为raw_data类型"))
                else:
                    if output_config["data_set_id"] not in result_table_ids:
                        raise ValidationError(_("输出结果表ID({})不在result_tables参数中").format(output_config["data_set_id"]))
        return attrs


class DataProcessingSerializer(DataProcessingMixinSerializer, serializers.ModelSerializer):
    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    result_tables = serializers.ListField(label=_("结果表详情"), required=False)
    inputs = DataDirectingSerializer(required=False, many=True, label=_("数据输入"))
    outputs = DataDirectingSerializer(required=False, many=True, label=_("数据输出"))
    tags = serializers.ListField(label=_("标签列表"), required=False, allow_empty=False)

    def validate(self, attrs):
        tags_validate(attrs)
        return attrs

    class Meta:
        model = DataProcessing
        fields = "__all__"


class DataProcessingUpdateSerializer(DataProcessingMixinSerializer, serializers.Serializer):
    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))
    processing_alias = serializers.CharField(required=False, max_length=255, label=_("数据处理名称"))
    generate_type = serializers.ChoiceField(
        required=False,
        label=_("生成类型"),
        choices=(
            ("user", _("用户生成")),
            ("system", _("系统生成")),
        ),
    )
    description = serializers.CharField(required=False, label=_("备注信息"))
    result_tables = serializers.ListField(label=_("结果表详情"), required=False)
    inputs = DataDirectingSerializer(required=False, many=True, label=_("数据输入"))
    outputs = DataDirectingSerializer(required=False, many=True, label=_("数据输出"))


class SyncHookSerializer(serializers.ModelSerializer):
    class Meta:
        model = DbOperateLog
        fields = "__all__"


class SyncHookListSerializer(serializers.Serializer):
    db_operations_list = SyncHookSerializer(required=False, many=True, label=_("参数集合列表"))
    db_operate_list = SyncHookSerializer(required=False, many=True, label=_("参数集合列表-recent"))
    batch = serializers.BooleanField(required=False, default=False)
    content_mode = serializers.ChoiceField(default="", choices=("id", "content", ""))
    affect_original = serializers.BooleanField(default=False)
    affect_cold_only = serializers.BooleanField(default=False)

    def validate(self, attrs):
        if attrs.get("db_operate_list", None) is None and attrs.get("db_operations_list", None) is None:
            raise ValidationError(_("Sync content缺失"))
        elif attrs.get("db_operations_list", None) is None:
            attrs["db_operations_list"] = attrs["db_operate_list"]
        return attrs


class ImportHookSerializer(serializers.Serializer):
    db_operate_list = SyncHookSerializer(required=True, many=True, label=_("参数集合列表"))


class DestroySerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))


class DataProcessingDestroySerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))
    with_data = serializers.BooleanField(default=False, label=_("是否删除下游结果表"))


class DataProcessingBulkDestroySerializer(serializers.Serializer):
    class DPDestroyConfigSerializer(serializers.Serializer):
        processing_id = serializers.CharField(required=True, label=_("数据处理id"))
        with_data = serializers.BooleanField(default=False, label=_("是否删除下游结果表"))

    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))
    processings = DPDestroyConfigSerializer(required=True, many=True, label=_("数据处理列表"))
    with_data = serializers.BooleanField(default=False, label=_("是否删除所有下游结果表"))


class DataTransferringSerializer(serializers.ModelSerializer):
    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    inputs = DataDirectingSerializer(required=False, many=True, label=_("数据输入"))
    outputs = DataDirectingSerializer(required=False, many=True, label=_("数据输出"))
    tags = serializers.ListField(label=_("标签列表"), required=False, allow_empty=False)

    def validate(self, attrs):
        tags_validate(attrs)
        return attrs

    class Meta:
        model = DataTransferring
        fields = "__all__"


class DataTransferringUpdateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))
    transferring_alias = serializers.CharField(required=False, max_length=255, label=_("数据传输名称"))
    generate_type = serializers.ChoiceField(
        required=False,
        label=_("生成类型"),
        choices=(
            ("user", _("用户生成")),
            ("system", _("系统生成")),
        ),
    )
    description = serializers.CharField(required=False, label=_("备注信息"))
    inputs = DataDirectingSerializer(required=False, many=True, label=_("数据输入"))
    outputs = DataDirectingSerializer(required=False, many=True, label=_("数据输出"))


class DataTransferringDestroyShortcutSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=False, max_length=32, label=_("用户名"))
    result_table_id = serializers.RegexField(
        regex=r"^\d+_[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$", max_length=255, label=_("结果表标识")
    )
    storage_cluster_config_id = serializers.IntegerField(allow_null=True, default=None, label=_("存储类型ID"))
    channel_cluster_config_id = serializers.IntegerField(allow_null=True, default=None, label=_("channel ID"))
    storage_type = serializers.ChoiceField(
        label=_("存储类型"),
        allow_null=True,
        default=None,
        choices=(
            ("channel", _("管道")),
            ("storage", _("存储")),
        ),
    )


class MetaTransactionSerializer(serializers.Serializer):
    class ApiOperateSerializer(serializers.Serializer):
        operate_object = serializers.ChoiceField(
            label=_("操作对象"),
            choices=(
                ("result_table", _("结果表")),
                ("data_processing", _("数据处理")),
                ("data_transferring", _("数据传输")),
            ),
        )
        operate_type = serializers.ChoiceField(
            label=_("操作类型"),
            choices=(
                ("create", _("创建")),
                ("update", _("修改")),
                ("destroy", _("删除")),
            ),
        )
        operate_params = serializers.DictField(label=_("操作参数"))

    bk_username = serializers.CharField(max_length=32, label=_("用户名"))
    api_operate_list = ApiOperateSerializer(label=_("API操作列表"), many=True)


class DataSetUpstreamsSerializer(serializers.Serializer):
    storage_id = serializers.IntegerField(required=False, allow_null=True, label=_("存储集群ID"))
    storage_type = serializers.ChoiceField(
        required=False,
        label=_("存储类型"),
        allow_null=True,
        choices=(
            ("channel", _("管道")),
            ("storage", _("存储")),
        ),
    )


class GenealogySerializer(serializers.Serializer):
    type = serializers.ChoiceField(
        label=_("数据类型"),
        choices=(
            ("raw_data", _("数据源")),
            ("result_table", _("结果表")),
        ),
    )
    qualified_name = serializers.CharField(
        label=_("定位键"),
    )
    depth = serializers.IntegerField(label=_("深度"), max_value=50, default=3)


class EventReportSerializer(serializers.Serializer):
    """
    事件上报
    """

    event_type = serializers.CharField(required=True, label=_("事件类型"))
    event_level = serializers.ChoiceField(required=True, label=_("事件等级"), choices=("crucial", "normal", "referential"))
    refer = serializers.CharField(required=True, label=_("事件来源"))
    expire = serializers.IntegerField(default=3600)
    triggered_at = serializers.CharField(required=True, label=_("事件触发时间"))
    description = serializers.CharField(required=False, allow_blank=True, max_length=255)
    event_message = serializers.JSONField(required=True, label=_("事件本体"))
