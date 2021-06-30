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

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError
from django.utils.translation import ugettext as _
from rest_framework import serializers

from dataflow.stream.settings import ImplementType


class ProcessingSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "static_data": [],
        "source_data": [],
        "input_result_tables": [
            "xxx_xxx"
        ],
        "sql": "select _path_, platid from xxx",
        "processing_id": "xxx_xxx",
        "outputs": [{bk_biz_id: 123,table_name: abc},{bk_biz_id:456,table_name: def}],
        "dict": {
            "window_type": "none",
            "description": "xxx",
            "count_freq": 0,
            "waiting_time": 0,
            "window_time": 0,
            "session_gap": 0
        },
        "project_id": 99999,
        "component_type": "flink"
    }
    """

    static_data = serializers.ListField(label=_("静态数据源"))
    source_data = serializers.ListField(required=False, label=_("是数据源的父节点id"))
    input_result_tables = serializers.ListField(label=_("数据源"))
    sql = serializers.CharField(required=False, label=_("sql"))
    processing_id = serializers.CharField(label=_("data processing"))
    processing_alias = serializers.CharField(required=False, label=_("processing alias"))
    project_id = serializers.IntegerField(label=_("项目"))
    component_type = serializers.CharField(label=_("组件类型"))
    implement_type = serializers.CharField(required=False, label=_("implement type"))
    tags = serializers.ListField(label=_("标签列表"), allow_empty=False)

    outputs = serializers.ListField(label=_("outputs"))
    dict = serializers.DictField(label=_("dict"))

    def validate(self, attr):
        info = super(ProcessingSerializer, self).validate(attr)
        if "dict" in info:
            if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
                dict = custom_params_valid(CodeDictSerializer, info["dict"])
            else:
                dict = custom_params_valid(SqlDictSerializer, info["dict"])
            info["dict"] = dict

        if "outputs" in info:
            if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
                outputs = custom_params_valid(CodeOutputsSerializer, info["outputs"], many=True)
            else:
                outputs = custom_params_valid(SqlOutputsSerializer, info["outputs"], many=True)
            info["outputs"] = outputs

        if "implement_type" not in info or info["implement_type"] != ImplementType.CODE.value:
            if "sql" not in info:
                raise ValidationError(_("sql不能为空"))
            if "source_data" not in info:
                raise ValidationError(_("source_data不能为空"))
        if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
            if "processing_alias" not in info:
                raise ValidationError(_("processing_alias不能为空"))
        return info


class WindowConfigSerializer(serializers.Serializer):
    class SchedulingContentSerializer(serializers.Serializer):
        class WindowLatenessSerializer(serializers.Serializer):
            allowed_lateness = serializers.BooleanField(required=False, label=_("allowed lateness"))
            lateness_count_freq = serializers.IntegerField(required=False, label=_("lateness count freq"))
            lateness_time = serializers.IntegerField(required=False, label=_("lateness time"))

        window_lateness = WindowLatenessSerializer(required=False, many=False, label=_("window lateness"))
        window_type = serializers.CharField(required=True, label=_("window type"))
        count_freq = serializers.IntegerField(required=False, label=_("count freq"))
        waiting_time = serializers.IntegerField(required=False, label=_("waiting time"))
        window_time = serializers.IntegerField(required=False, label=_("window time"))
        session_gap = serializers.IntegerField(required=False, label=_("session gap"))
        expired_time = serializers.IntegerField(required=False, label=_("expired time"))

    scheduling_content = SchedulingContentSerializer(required=True, many=False, label=_("scheduling content"))


class SqlOutputsSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("biz id"))
    table_name = serializers.CharField(label=_("表名"))


class CodeOutputsSerializer(serializers.Serializer):
    class FieldSerializer(serializers.Serializer):
        field_name = serializers.CharField(label=_("field name"))
        field_type = serializers.CharField(label=_("field type"))
        field_alias = serializers.CharField(label=_("field alias"))
        event_time = serializers.BooleanField(label=_("是否为数据时间字段"))

    bk_biz_id = serializers.IntegerField(label=_("biz id"))
    result_table_name = serializers.CharField(label=_("表名"))
    result_table_name_alias = serializers.CharField(label=_("表的别名"))
    fields = FieldSerializer(required=True, many=True, label=_("fields"))


class SqlDictSerializer(serializers.Serializer):
    window_type = serializers.CharField(label=_("窗口类型"))
    description = serializers.CharField(label=_("描述"), allow_null=True, allow_blank=True)
    count_freq = serializers.IntegerField(label=_("count freq"))
    waiting_time = serializers.IntegerField(label=_("waiting time"))
    window_time = serializers.IntegerField(label=_("window time"))
    session_gap = serializers.IntegerField(label=_("session gap"))
    allowed_lateness = serializers.BooleanField(required=False, label=_("是否处理延迟数据"))
    lateness_time = serializers.IntegerField(required=False, label=_("延迟时间"))
    lateness_count_freq = serializers.IntegerField(required=False, label=_("延迟数据统计频率"))
    # 针对数据稀疏场景下的session窗口配置
    expired_time = serializers.IntegerField(required=False, label=_("过期事件"))


class CodeDictSerializer(serializers.Serializer):
    class AdvancedSerializer(serializers.Serializer):
        use_savepoint = serializers.BooleanField(label=_("是否使用savepoint"))

    programming_language = serializers.CharField(label=_("编程语言"))
    user_args = serializers.CharField(required=True, allow_blank=True, label=_("用户参数"))
    code = serializers.CharField(required=True, label=_("code"))
    advanced = AdvancedSerializer(required=True, many=False, label=_("高级配置"))


class UpdateProcessingSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "static_data": [],
        "source_data": [],
        "input_result_tables": [
            "xxx_xxx"
        ],
        "sql": "select _path_, platid from xxx",
        "outputs": [{bk_biz_id: 123,table_name: abc},{bk_biz_id:456,table_name: def}],
        "dict": {
            "window_type": "none",
            "description": "xxx",
            "count_freq": 0,
            "waiting_time": 0,
            "window_time": 0,
            "session_gap": 0
        },
        "project_id": 99999
    }
    """

    processing_alias = serializers.CharField(required=False, label=_("processing alias"))
    static_data = serializers.ListField(label=_("静态数据源"))
    source_data = serializers.ListField(required=False, label=_("是数据源的父节点id"))
    input_result_tables = serializers.ListField(label=_("数据源"))
    sql = serializers.CharField(required=False, label=_("sql"))
    outputs = serializers.ListField(label=_("outputs"))
    dict = serializers.DictField(label=_("dict"))
    project_id = serializers.IntegerField(label=_("项目"))
    tags = serializers.ListField(label=_("标签列表"), allow_empty=False)
    implement_type = serializers.CharField(required=False, label=_("implement type"))
    delete_result_tables = serializers.ListField(required=False, label=_("delete result tables"))

    def validate(self, attr):
        info = super(UpdateProcessingSerializer, self).validate(attr)
        if "dict" in info:
            if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
                dict = custom_params_valid(CodeDictSerializer, info["dict"])
            else:
                dict = custom_params_valid(SqlDictSerializer, info["dict"])
            info["dict"] = dict

        if "outputs" in info:
            if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
                outputs = custom_params_valid(CodeOutputsSerializer, info["outputs"], many=True)
            else:
                outputs = custom_params_valid(SqlOutputsSerializer, info["outputs"], many=True)
            info["outputs"] = outputs

        if "implement_type" not in info or info["implement_type"] != ImplementType.CODE.value:
            if "sql" not in info:
                raise ValidationError(_("sql不能为空"))
            if "source_data" not in info:
                raise ValidationError(_("source_data不能为空"))
        if "implement_type" in info and info["implement_type"] == ImplementType.CODE.value:
            if "processing_alias" not in info:
                raise ValidationError(_("processing_alias不能为空"))
            if "delete_result_tables" not in info:
                raise ValidationError(_("delete_result_tables不能为空"))
        return info


class SaveYamlProcessingSerializer(serializers.Serializer):
    heads = serializers.CharField(label=_("heads"))
    tails = serializers.CharField(label=_("tails"))
    source = serializers.ListField(required=True, label=_("source"))
    project_id = serializers.IntegerField(label=_("project id"))


class ChangeComponentType(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "processings": [rt1,rt2],
        "component_type": 'flink/storm'
    }
    """

    processings = serializers.ListField(label="processings")
    component_type = serializers.CharField(label="component type")


class SingleProcessingJobSerializer(serializers.Serializer):
    class JobConfigSerializer(serializers.Serializer):
        processor_logic = serializers.DictField(label=_("processor logic"))
        heads = serializers.CharField(label=_("heads"))
        tails = serializers.CharField(label=_("tails"))
        processor_type = serializers.CharField(label=_("processor_type"))
        offset = serializers.IntegerField(label=_("offset"))

    project_id = serializers.IntegerField(label=_("项目"))
    component_type = serializers.CharField(label=_("组件类型"))
    cluster_group = serializers.CharField(label=_("集群组"))
    processings = serializers.ListField(label=_("数据处理节点"))
    tags = serializers.ListField(label=_("标签列表"), allow_empty=False)
    deploy_config = serializers.DictField(required=False, label=_("用户代码包"))
    job_config = JobConfigSerializer(required=False, label=_("用户任务配置"))


class RecoverJobSerializer(serializers.Serializer):
    job_id_list = serializers.ListField(required=False, label=_("job_id列表"))
    concurrency = serializers.IntegerField(required=False, label=_("恢复job的并发"))
