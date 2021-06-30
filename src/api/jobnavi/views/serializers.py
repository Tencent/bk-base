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
from rest_framework import serializers
from django.utils.translation import ugettext as _
from django.conf import settings
from common.exceptions import ValidationError


def tags_validate(attrs):
    if settings.MULTI_GEOG_AREA:
        tags = attrs.get("tags")
        if not tags:
            raise ValidationError(_("tags 字段必填。"))
    else:
        tags = ["inland"]
    attrs["tags"] = tags


class CommonSerializer(serializers.Serializer):
    tags = serializers.ListField(required=False, label=_("地域标签信息"))

    def validate(self, attrs):
        tags_validate(attrs)
        return attrs


# schedule view serializers
class ScheduleSerializer(CommonSerializer):
    class PeriodSerializer(serializers.Serializer):
        timezone = serializers.CharField(required=False, label=_("时区"))
        cron_expression = serializers.CharField(required=False, label=_("cron表达式"), allow_blank=True)
        frequency = serializers.IntegerField(required=False, label=_("统计频率"))
        period_unit = serializers.CharField(required=False, label=_("周期单位"), allow_blank=True)
        first_schedule_time = serializers.IntegerField(required=False, label=_("启动时间"))
        delay = serializers.CharField(required=False, label=_("延迟"), allow_blank=True)

    class ParentSerializer(serializers.Serializer):
        parent_id = serializers.CharField(required=True, label=_("父任务标识"))
        dependency_rule = serializers.CharField(required=True, label=_("依赖规则"))
        param_type = serializers.CharField(required=True, label=_("依赖值类型"))
        param_value = serializers.CharField(required=True, label=_("依赖值"))

    class RecoverySerializer(serializers.Serializer):
        enable = serializers.BooleanField(required=True, label=_("是否可恢复"))
        interval_time = serializers.CharField(required=False, label=_("间隔时间"))
        retry_times = serializers.IntegerField(required=False, label=_("重试次数"))

    schedule_id = serializers.CharField(required=True, label=_("任务标识"))
    type_id = serializers.CharField(required=True, label=_("任务类型"))
    description = serializers.CharField(required=False, label=_("任务描述"))
    period = PeriodSerializer(required=False, label=_("周期"))
    parents = ParentSerializer(required=False, many=True, label=_("依赖关系"))
    recovery = RecoverySerializer(required=False, label=_("重试策略"))
    active = serializers.BooleanField(required=False, label=_("是否可用"))
    exec_oncreate = serializers.BooleanField(required=False, label=_("创建后执行"))
    execute_before_now = serializers.BooleanField(required=False, label=_("是否可以从当前时间之前执行任务"))
    node_label = serializers.CharField(allow_null=True, required=False, label=_("节点标签"))
    decommission_timeout = serializers.CharField(allow_blank=True, required=False, label=_("退服超时时间"))
    max_running_task = serializers.IntegerField(required=False, label=_("可运行的最大任务数"))
    created_by = serializers.CharField(required=False, label=_("创建人"))


class CreateScheduleSerializer(ScheduleSerializer):
    schedule_id = serializers.CharField(required=True, label=_("任务标识"))


class UpdateScheduleSerializer(ScheduleSerializer):
    pass


class PartialUpdateScheduleSerializer(CommonSerializer):
    pass


# execute view serializers
class ListExecuteSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("任务标识"))
    schedule_time = serializers.IntegerField(required=False, label=_("任务调度时间"))


class ExecuteSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("任务标识"))
    schedule_time = serializers.IntegerField(required=False, label=_("任务调度时间"))


class QuerySubmittedExecuteByTimeSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("schedule_id"))
    start_time = serializers.IntegerField(required=True, label=_("start_time"))
    end_time = serializers.IntegerField(required=True, label=_("end_time"))


class RunSerializer(CommonSerializer):
    rerun_processings = serializers.CharField(required=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=True, label=_("rerun_model"))
    start_time = serializers.IntegerField(required=True, label=_("起始时间"))
    end_time = serializers.IntegerField(required=True, label=_("截止时间"))
    priority = serializers.CharField(required=False, allow_blank=True, label=_("任务优先度"))


class RerunSerializer(CommonSerializer):
    rerun_processings = serializers.CharField(required=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=True, label=_("rerun_model"))
    start_time = serializers.IntegerField(required=True, label=_("起始时间"))
    end_time = serializers.IntegerField(required=True, label=_("截止时间"))
    exclude_statuses = serializers.CharField(required=False, label=_("不补算指定状态的任务"))


class RedoSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("schedule_id"))
    schedule_time = serializers.IntegerField(required=True, label=_("schedule_time"))
    is_run_depend = serializers.BooleanField(required=True, label=_("是否重算子节点"))
    exclude_statuses = serializers.CharField(required=False, label=_("不重算指定状态的任务"))


# event view serializers
class EventSerializer(CommonSerializer):
    execute_id = serializers.IntegerField(required=True, label=_("任务标识"))
    event_name = serializers.CharField(required=True, label=_("事件名称"))
    change_status = serializers.CharField(required=False, label=_("任务改变状态"))
    event_info = serializers.CharField(required=False, label=_("事件信息"))


# task type view serializers
class UploadFileTaskSerializer(CommonSerializer):
    pass


class TaskTypeSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    tag = serializers.CharField(label=_("任务类型tag"))
    main = serializers.CharField(label=_("任务入口"))
    env = serializers.CharField(allow_null=True, label=_("任务环境配置"))
    sys_env = serializers.CharField(allow_null=True, label=_("任务系统环境依赖"))
    language = serializers.CharField(label=_("任务运行语言"))
    task_mode = serializers.CharField(label=_("任务运行模式"))
    recoverable = serializers.BooleanField(required=False, label=_("任务是否可自愈"))
    created_by = serializers.CharField(required=False, label=_("创建人"))
    description = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_("任务类型描述"))

    def validate_language(self, language):
        languages = ["java", "python"]
        if language not in languages:
            raise ValidationError(
                _("非法任务运行语言: %(language)s，目前仅支持以下语言[%(languages)s]")
                % {"language": language, "languages": ", ".join(languages)}
            )
        return language

    def validate_task_mode(self, task_mode):
        task_modes = ["process", "thread"]
        if task_mode not in task_modes:
            raise ValidationError(
                _("非法任务运行模式: %(task_mode)s，目前仅支持以下模式[%(task_modes)s]")
                % {"task_mode": task_mode, "task_modes": ", ".join(task_modes)}
            )
        return task_mode


class DeleteTaskTypeTagSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    tag = serializers.CharField(label=_("任务类型tag"))


class RetrieveTaskTypeTagAliasSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    tag = serializers.CharField(label=_("任务类型tag"))


class TaskTypeTagAliasSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    tag = serializers.CharField(label=_("任务类型tag"))
    alias = serializers.CharField(label=_("任务类型tag别名"))
    description = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_("任务类型tag别名描述"))


class DeleteTaskTypeTagAliasSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    tag = serializers.CharField(label=_("任务类型tag"))
    alias = serializers.CharField(label=_("任务类型tag别名"))


class RetrieveTaskTypeDefaultTagSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    node_label = serializers.CharField(required=False, label=_("Runner节点标签"))


class TaskTypeDefaultTagSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    node_label = serializers.CharField(required=False, label=_("Runner节点标签"))
    default_tag = serializers.CharField(label=_("默认任务类型tag"))


class DeleteTaskTypeDefaultTagSerializer(CommonSerializer):
    type_id = serializers.CharField(label=_("任务类型ID"))
    node_label = serializers.CharField(required=False, label=_("Runner节点标签"))


# admin schedule view serializers
class CalculateScheduleTaskTimeSerializer(CommonSerializer):
    rerun_processings = serializers.CharField(required=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=True, label=_("rerun_model"))
    start_time = serializers.IntegerField(required=True, label=_("起始时间"))
    end_time = serializers.IntegerField(required=True, label=_("截止时间"))


class QueryFailedExecutesSerializer(CommonSerializer):
    begin_time = serializers.IntegerField(required=True, label=_("起始时间"))
    end_time = serializers.IntegerField(required=True, label=_("截止时间"))
    type_id = serializers.CharField(allow_blank=True, allow_null=True, label=_("任务类型"))


# data makeup view serializers
class ListDataMakeupSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("schedule_id"))
    start_time = serializers.IntegerField(required=True, label=_("start_time"))
    end_time = serializers.IntegerField(required=True, label=_("end_time"))


class CheckDataMakeupAllowedSerializer(CommonSerializer):
    schedule_id = serializers.CharField(required=True, label=_("schedule_id"))
    schedule_time = serializers.IntegerField(required=True, label=_("schedule_time"))


class RunDataMakeupSerializer(CommonSerializer):
    processing_id = serializers.CharField(required=True, label=_("processing_id"))
    rerun_processings = serializers.CharField(required=False, allow_blank=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=True, label=_("rerun_model"))
    target_schedule_time = serializers.IntegerField(required=True, label=_("target_schedule_time"))
    source_schedule_time = serializers.IntegerField(required=True, label=_("source_schedule_time"))
    dispatch_to_storage = serializers.BooleanField(required=True, label=_("是否下发到存储"))


# task log view serializers
class RetrieveTaskLogSerializer(CommonSerializer):
    begin = serializers.IntegerField(required=True, label=_("begin"))
    end = serializers.IntegerField(required=True, label=_("end"))


# runner node label view serializers
class RunnerNodeLabelSerializer(CommonSerializer):
    runner_id = serializers.CharField(label=_("Runner节点ID"))
    node_label = serializers.CharField(label=_("Runner节点标签"))
    description = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_("Runner节点标签描述"))


class DeleteRunnerNodeLabelSerializer(CommonSerializer):
    node_label = serializers.CharField(label=_("Runner节点标签"))


class RunnerNodeLabelDefinitionSerializer(CommonSerializer):
    node_label = serializers.CharField(label=_("Runner节点标签定义名"))
    description = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_("Runner节点标签定义描述"))


class DeleteNodeLabelDefinitionSerializer(CommonSerializer):
    node_label = serializers.CharField(label=_("Runner节点标签定义名"))


# config view serializers
class ConfigSerializer(CommonSerializer):
    cluster_domain = serializers.CharField(required=True, label=_("集群连接信息"))
    version = serializers.CharField(required=True, label=_("版本"))
    created_by = serializers.CharField(required=True, label=_("创建人"))


class CreateConfigSerializer(ConfigSerializer):
    cluster_name = serializers.CharField(required=True, label=_("集群名称"))


class UpdateConfigSerializer(ConfigSerializer):
    pass
