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

from django.utils.translation import ugettext as _
from rest_framework import serializers


class CreateModelingJobSerializer(serializers.Serializer):
    jobserver_config = serializers.JSONField(required=False, label=_("job的server配置信息"))
    job_config = serializers.CharField(required=False, label=_("job的配置信息，包含heads,tails等"))
    processing_id = serializers.CharField(label=_("processing的ID"), required=True)
    cluster_group = serializers.CharField(label=_("计算集群"), required=True)
    project_id = serializers.IntegerField(label=_("工程id"), required=True)
    bk_username = serializers.CharField(label=_("用户名称"), required=True)
    code_version = serializers.CharField(label=_("code version"), required=False, default="0.1.0")
    deploy_mode = serializers.CharField(label=_("deploy mode"), required=False, default="k8s")
    deploy_config = serializers.JSONField(required=False, label=_("部署配置信息"))


class StartModelingJobSerializer(serializers.Serializer):
    job_id = serializers.RegexField(
        required=True,
        label=_("Job ID"),
        regex=r"^[0-9]+(_[a-zA-Z0-9]+)*$",
        max_length=255,
    )
    project_id = serializers.IntegerField(required=True, label=_("项目ID"))
    is_restart = serializers.BooleanField(required=False, label=_("是否重试"), default=False)
    bk_username = serializers.CharField(label=_("用户信息"), required=True)


class DeleteJobSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("要删除的Job Id"), required=False)
    with_data = serializers.BooleanField(required=False, label=_("是否删除数据"), default=True)


class CreateMultiJobSerializer(serializers.Serializer):
    processing_id = serializers.CharField(label=_("processing的ID"), required=True)
    jobserver_config = serializers.JSONField(label=_("job的server配置信息"), required=True)
    project_id = serializers.IntegerField(label=_("项目ID"), required=False)
    cluster_group = serializers.CharField(label=_("集群Group"), required=True)
    cluster_name = serializers.CharField(label=_("集群名称"), required=True)


class JobStatusSerializer(serializers.Serializer):
    job_id = serializers.RegexField(
        required=False, label=_("Job ID"), regex=r"^[0-9]+(_[a-zA-Z0-9]+)*$", max_length=255
    )
    start_time = serializers.CharField(required=True, label=_("Start time"))
    end_time = serializers.CharField(required=True, label=_("End time"))


class StartMultiJobSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("Job的ID"), required=True)
    is_restart = serializers.BooleanField(label=_("是否重启"), required=True)


class StopMultiJobSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("Job的ID"), required=True)


class DeleteMultiJobSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("Job的ID"), required=True)


class ParseJobResultSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务的ID"), required=True)


class SyncStatusSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务的ID"), required=True)


class ModelingJobTaskSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务ID"))


class ModelingJobSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务ID"))


class ExecuteSqlSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(required=True, label=_("项目ID"))
