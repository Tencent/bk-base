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

import json
from datetime import datetime

from django.db import models
from django.utils.translation import ugettext_lazy as _


class ProcessingStreamInfo(models.Model):
    processing_id = models.CharField(primary_key=True, max_length=255)
    stream_id = models.CharField(max_length=255, blank=True, null=True)
    concurrency = models.IntegerField(blank=True, null=True)
    window = models.TextField(blank=True, null=True)
    checkpoint_type = models.CharField(max_length=255, blank=True, null=True)
    processor_type = models.CharField(max_length=255, blank=True, null=True)
    processor_logic = models.TextField(blank=True, null=True)
    component_type = models.CharField(max_length=128)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.stream"
        db_table = "processing_stream_info"


class ProcessingStreamJob(models.Model):
    stream_id = models.CharField(primary_key=True, max_length=255)
    running_version = models.CharField(max_length=64, blank=True, null=True)
    component_type = models.CharField(max_length=128)
    jobserver_config = models.TextField(blank=True, null=True)
    cluster_group = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255, blank=True, null=True)
    heads = models.TextField()
    tails = models.TextField()
    status = models.CharField(max_length=255)
    concurrency = models.IntegerField(blank=True, null=True)
    deploy_mode = models.CharField(max_length=255, blank=True, null=True)
    deploy_config = models.TextField(blank=True, null=True)
    offset = models.IntegerField(blank=True, null=True, default=0)
    implement_type = models.CharField(max_length=64, blank=True, null=True)
    programming_language = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    snapshot_config = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.stream"
        db_table = "processing_stream_job"


class ProcessingJobInfo(models.Model):
    job_id = models.CharField(primary_key=True, max_length=255)
    processing_type = models.CharField(max_length=255)
    code_version = models.CharField(max_length=64, blank=True, null=True)
    component_type = models.CharField(max_length=128)
    jobserver_config = models.CharField(max_length=255, null=True)
    cluster_group = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255, blank=True, null=True)
    job_config = models.TextField(blank=True, null=True)
    deploy_mode = models.CharField(max_length=255, blank=True, null=True)
    deploy_config = models.TextField(blank=True, null=True)
    locked = models.IntegerField(blank=True, null=True, default=0)
    implement_type = models.CharField(max_length=64, blank=True, null=True)
    programming_language = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.stream"
        db_table = "processing_job_info"


class DataflowControlList(models.Model):
    id = models.AutoField(primary_key=True)
    case = models.CharField(_("当前功能控制场景"), max_length=255)
    value = models.CharField(_("列入当前功能控制名单的值"), max_length=255)
    description = models.TextField(_("备注信息"))

    class Meta:
        app_label = "dataflow.stream"
        db_table = "dataflow_control_list"


class DataFlowYamlInfo(models.Model):
    class STATUS(object):
        NO_START = "no-start"
        RUNNING = "running"
        STARTING = "starting"
        FAILURE = "failure"
        STOPPING = "stopping"

    STATUS_CHOICES = (
        STATUS.NO_START,
        STATUS.RUNNING,
        STATUS.STARTING,
        STATUS.FAILURE,
        STATUS.STOPPING,
    )

    yaml_id = models.AutoField(primary_key=True)
    yaml_name = models.CharField("yaml name", max_length=255)
    yaml_conf = models.TextField("yaml conf", blank=True, null=True, default="{}")
    project_id = models.IntegerField("project id", max_length=11)
    job_id = models.CharField("job id", max_length=255, blank=True, null=True, default=None)
    status = models.CharField(
        "job status",
        max_length=32,
        blank=True,
        null=True,
        choices=STATUS_CHOICES,
        default=STATUS.NO_START,
    )
    is_locked = models.BooleanField("is it locked", default=False)
    active = models.BooleanField("is it active", default=True)
    created_by = models.CharField("created by", max_length=128, null=True)
    created_at = models.DateTimeField("created at", auto_now_add=True)
    locked_by = models.CharField("locked by", max_length=128, null=True)
    locked_at = models.DateTimeField("locked at", null=True)
    updated_by = models.CharField("updated by", max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField("updated at", blank=True, null=True, auto_now=True)
    description = models.TextField("job description", blank=True, null=True, default=None)

    class Meta:
        app_label = "dataflow.stream"
        db_table = "dataflow_yaml_info"


class DataFlowYamlExecuteLog(models.Model):
    class ACTION(object):
        START = "start"
        STOP = "stop"
        RESTART = "restart"

    ACTION_CHOICES = (ACTION.START, ACTION.STOP, ACTION.RESTART)

    # 作业操作启动状态
    DEPLOY_STATUS = [DataFlowYamlInfo.STATUS.STARTING, DataFlowYamlInfo.STATUS.STOPPING]
    # 作业操作结束状态
    FINAL_STATUS = [
        DataFlowYamlInfo.STATUS.NO_START,
        DataFlowYamlInfo.STATUS.RUNNING,
        DataFlowYamlInfo.STATUS.FAILURE,
    ]

    id = models.AutoField(primary_key=True)
    yaml_id = models.IntegerField("yaml id", max_length=11)
    action = models.CharField("action type", max_length=32, choices=ACTION_CHOICES)
    status = models.CharField("status", max_length=32, choices=DataFlowYamlInfo.STATUS_CHOICES)
    logs = models.TextField("log content", null=True)
    start_time = models.DateTimeField("start time", null=True)
    end_time = models.DateTimeField("end time", null=True)
    context = models.TextField("context", null=True)
    created_by = models.CharField("created by", max_length=128)
    created_at = models.DateTimeField("created at", auto_now_add=True)
    description = models.TextField("description", blank=True, null=True, default=None)

    def set_status(self, status):
        self.status = status
        if self.status in self.DEPLOY_STATUS:
            self.start_time = datetime.now()
        elif self.status in self.FINAL_STATUS:
            self.end_time = datetime.now()
        self.save(update_fields=["status", "start_time", "end_time"])

    def get_context(self):
        if self.context is None:
            return None
        _context = json.loads(self.context)
        return _context

    def add_log(self, message, level, error):
        _log = {
            "level": level,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": message,
            "error": error,
        }
        if self.logs is None:
            new_logs = [_log]
        else:
            new_logs = json.loads(self.logs)
            new_logs.append(_log)
        self.logs = json.dumps(new_logs)
        self.save(update_fields=["logs"])

    def change_log(self, message, level, error):
        _log = {
            "level": level,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": message,
            "error": error,
        }
        if self.logs is None:
            new_logs = [_log]
        else:
            new_logs = json.loads(self.logs)
            new_logs[-1] = _log
        self.logs = json.dumps(new_logs)
        self.save(update_fields=["logs"])

    class Meta:
        app_label = "dataflow.stream"
        db_table = "dataflow_yaml_execute_log"
