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

from django.db import models


class DatalabNotebookTaskModel(models.Model):
    notebook_id = models.AutoField(primary_key=True)
    notebook_name = models.CharField(max_length=256)
    project_id = models.IntegerField(max_length=7)
    project_type = models.CharField(max_length=128)
    notebook_url = models.CharField(max_length=1000, default="")
    content_name = models.CharField(max_length=256)
    kernel_type = models.CharField(max_length=128, default="Python3")
    lock_user = models.CharField(max_length=128, default="")
    lock_time = models.DateTimeField(blank=True, null=True)
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_notebook_task_info"
        app_label = "notebooks"


class DatalabNotebookOutputModel(models.Model):
    id = models.AutoField(primary_key=True)
    notebook_id = models.IntegerField(max_length=11)
    cell_id = models.CharField(max_length=64)
    output_type = models.CharField(max_length=64, default="")
    output_foreign_id = models.CharField(max_length=64, default="")
    sql = models.TextField(default="")
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_notebook_output"
        app_label = "notebooks"
        unique_together = ["output_type", "output_foreign_id"]


class DatalabNotebookExecuteInfoModel(models.Model):
    id = models.AutoField(primary_key=True)
    notebook_id = models.IntegerField(max_length=11)
    cell_id = models.IntegerField(max_length=11)
    code_text = models.TextField(default="")
    stage_start_time = models.DateTimeField(blank=True, null=True)
    stage_end_time = models.DateTimeField(blank=True, null=True)
    stage_status = models.CharField(max_length=64, default="")
    error_message = models.CharField(max_length=1000, default="")
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_notebook_execute_info"
        app_label = "notebooks"


class DatalabNotebookMLSqlInfoModel(models.Model):
    id = models.AutoField(primary_key=True)
    notebook_id = models.IntegerField(max_length=11)
    kernel_id = models.CharField(max_length=128, default="")
    execute_time = models.DateTimeField(blank=True, null=True)
    sql = models.TextField(default="")
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_notebook_mlsql_info"
        app_label = "notebooks"


class DatalabNotebookReportInfoModel(models.Model):
    id = models.AutoField(primary_key=True)
    notebook = models.ForeignKey("DatalabNotebookTaskModel", on_delete=models.CASCADE, related_name="notebook_report")
    report_secret = models.CharField(max_length=128)
    report_config = models.TextField(default="")
    content_name = models.CharField(max_length=256)
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_notebook_report_info"
        app_label = "notebooks"


class JupyterHubUsersModel(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    admin = models.SmallIntegerField(max_length=1, default=0)
    created = models.DateTimeField(default=None)
    last_activity = models.DateTimeField(default=None)
    cookie_id = models.CharField(max_length=256, unique=True)
    state = models.TextField(default=None)
    encrypted_auth_state = models.BinaryField(default=None)

    class Meta:
        managed = False
        db_table = "users"
        app_label = "jupyterhub"


class JupyterHubSpawnersModel(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.IntegerField(max_length=11, default=None)
    server_id = models.IntegerField(max_length=11, default=None)
    state = models.TextField(default=None)
    name = models.CharField(max_length=256, unique=True)
    started = models.DateTimeField(default=None)
    last_activity = models.DateTimeField(default=None)
    user_options = models.TextField(default=None)

    class Meta:
        managed = False
        db_table = "spawners"
        app_label = "jupyterhub"
