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


class DatalabQueryTaskModel(models.Model):
    query_id = models.AutoField(primary_key=True)
    query_name = models.CharField(max_length=256)
    project_id = models.IntegerField(max_length=7)
    project_type = models.CharField(max_length=128)
    sql_text = models.TextField(default="")
    query_task_id = models.CharField(max_length=64, default="")
    lock_user = models.CharField(max_length=128, default="")
    lock_time = models.DateTimeField(blank=True, null=True)
    chart_config = models.TextField(default="")
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, auto_now=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_query_task_info"
        app_label = "queries"


class DatalabHistoryTaskModel(models.Model):
    id = models.AutoField(primary_key=True)
    query_id = models.IntegerField(max_length=11)
    query_task_id = models.CharField(max_length=64, default="")
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_history_task_info"
        app_label = "queries"


class DatalabBKSqlFunctionModel(models.Model):
    id = models.AutoField(primary_key=True)
    sql_type = models.CharField(max_length=256)
    func_name = models.CharField(max_length=256)
    func_alias = models.CharField(max_length=256)
    func_group = models.CharField(max_length=256)
    usage = models.CharField(max_length=256)
    params = models.CharField(max_length=256)
    explain = models.TextField(default="")
    version = models.CharField(max_length=256)
    support_framework = models.CharField(max_length=256)
    support_storage = models.CharField(max_length=256)
    active = models.IntegerField(default=1)
    example = models.TextField(default="")
    example_return_value = models.TextField(default="")
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "datalab_bksql_function_config"
        app_label = "queries"
