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


class UnifiedComputingGraphInfo(models.Model):
    graph_id = models.CharField(primary_key=True, max_length=255)
    jobserver_config = models.TextField(blank=True, null=True)
    graph_config = models.TextField(blank=True, null=True)
    job_info_list = models.TextField(blank=True, null=True)
    is_locked = models.IntegerField(blank=True, null=True, default=0)
    locked_by = models.CharField("locked by", max_length=255, null=True)
    locked_at = models.DateTimeField("locked at", null=True)
    created_by = models.CharField("created by", max_length=128, null=True)
    created_at = models.DateTimeField("created at", auto_now_add=True)
    updated_by = models.CharField("updated by", max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField("updated at", blank=True, null=True, auto_now=True)
    description = models.TextField("job description", blank=True, null=True, default=None)

    class Meta:
        app_label = "dataflow.uc"
        db_table = "unified_computing_graph_info"


class UnifiedComputingGraphJob(models.Model):
    graph_id = models.CharField(primary_key=True, max_length=255)
    jobserver_config = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=255)
    graph_config = models.TextField(blank=True, null=True)
    job_info_list = models.TextField(blank=True, null=True)
    updated_by = models.CharField("updated by", max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField("updated at", blank=True, null=True, auto_now=True)
    description = models.TextField("job description", blank=True, null=True, default=None)

    class Meta:
        app_label = "dataflow.uc"
        db_table = "unified_computing_graph_job"
