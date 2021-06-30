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


class JobnaviClusterConfig(models.Model):
    cluster_name = models.CharField(max_length=32)
    cluster_domain = models.TextField()
    version = models.CharField(max_length=32)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    description = models.TextField()

    class Meta:
        # managed = False
        db_table = "jobnavi_cluster_config"
        app_label = "dataflow.batch"


class ProcessingBatchInfo(models.Model):
    processing_id = models.CharField(primary_key=True, max_length=255)
    batch_id = models.CharField(max_length=255, blank=True, null=True)
    processor_type = models.CharField(max_length=255, blank=True, null=True)
    processor_logic = models.TextField(blank=True, null=True)
    count_freq = models.IntegerField()
    schedule_period = models.CharField(max_length=5)
    delay = models.IntegerField()
    submit_args = models.TextField()
    component_type = models.CharField(max_length=128)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True, auto_now_add=True)
    description = models.TextField()

    class Meta:
        # managed = False
        db_table = "processing_batch_info"
        app_label = "dataflow.batch"


class ProcessingBatchJob(models.Model):
    batch_id = models.CharField(primary_key=True, max_length=255)
    processor_type = models.CharField(max_length=255, blank=True, null=True)
    processor_logic = models.TextField(blank=True, null=True)
    schedule_time = models.BigIntegerField(blank=True, null=True)
    schedule_period = models.CharField(max_length=5)
    count_freq = models.IntegerField()
    delay = models.IntegerField()
    submit_args = models.TextField()
    storage_args = models.TextField()
    running_version = models.CharField(max_length=64, blank=True, null=True)
    jobserver_config = models.CharField(max_length=255, null=True)
    cluster_group = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255)
    deploy_mode = models.CharField(max_length=255)
    deploy_config = models.TextField()
    implement_type = models.CharField(max_length=64, blank=True, null=True)
    programming_language = models.CharField(max_length=64, blank=True, null=True)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    description = models.TextField()

    class Meta:
        # managed = False
        db_table = "processing_batch_job"
        app_label = "dataflow.batch"


class ProcessingJobInfo(models.Model):
    job_id = models.CharField(primary_key=True, max_length=255)
    processing_type = models.CharField(max_length=255, null=True)
    code_version = models.CharField(max_length=64, blank=True, null=True)
    component_type = models.CharField(max_length=255, null=True)
    jobserver_config = models.CharField(max_length=255, null=True)
    cluster_group = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255)
    job_config = models.TextField()
    deploy_mode = models.CharField(max_length=255)
    deploy_config = models.TextField()
    implement_type = models.CharField(max_length=64, blank=True, null=True)
    programming_language = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True, auto_now_add=True)
    description = models.TextField()

    class Meta:
        # managed = False
        db_table = "processing_job_info"
        app_label = "dataflow.batch"


class ProcessingVersionConfig(models.Model):
    componet_type = models.CharField(max_length=32)
    branch = models.CharField(max_length=32)
    version = models.CharField(max_length=32)
    description = models.TextField(blank=True, null=True)

    class Meta:
        # managed = False
        db_table = "processing_version_config"
        app_label = "dataflow.batch"


class StorageHdfsExportRetry(models.Model):
    result_table_id = models.CharField(max_length=255)
    schedule_time = models.BigIntegerField(blank=True, null=True)
    data_dir = models.TextField(blank=True, null=True)
    retry_times = models.IntegerField(default=0)
    batch_storage_type = models.TextField(default="HDFS")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # managed = False
        db_table = "storage_hdfs_export_retry"
        app_label = "dataflow.batch"


class StorageHdfsPathStatus(models.Model):
    result_table_id = models.CharField(max_length=255)
    path_time = models.CharField(max_length=11)
    data_time = models.DateTimeField()
    write_time = models.DateTimeField()
    is_rerun = models.IntegerField()
    path = models.TextField()
    partition_num = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, null=True, auto_now_add=True)

    class Meta:
        # managed = False
        db_table = "storage_hdfs_path_status"
        app_label = "dataflow.batch"


class DataflowControlList(models.Model):
    id = models.AutoField(primary_key=True)
    case = models.CharField(max_length=255)
    value = models.CharField(max_length=255)
    description = models.TextField()

    class Meta:
        app_label = "dataflow.batch"
        db_table = "dataflow_control_list"


class BatchOneTimeExecuteJob(models.Model):
    job_id = models.CharField(primary_key=True, max_length=255)
    job_type = models.CharField(max_length=255)
    execute_id = models.IntegerField(blank=True, null=True)
    jobserver_config = models.TextField(max_length=255, null=True)
    cluster_group = models.CharField(max_length=255)
    job_config = models.TextField(blank=True, null=True)
    deploy_config = models.TextField(blank=True, null=True)
    processing_logic = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "batch_one_time_execute_job"
