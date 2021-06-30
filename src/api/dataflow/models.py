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

from common.base_utils import model_to_dict
from common.transaction import meta_sync_register
from django.db import models


class ProcessingClusterConfig(models.Model):
    cluster_domain = models.CharField(max_length=255, default="default")
    cluster_group = models.CharField(max_length=128)
    cluster_name = models.CharField(max_length=128)
    cluster_label = models.CharField(max_length=128)
    priority = models.IntegerField()
    version = models.CharField(max_length=128)
    belong = models.CharField(max_length=128, blank=True, null=True)
    component_type = models.CharField(max_length=128)
    geog_area_code = models.CharField(max_length=50)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()

    class Meta:
        app_label = "dataflow"
        db_table = "processing_cluster_config"


class DebugErrorLog(models.Model):
    id = models.BigIntegerField(primary_key=True)
    debug_id = models.CharField(max_length=255)
    job_id = models.CharField(max_length=255)
    job_type = models.CharField(max_length=64)
    processing_id = models.CharField(max_length=255)
    error_code = models.CharField(max_length=64, blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    error_message_en = models.TextField(blank=True, null=True)
    debug_date = models.BigIntegerField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "debug_error_log"


class DebugMetricLog(models.Model):
    id = models.BigIntegerField(primary_key=True)
    debug_id = models.CharField(max_length=255)
    job_id = models.CharField(max_length=255)
    operator = models.CharField(max_length=126)
    job_type = models.CharField(max_length=64)
    job_config = models.TextField(blank=True, null=True, default=None)
    processing_id = models.CharField(max_length=255, blank=True, null=True, default=None)
    input_total_count = models.BigIntegerField(blank=True, null=True)
    output_total_count = models.BigIntegerField(blank=True, null=True)
    filter_discard_count = models.BigIntegerField(blank=True, null=True)
    transformer_discard_count = models.BigIntegerField(blank=True, null=True)
    aggregator_discard_count = models.BigIntegerField(blank=True, null=True)
    debug_start_at = models.DateTimeField(blank=True, null=True)
    debug_end_at = models.DateTimeField(blank=True, null=True)
    debug_heads = models.CharField(max_length=255, blank=True, null=True)
    debug_tails = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "debug_metric_log"


class DebugResultDataLog(models.Model):
    id = models.BigIntegerField(primary_key=True)
    debug_id = models.CharField(max_length=255)
    job_id = models.CharField(max_length=255)
    job_type = models.CharField(max_length=64)
    processing_id = models.CharField(max_length=255)
    result_data = models.TextField(blank=True, null=True)
    debug_date = models.BigIntegerField(blank=True, null=True)
    thedate = models.IntegerField()

    class Meta:
        app_label = "dataflow"
        db_table = "debug_result_data_log"


class ProcessingVersionConfig(models.Model):
    component_type = models.CharField(max_length=32)
    branch = models.CharField(max_length=32)
    version = models.CharField(max_length=32)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "processing_version_config"


class DataflowCustomCalculateJobInfo(models.Model):
    custom_calculate_id = models.CharField(primary_key=True, max_length=255)
    custom_type = models.CharField(max_length=64)
    data_start = models.BigIntegerField(blank=True, null=True)
    data_end = models.BigIntegerField(blank=True, null=True)
    status = models.CharField(max_length=64)
    heads = models.CharField(max_length=255, blank=True, null=True)
    tails = models.CharField(max_length=255, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()
    rerun_model = models.CharField(max_length=64, blank=True, null=True)
    rerun_processings = models.TextField(blank=True, null=True)
    geog_area_code = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "dataflow_custom_calculate_job_info"


class DataflowCustomCalculateExecuteLog(models.Model):
    id = models.BigIntegerField(primary_key=True)
    custom_calculate_id = models.CharField(max_length=255)
    execute_id = models.CharField(max_length=255)
    job_id = models.CharField(max_length=255)
    schedule_time = models.BigIntegerField(blank=True, null=True)
    status = models.CharField(max_length=64)
    info = models.TextField(null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()

    class Meta:
        app_label = "dataflow"
        db_table = "dataflow_custom_calculate_execute_log"


class ParametersInfo(models.Model):
    id = models.AutoField(primary_key=True)
    key = models.CharField(max_length=255)
    value = models.TextField(blank=True, null=True)
    module = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)

    @classmethod
    def list(cls, module=None):
        """
        返回节点类型配置列表
        :return:
        """
        if module:
            params_info_objects = cls.objects.filter(module=module)
        else:
            params_info_objects = cls.objects.all()
        params_info = [model_to_dict(params) for params in params_info_objects]
        return params_info

    class Meta:
        app_label = "dataflow"
        db_table = "dataflow_parameters_info"


class ProcessingUdfInfo(models.Model):
    id = models.BigIntegerField(primary_key=True)
    processing_id = models.CharField(max_length=255)
    processing_type = models.CharField(max_length=32)
    udf_name = models.CharField(max_length=255)
    udf_info = models.TextField()
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "processing_udf_info"


class ProcessingUdfJob(models.Model):
    id = models.BigIntegerField(primary_key=True)
    job_id = models.CharField(max_length=255)
    processing_id = models.CharField(max_length=255)
    processing_type = models.CharField(max_length=32)
    udf_name = models.CharField(max_length=255)
    udf_info = models.TextField()
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "processing_udf_job"


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
        app_label = "dataflow"
        db_table = "processing_job_info"


class FlowJob(models.Model):
    id = models.IntegerField(primary_key=True)
    flow_id = models.IntegerField(blank=True, null=True)
    node_id = models.IntegerField(blank=True, null=True)
    job_id = models.CharField(max_length=255, blank=True, null=True)
    job_type = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "dataflow_job"


class FlowInfo(models.Model):
    flow_id = models.IntegerField(primary_key=True)
    flow_name = models.CharField(max_length=255)
    project_id = models.IntegerField()
    status = models.CharField(max_length=32, blank=True, null=True)
    is_locked = models.IntegerField(blank=True, null=True)
    latest_version = models.CharField(max_length=255, blank=True, null=True)
    bk_app_code = models.CharField(max_length=255, blank=True)
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    locked_by = models.CharField(max_length=255)
    locked_at = models.DateTimeField(blank=True, null=True)
    locked_description = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    tdw_conf = models.TextField(blank=True, null=True)
    custom_calculate_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        app_label = "dataflow"
        db_table = "dataflow_info"


# 元数据系统间同步
meta_sync_register(ProcessingClusterConfig)
