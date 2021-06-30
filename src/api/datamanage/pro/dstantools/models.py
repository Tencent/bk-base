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
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin sqlcustom [app_label]'
# into your database.


from django.db import models
from common.transaction import meta_sync_register


class DmTaskConfig(models.Model):
    id = models.AutoField(primary_key=True)
    task_name = models.CharField(max_length=128)
    project_id = models.IntegerField()
    standard_version_id = models.IntegerField()
    description = models.TextField(blank=True, null=True)
    data_set_type = models.CharField(max_length=128)
    data_set_id = models.CharField(max_length=128, blank=True, null=True)
    standardization_type = models.IntegerField()
    flow_id = models.IntegerField(blank=True, null=True)
    task_status = models.CharField(max_length=128)
    edit_status = models.CharField(max_length=50)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dm_task_config'
        app_label = 'dstan'


class DmTaskContentConfig(models.Model):
    id = models.AutoField(primary_key=True)
    task_id = models.IntegerField()
    parent_id = models.CharField(max_length=256)
    standard_version_id = models.IntegerField()
    standard_content_id = models.IntegerField(default=0)
    source_type = models.CharField(max_length=128)
    result_table_id = models.CharField(max_length=128)
    result_table_name = models.CharField(max_length=128)
    task_content_name = models.CharField(max_length=128)
    task_type = models.CharField(max_length=128)
    task_content_sql = models.TextField(blank=True, null=True)
    node_config = models.TextField(blank=True, null=True)
    request_body = models.TextField(blank=True, null=True)
    flow_body = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dm_task_content_config'
        app_label = 'dstan'


class DmTaskDetaildataFieldConfig(models.Model):
    task_content_id = models.IntegerField()
    field_name = models.CharField(max_length=128)
    field_alias = models.CharField(max_length=128, blank=True, null=True)
    field_type = models.CharField(max_length=128)
    field_index = models.IntegerField()
    compute_model = models.TextField()
    unit = models.CharField(max_length=128, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dm_task_detaildata_field_config'
        app_label = 'dstan'


class DmTaskIndicatorFieldConfig(models.Model):
    task_content_id = models.IntegerField()
    field_name = models.CharField(max_length=128)
    field_alias = models.CharField(max_length=128, blank=True, null=True)
    field_type = models.CharField(max_length=128)
    is_dimension = models.IntegerField(blank=True, null=True)
    add_type = models.CharField(max_length=128)
    unit = models.CharField(max_length=128, blank=True, null=True)
    field_index = models.IntegerField()
    compute_model = models.TextField()
    description = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dm_task_indicator_field_config'
        app_label = 'dstan'


class DmSchemaTmpConfig(models.Model):
    bk_table_id = models.CharField(primary_key=True, max_length=128)
    schema = models.TextField()
    bk_sql = models.TextField()
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'dm_schema_tmp_config'
        app_label = 'dstan'


class DmTaskDetail(models.Model):
    id = models.AutoField(primary_key=True)
    task_id = models.IntegerField()
    task_content_id = models.IntegerField()
    standard_version_id = models.IntegerField()
    standard_content_id = models.IntegerField(default=0)
    bk_biz_id = models.IntegerField()
    project_id = models.IntegerField()
    data_set_type = models.CharField(max_length=128)
    data_set_id = models.CharField(max_length=128)
    task_type = models.CharField(max_length=128)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dm_task_detail'
        app_label = 'dstan'


class Test(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'test'
        app_label = 'dstan'


meta_sync_register(DmTaskConfig)
meta_sync_register(DmTaskContentConfig)
meta_sync_register(DmTaskDetail)
meta_sync_register(DmTaskDetaildataFieldConfig)
meta_sync_register(DmTaskIndicatorFieldConfig)
