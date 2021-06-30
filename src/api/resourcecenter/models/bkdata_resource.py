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
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from common.transaction import meta_sync_register


class ResourceClusterConfig(models.Model):
    cluster_id = models.CharField(primary_key=True, max_length=64)
    cluster_type = models.CharField(max_length=45)
    cluster_name = models.CharField(max_length=45)
    component_type = models.CharField(max_length=45)
    geog_area_code = models.CharField(max_length=45)
    resource_group_id = models.CharField(max_length=45)
    resource_type = models.CharField(max_length=45)
    service_type = models.CharField(max_length=45)
    src_cluster_id = models.CharField(max_length=128)
    active = models.CharField(max_length=45, blank=True, null=True)
    splitable = models.CharField(max_length=45, blank=True, null=True)
    cpu = models.FloatField(blank=True, null=True)
    memory = models.FloatField(blank=True, null=True)
    gpu = models.FloatField(blank=True, null=True)
    disk = models.FloatField(blank=True, null=True)
    net = models.FloatField(blank=True, null=True)
    slot = models.FloatField(blank=True, null=True)
    available_cpu = models.FloatField(blank=True, null=True)
    available_memory = models.FloatField(blank=True, null=True)
    available_gpu = models.FloatField(blank=True, null=True)
    available_disk = models.FloatField(blank=True, null=True)
    available_net = models.FloatField(blank=True, null=True)
    available_slot = models.FloatField(blank=True, null=True)
    connection_info = models.TextField(blank=True, null=True)
    extra_info = models.TextField(blank=True, null=True)
    priority = models.IntegerField(null=True)
    belongs_to = models.CharField(max_length=64, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_cluster_config"
        app_label = "resourcecenter"


class ResourceGroupInfo(models.Model):
    resource_group_id = models.CharField(primary_key=True, max_length=45)
    group_name = models.CharField(max_length=255)
    group_type = models.CharField(max_length=45)
    bk_biz_id = models.IntegerField()
    status = models.CharField(max_length=45)
    process_id = models.CharField(max_length=45)
    description = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_group_info"
        app_label = "resourcecenter"


class ResourceGeogAreaClusterGroup(models.Model):
    resource_group_id = models.CharField(max_length=45)
    geog_area_code = models.CharField(primary_key=True, max_length=45)
    cluster_group = models.CharField(unique=True, max_length=45)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_geog_area_cluster_group"
        unique_together = (("geog_area_code", "resource_group_id"),)
        app_label = "resourcecenter"


class ResourceServiceConfig(models.Model):
    resource_type = models.CharField(primary_key=True, max_length=45)
    service_type = models.CharField(max_length=45)
    service_name = models.CharField(max_length=128)
    active = models.IntegerField()
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "resource_service_config"
        unique_together = (("resource_type", "service_type"),)
        app_label = "resourcecenter"


class ResourceUnitConfig(models.Model):
    resource_unit_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=45)
    resource_type = models.CharField(max_length=45)
    service_type = models.CharField(max_length=45)
    cpu = models.FloatField(blank=True, null=True)
    memory = models.FloatField(blank=True, null=True)
    gpu = models.FloatField(blank=True, null=True)
    disk = models.FloatField(blank=True, null=True)
    net = models.FloatField(blank=True, null=True)
    slot = models.FloatField(blank=True, null=True)
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_unit_config"
        app_label = "resourcecenter"


class ResourcesGroupCapacity(models.Model):
    resource_group_id = models.CharField(primary_key=True, max_length=45)
    geog_area_code = models.CharField(max_length=45)
    resource_type = models.CharField(max_length=45)
    service_type = models.CharField(max_length=45)
    cluster_id = models.CharField(max_length=64)
    cpu = models.FloatField(blank=True, null=True)
    memory = models.FloatField(blank=True, null=True)
    gpu = models.FloatField(blank=True, null=True)
    disk = models.FloatField(blank=True, null=True)
    net = models.FloatField(blank=True, null=True)
    slot = models.FloatField(blank=True, null=True)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_group_capacity"
        unique_together = (("resource_group_id", "geog_area_code", "resource_type", "service_type", "cluster_id"),)
        app_label = "resourcecenter"


class ResourcesGroupCapacityApplyForm(models.Model):
    apply_id = models.AutoField(primary_key=True)
    apply_type = models.CharField(max_length=45)
    resource_group_id = models.CharField(max_length=45)
    geog_area_code = models.CharField(max_length=45)
    resource_type = models.CharField(max_length=45)
    service_type = models.CharField(max_length=45)
    resource_unit_id = models.IntegerField()
    num = models.IntegerField()
    operate_result = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=45)
    process_id = models.CharField(max_length=45)
    description = models.TextField(blank=True, null=True)
    cluster_id = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=45, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=45, blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_group_capacity_apply_form"
        app_label = "resourcecenter"


class ResourceJobSubmitLog(models.Model):
    submit_id = models.BigAutoField(primary_key=True)
    job_id = models.CharField(max_length=255)
    resource_group_id = models.CharField(max_length=45)
    geog_area_code = models.CharField(max_length=45)
    status = models.CharField(max_length=100, blank=True, null=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        db_table = "resource_job_submit_log"
        app_label = "resourcecenter"


class ResourceJobSubmitInstanceLog(models.Model):
    submit_id = models.BigIntegerField(primary_key=True)
    cluster_id = models.CharField(max_length=128)
    cluster_name = models.CharField(max_length=45, blank=True, null=True)
    resource_type = models.CharField(max_length=45)
    cluster_type = models.CharField(max_length=45)
    inst_id = models.CharField(max_length=128)

    class Meta:
        db_table = "resource_job_submit_instance_log"
        unique_together = (("submit_id", "cluster_id"),)
        app_label = "resourcecenter"


# 元数据系统间同步
meta_sync_register(ResourceGroupInfo)
