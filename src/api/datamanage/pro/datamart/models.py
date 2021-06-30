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


class SourceTagAccessScenarioConfig(models.Model):
    id = models.AutoField(primary_key=True)
    access_scenario_id = models.IntegerField()
    access_source_id = models.IntegerField()
    active = models.IntegerField(blank=True, null=True)
    seq_index = models.IntegerField()
    created_by = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=128, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = 'source_tag_access_scenario_config'
        app_label = 'dstan'


class AccessScenarioConfig(models.Model):
    id = models.AutoField(primary_key=True)
    data_scenario_name = models.CharField(max_length=128)
    data_scenario_alias = models.CharField(max_length=128)
    active = models.IntegerField(blank=True, null=True)
    type = models.CharField(max_length=12, blank=True, null=True)
    orders = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=128, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()
    attribute = models.CharField(max_length=30, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'access_scenario_config'
        app_label = 'dstan'
