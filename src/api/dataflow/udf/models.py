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

from common.transaction import meta_sync_register
from django.db import models


class BksqlFunctionConfigV3(models.Model):
    FUNC_GROUP_CHOICE = (
        ("Mathematical Function", "Mathematical Function"),
        ("String Function", "String Function"),
        ("Conditional Function", "Conditional Function"),
        ("Aggregate Function", "Aggregate Function"),
        ("Type Conversion Function", "Type Conversion Function"),
        ("Date and Time Function", "Date and Time Function"),
        ("Table-valued Function", "Table-valued Function"),
        ("Window Function", "Window Function"),
        ("Other Function", "Other Function"),
    )

    FUNC_TYPE_CHOICE = (
        ("Internal Function", "Internal Function"),
        ("User-defined Function", "User-defined Function"),
    )

    func_name = models.CharField(primary_key=True, max_length=255)
    func_alias = models.CharField(max_length=255)
    func_group = models.CharField(max_length=1, choices=FUNC_GROUP_CHOICE)
    explain = models.TextField(blank=True, null=True)
    func_type = models.CharField(max_length=1, choices=FUNC_TYPE_CHOICE)
    func_udf_type = models.CharField(max_length=255, blank=True, null=True)
    func_language = models.CharField(max_length=255, blank=True, null=True)
    example = models.TextField(blank=True, null=True)
    example_return_value = models.TextField(blank=True, null=True)
    version = models.CharField(max_length=255, blank=True, null=True)
    support_framework = models.CharField(max_length=255)
    active = models.IntegerField(blank=True, null=True, default=1)
    created_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_config_v3"


class BksqlFunctionDevConfig(models.Model):
    id = models.AutoField(primary_key=True)
    func_name = models.CharField(max_length=255)
    version = models.CharField(max_length=255)
    func_alias = models.CharField(max_length=255, blank=True, null=True)
    func_language = models.CharField(max_length=255, blank=True, null=True)
    func_udf_type = models.CharField(max_length=255, blank=True, null=True)
    input_type = models.TextField(blank=True, null=True)
    return_type = models.TextField(blank=True, null=True)
    explain = models.TextField(blank=True, null=True)
    example = models.TextField(blank=True, null=True)
    example_return_value = models.TextField(blank=True, null=True)
    code_config = models.TextField(blank=True, null=True)
    sandbox_name = models.CharField(max_length=255, default="default")
    released = models.IntegerField(default=0)
    locked = models.IntegerField(default=0)
    locked_by = models.CharField(max_length=50, default="")
    locked_at = models.DateTimeField(blank=True, null=True, default=None)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    debug_id = models.CharField(max_length=255, blank=True, null=True)
    support_framework = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_dev_config"


class BksqlFunctionSandboxConfig(models.Model):
    sandbox_name = models.CharField(max_length=255, primary_key=True)
    java_security_policy = models.TextField(blank=True, null=True, default=None)
    python_disable_imports = models.TextField(blank=True, null=True, default=None)
    python_install_packages = models.TextField(blank=True, null=True, default=None)
    description = models.TextField(blank=True, null=True, default=None)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_sandbox_config"


class BksqlFunctionPatameterConfig(models.Model):
    id = models.AutoField(primary_key=True)
    func_name = models.CharField(max_length=255)
    input_type = models.TextField(blank=True, null=True)
    return_type = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_patameter_config"


class BksqlFunctionUpdateLog(models.Model):
    id = models.AutoField(primary_key=True)
    func_name = models.CharField(max_length=255)
    version = models.CharField(max_length=255)
    release_log = models.TextField()
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField()
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_update_log"


class BksqlFunctionDebugLog(models.Model):
    debug_id = models.CharField(max_length=255, primary_key=True)
    sql = models.TextField()
    source_result_table_id = models.CharField(max_length=255)
    debug_data = models.TextField()
    created_by = models.CharField(max_length=50, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.udf"
        db_table = "bksql_function_debug_log"


# 注册元数据 SyncHook
meta_sync_register(BksqlFunctionDevConfig)
