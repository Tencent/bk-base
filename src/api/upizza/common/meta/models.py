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

from common.base_utils import model_to_dict
from django.db import models


class MetaSyncSupport(models.Model):
    meta_sync = True

    class Meta:
        abstract = True

    def capture_values(self):
        model_dict = model_to_dict(self)
        for dict_key, dict_val in list(model_dict.items()):
            if isinstance(dict_val, (list, dict)):
                model_dict[dict_key] = json.dumps(dict_val)
        return model_dict

    @staticmethod
    def model_to_dict(instance, fields=None, exclude=None):
        """Return django model Dict, Override django model_to_dict: <foreignkey use column as key>"""
        opts = instance._meta
        data = {}
        from itertools import chain

        for f in chain(opts.concrete_fields, opts.private_fields, opts.many_to_many):
            key = f.name
            if fields and f.name not in fields:
                continue
            if exclude and f.name in exclude:
                continue
            if f.get_internal_type() == "ForeignKey":
                key = f.column
            if hasattr(f, "meta_value_from_object"):
                data[key] = f.meta_value_from_object(instance)
            else:
                data[key] = f.value_from_object(instance)
        return data


class Tag(models.Model):
    id = models.AutoField(primary_key=True)
    code = models.CharField(max_length=128, unique=True)
    alias = models.CharField(max_length=128, blank=True, null=True)
    tag_type = models.CharField(max_length=32)
    parent_id = models.IntegerField()
    seq_index = models.IntegerField(default=0)
    sync = models.IntegerField(default=0)
    kpath = models.IntegerField(default=0)
    icon = models.TextField(blank=True, null=True)
    active = models.IntegerField(default=1)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "tag"
        app_label = "tag"


class TagTarget(models.Model):
    id = models.AutoField(primary_key=True)
    target_id = models.CharField(max_length=128)
    target_type = models.CharField(max_length=64)
    tag_code = models.CharField(max_length=128)
    source_tag_code = models.CharField(max_length=128)

    probability = models.FloatField(null=True, blank=True)
    checked = models.IntegerField(default=0)
    active = models.IntegerField(default=1)
    scope = models.CharField(
        max_length=128, default="business", choices=(("field", "字段级"), ("table", "表级"), ("business", "业务级"))
    )

    description = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_by = models.CharField(max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    # 用于查询的冗余信息
    tag_type = models.CharField(max_length=32, blank=True, null=True)
    bk_biz_id = models.IntegerField(blank=True, null=True)
    project_id = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = "tag_target"
        app_label = "tag"
        unique_together = [["tag_code", "target_type", "target_id", "source_tag_code"]]


class TagMapping(models.Model):
    code = models.CharField(max_length=128, primary_key=True)
    mapped_code = models.CharField(max_length=128)

    class Meta:
        db_table = "tag_mapping"
        app_label = "tag"
