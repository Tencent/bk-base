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
import django_filters
from django.db.models import fields

COMMON_CMP_LOOKUPS = ["gt", "gte", "lt", "lte"]
COMMON_LIKE_LOOKUPS = ["contains", "startswith", "endswith"]
COMMON_DATE_LOOKUPS = ["year", "month", "day"]
COMMON_TIME_LOOKUPS = ["hour", "minute", "second"]

NUMBER_LOOKUPS = COMMON_CMP_LOOKUPS + ["in"]
DATE_LOOKUPS = COMMON_CMP_LOOKUPS + COMMON_DATE_LOOKUPS + ["range"]
TIME_LOOKUPS = COMMON_CMP_LOOKUPS + COMMON_TIME_LOOKUPS + ["range"]
DATETIME_LOOKUPS = DATE_LOOKUPS + COMMON_TIME_LOOKUPS

ADVANCED_LOOKUPS = {
    fields.AutoField: NUMBER_LOOKUPS,
    fields.BigIntegerField: NUMBER_LOOKUPS,
    fields.CharField: COMMON_LIKE_LOOKUPS + ["in"],
    fields.DateField: DATE_LOOKUPS,
    fields.DateTimeField: DATETIME_LOOKUPS,
    fields.DecimalField: NUMBER_LOOKUPS,
    fields.EmailField: COMMON_LIKE_LOOKUPS,
    fields.FilePathField: COMMON_LIKE_LOOKUPS,
    fields.FloatField: NUMBER_LOOKUPS,
    fields.IntegerField: NUMBER_LOOKUPS,
    fields.IPAddressField: COMMON_LIKE_LOOKUPS + ["in"],
    fields.GenericIPAddressField: COMMON_LIKE_LOOKUPS + ["in"],
    fields.PositiveIntegerField: NUMBER_LOOKUPS,
    fields.PositiveSmallIntegerField: NUMBER_LOOKUPS,
    fields.SlugField: COMMON_LIKE_LOOKUPS + ["in"],
    fields.SmallIntegerField: NUMBER_LOOKUPS,
    fields.TextField: COMMON_LIKE_LOOKUPS,
    fields.TimeField: TIME_LOOKUPS,
    fields.URLField: COMMON_LIKE_LOOKUPS,
    fields.UUIDField: ["in"],
}

IMPLICIT_LOOKUPS = ["exact", "isnull"]

EXCLUDE_FIELDS = ["app_code", "app_secret", "username", "bk_token", "operator"]
INCLUDE_FIELDS = []


def get_lookups(model, include_fields=None, exclude_fields=None):
    result = {}
    if exclude_fields:
        exclude_fields.extend(EXCLUDE_FIELDS)
    else:
        exclude_fields = EXCLUDE_FIELDS
    if include_fields:
        include_fields.extend(INCLUDE_FIELDS)
    else:
        include_fields = INCLUDE_FIELDS

    for field in model._meta.get_fields():
        if field.is_relation:
            continue
        if include_fields and field.attname not in include_fields:
            # 指定了需过滤的字段（白名单）
            continue
        if exclude_fields and field.attname in exclude_fields:
            # 指定了无需过滤的字段（黑名单）
            continue

        lookups = []
        lookups.extend(IMPLICIT_LOOKUPS)
        lookups.extend(ADVANCED_LOOKUPS.get(field.__class__, []))
        result[field.attname] = set(lookups)

    return result


def get_filterset(filter_model, include_fields=None, exclude_fields=None):
    class Meta:
        model = filter_model
        fields = get_lookups(model, include_fields, exclude_fields)

    cls_name = "%sFilterSet" % filter_model.__name__
    return type(
        cls_name,
        (django_filters.FilterSet,),
        {
            "Meta": Meta,
        },
    )
