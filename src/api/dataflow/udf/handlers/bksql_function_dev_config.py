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

from common.transaction import auto_meta_sync
from django.db.models import IntegerField, Max, Q
from django.db.models.functions import Cast, Substr

from dataflow.udf.models import BksqlFunctionDevConfig


@auto_meta_sync(using="default")
def save(**kwargs):
    function_log = BksqlFunctionDevConfig(**kwargs)
    function_log.save()


@auto_meta_sync(using="default")
def update_version_config(func_name, version, **kwargs):
    rows = BksqlFunctionDevConfig.objects.filter(func_name=func_name, version=version)
    for row in rows:
        row.__dict__.update(**kwargs)
        row.save()


def get(**kwargs):
    return BksqlFunctionDevConfig.objects.get(**kwargs)


def exists_no_dev_version(version, func_name):
    criterion1 = ~Q(version=version)
    criterion2 = Q(func_name=func_name)
    return BksqlFunctionDevConfig.objects.filter(criterion1 & criterion2).exists()


def exists_dev_version(version, func_name):
    criterion1 = Q(version=version)
    criterion2 = Q(func_name=func_name)
    return BksqlFunctionDevConfig.objects.filter(criterion1 & criterion2).exists()


def filter(**kwargs):
    return BksqlFunctionDevConfig.objects.filter(**kwargs)


def filter_criterion(criterion, **kwargs):
    return BksqlFunctionDevConfig.objects.filter(criterion, **kwargs)


@auto_meta_sync(using="default")
def delete(**kwargs):
    BksqlFunctionDevConfig.objects.filter(**kwargs).delete()


def get_max_version(func_name):
    criterion1 = ~Q(version="dev")
    criterion2 = Q(func_name=func_name)
    return (
        BksqlFunctionDevConfig.objects.filter(criterion1 & criterion2)
        .annotate(version_num=Cast(Substr("version", 2), IntegerField()))
        .aggregate(max_version=Max("version_num"))
    )
