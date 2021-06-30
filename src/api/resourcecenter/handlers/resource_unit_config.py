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
from resourcecenter.models.bkdata_resource import ResourceUnitConfig


def save(**kwargs):
    return ResourceUnitConfig.objects.create(**kwargs)


def update(resource_unit_id, **kwargs):
    obj = ResourceUnitConfig.objects.get(resource_unit_id=resource_unit_id)
    obj.__dict__.update(**kwargs)
    obj.save()


def get(resource_unit_id):
    return ResourceUnitConfig.objects.get(resource_unit_id=resource_unit_id)


def delete(resource_unit_id):
    ResourceUnitConfig.objects.get(resource_unit_id=resource_unit_id).delete()


def filter_list(**kwargs):
    return ResourceUnitConfig.objects.filter(**kwargs)
