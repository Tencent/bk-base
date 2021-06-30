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
from django.db import transaction

from dataflow.models import ProcessingClusterConfig
from dataflow.shared.meta.tag.tag_helper import TagHelper


def where(**kwargs):
    return ProcessingClusterConfig.objects.filter(**kwargs)


def delete(**kwargs):
    with auto_meta_sync(using="default"):
        ProcessingClusterConfig.objects.filter(**kwargs).delete()


@transaction.atomic()
def save(tag, **kwargs):
    target_ids = []
    with auto_meta_sync(using="default"):
        job_info = ProcessingClusterConfig(**kwargs)
        job_info.save()
        target_ids.append(("processing_cluster", job_info.id))
    TagHelper.create_tag(target_ids, [tag])


def update(geog_area_code, cluster_name, component_type, **kwargs):
    with auto_meta_sync(using="default"):
        ProcessingClusterConfig.objects.filter(
            geog_area_code=geog_area_code,
            cluster_name=cluster_name,
            component_type=component_type,
        ).update(**kwargs)
