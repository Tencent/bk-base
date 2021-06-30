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
from django.db import transaction
from jobnavi.models.bkdata_flow import DataflowJobNaviClusterConfig
from common.transaction import auto_meta_sync
from common.meta.common import create_tag_to_target


def update(cluster_name, geog_area_code, **kwargs):
    with auto_meta_sync(using='default'):
        DataflowJobNaviClusterConfig.objects.filter(
            cluster_name=cluster_name, geog_area_code=geog_area_code).update(**kwargs)


def delete(**kwargs):
    with auto_meta_sync(using='default'):
        DataflowJobNaviClusterConfig.objects.filter(**kwargs).delete()


@transaction.atomic()
def save(tag, **kwargs):
    target_ids = []
    with auto_meta_sync(using='default'):
        cluster_config = DataflowJobNaviClusterConfig(**kwargs)
        cluster_config.save()
        target_ids.append(('jobnavi_cluster', cluster_config.id))
    with auto_meta_sync(using='bkdata_basic'):
        create_tag_to_target(target_ids, [tag])


def filter(**kwargs):
    return DataflowJobNaviClusterConfig.objects.filter(**kwargs)
