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

from dataflow.modeling.models import ModelRelease


class ModelReleaseHandler(object):
    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def create_model_release(cls, params):
        ModelRelease.objects.create(
            model_experiment_id=0,
            model_id=params["model_id"],
            active=params["active"],
            description=params["description"],
            publish_status=params["publish_status"],
            model_config_template=params["model_config_template"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
            version_index=params["version_index"],
            protocol_version=params["protocol_version"],
            basic_model_id=params["basic_model_id"],
        )

    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def save(cls, **kwargs):
        ModelRelease(**kwargs).save()

    @classmethod
    def where(cls, **kwargs):
        return ModelRelease.objects.filter(**kwargs)
