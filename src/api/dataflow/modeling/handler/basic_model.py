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

from dataflow.modeling.models import BasicModel


class BasicModelHandler(object):
    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def create_basic_model(cls, params):
        BasicModel.objects.create(
            basic_model_id=params["basic_model_id"],
            basic_model_name=params["basic_model_name"],
            basic_model_alias=params["basic_model_alias"],
            algorithm_name=params["algorithm_name"],
            algorithm_version=params["algorithm_version"],
            description=params["description"],
            status=params["status"],
            active=params["active"],
            config=params["config"],
            execute_config=params["execute_config"],
            properties=params["properties"],
            experiment_id=0,
            experiment_instance_id=0,
            source=params["source"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
            model_id=params["model_id"],
        )

    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def save(cls, **kwargs):
        BasicModel(**kwargs).save()

    @classmethod
    def exists(cls, **kwargs):
        return BasicModel.objects.filter(**kwargs).exists()
