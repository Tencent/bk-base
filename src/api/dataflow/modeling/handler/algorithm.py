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
from django.db.models import Q

from dataflow.modeling.models import Algorithm


class AlgorithmHandler(object):
    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def create_algorithm(cls, params):
        Algorithm.objects.create(
            algorithm_name=params["algorithm_name"],
            algorithm_alias=params["algorithm_alias"],
            description=params["description"],
            algorithm_type=params["algorithm_type"],
            generate_type=params["generate_type"],
            sensitivity=params["sensitivity"],
            project_id=params["project_id"],
            run_env=params["run_env"],
            framework=params["framework"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
        )

    @classmethod
    def get_algorithm(cls, bk_username, framework=None):
        if framework:
            return Algorithm.objects.filter(
                Q(framework=framework) & (Q(sensitivity="public") | Q(created_by=bk_username))
            )
        else:
            return Algorithm.objects.filter(Q(sensitivity="public") | Q(created_by=bk_username))

    @classmethod
    def get_spark_algorithm(cls):
        return Algorithm.objects.filter(framework="spark_mllib", project_id=0)

    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def save(cls, **kwargs):
        Algorithm(**kwargs).save()
