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
from django.db.models import Max

from dataflow.modeling.models import ModelExperiment


class ModelExperimentHandler(object):
    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def create_model_experiment(cls, **kvargs):
        experiment = ModelExperiment(**kvargs)
        experiment.save()
        return experiment.experiment_id

    @classmethod
    def get_max_experiment_index(cls, model_id):
        return ModelExperiment.objects.filter(model_id=model_id).aggregate(Max("experiment_index"))[
            "experiment_index__max"
        ]

    @classmethod
    def is_model_experiment_exists(cls, model_id):
        return ModelExperiment.objects.filter(model_id=model_id).exists()

    @classmethod
    def is_model_experiment_exists_by_name(cls, experiment_name):
        return ModelExperiment.objects.filter(experiment_name=experiment_name).exists()

    @classmethod
    def get_experiment_by_name(cls, experiment_name):
        return ModelExperiment.objects.get(experiment_name=experiment_name)

    @classmethod
    def get_experiment_list(cls, model_id):
        experiment_list = ModelExperiment.objects.filter(model_id=model_id)
        result_list = []
        for experiment_item in experiment_list:
            experiment_json = {
                "experiment_id": experiment_item.experiment_id,
                "experiment_name": experiment_item.experiment_name,
                "experiment_alias": experiment_item.experiment_alias,
            }
            result_list.append(experiment_json)
        return result_list
