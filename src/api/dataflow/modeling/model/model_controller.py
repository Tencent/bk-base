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

from dataflow.modeling.model.platform_model import PlatformModel


class ModelController(object):
    def __init__(self):
        self.model = PlatformModel()

    @classmethod
    def inspection_before_release(cls, sql):
        return PlatformModel.inspection_before_release(sql)

    @classmethod
    def create(
        cls,
        last_sql,
        project_id,
        model_name,
        model_alias,
        is_public,
        result_table_ids,
        description,
        experiment_name,
        evaluation_result={},
        notebook_id=None,
    ):
        return PlatformModel.create(
            last_sql,
            project_id,
            model_name,
            model_alias,
            is_public,
            result_table_ids,
            description,
            experiment_name,
            evaluation_result,
            notebook_id,
        )

    def update(
        self,
        model_name,
        last_sql,
        project_id,
        result_table_ids,
        description,
        experiment_name,
        experiment_id,
        evaluation_result={},
    ):
        return self.model.update(
            model_name,
            last_sql,
            project_id,
            result_table_ids,
            description,
            experiment_name,
            experiment_id,
            evaluation_result,
        )

    def get_release_result(self, task_id):
        return self.model.get_release_result(task_id)

    def generate_release_debug_config(self, debug_id):
        return self.model.generate_release_debug_config(debug_id)

    def get_release_model_by_project(self, project_id):
        return self.model.get_release_model_by_project(project_id)

    def get_update_model_by_project(self, project_id):
        return self.model.get_update_model_by_project(project_id)

    def get_release_model_by_name(self, model_name):
        return self.model.get_model_by_name(model_name)

    def check_model_update(self, model_id, processing_id):
        return self.model.check_model_update(model_id, processing_id)

    def update_model_info(self, model_id, processing_id):
        self.model.update_model_info(model_id, processing_id)

    def is_model_experiment_exists(self, model_name, model_experiemnt_name):
        return self.model.is_model_experiment_exists(model_name, model_experiemnt_name)
