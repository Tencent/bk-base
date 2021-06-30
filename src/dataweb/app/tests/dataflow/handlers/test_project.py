# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from apps.dataflow.handlers.project import Project
from tests.test_base import BaseTestCase


class TestIeodProject(BaseTestCase):
    def test_wrap_lab_count(self):
        projects = [{"project_id": 1}, {"project_id": 2}, {"project_id": 3}, {"project_id": 4}]
        fresult = [
            {"project_id": 1, "query_count": 10, "notebook_count": 12},
            {"project_id": 2, "query_count": 2, "notebook_count": 5},
            {"project_id": 3, "query_count": 3, "notebook_count": 3},
            {"project_id": 4, "query_count": 0, "notebook_count": 0},
        ]

        Project.wrap_lab_count(projects)
        self.assertEqual(projects, fresult)
