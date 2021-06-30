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


from apps.api import DataFlowApi, DataLabApi


class Project(object):
    @classmethod
    def wrap_flow_count(cls, projects):
        """
        补充任务数量
        """
        if len(projects) < 50:
            project_ids = [p["project_id"] for p in projects]
            flow_counts = DataFlowApi.flow_flows_projects.count({"project_id": project_ids})
        else:
            # 项目数量太多的用户，不适用 __in 查询方式，拉取全量，本地过滤
            flow_counts = DataFlowApi.flow_flows_projects.count()
        m_flow_count = {_c["project_id"]: _c for _c in flow_counts}

        for project in projects:
            _project_id = project["project_id"]
            if _project_id in list(m_flow_count.keys()):
                _count = m_flow_count[_project_id]
                project["flow_count"] = _count["count"]
                project["running_count"] = _count["running_count"]
                project["no_start_count"] = _count["no_start_count"]
                project["normal_count"] = _count["normal_count"]
                project["exception_count"] = _count["exception_count"]
            else:
                project["flow_count"] = 0
                project["running_count"] = 0
                project["no_start_count"] = 0
                project["normal_count"] = 0
                project["exception_count"] = 0

    @classmethod
    def wrap_lab_count(cls, projects):
        """
        为任务添加笔记数量和访问数量
        """
        if len(projects) < 50:
            project_ids = [p["project_id"] for p in projects]
            lab_count = DataLabApi.get_query_count_by_project({"project_id": project_ids})
        else:
            lab_count = DataLabApi.get_query_count_by_project()
        m_lab_count = {int(_c["project_id"]): _c for _c in lab_count}

        for project in projects:
            _project_id = project["project_id"]
            if _project_id in m_lab_count:
                _count = m_lab_count[_project_id]
                project["query_count"] = _count["query_count"]
                project["notebook_count"] = _count["notebook_count"]
            else:
                project["query_count"] = 0
                project["notebook_count"] = 0
