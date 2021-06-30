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
from django.conf import settings

from auth.api import MetaApi
from auth.constants import SubjectTypeChoices
from auth.handlers.object_classes import ObjectFactory
from auth.handlers.resource_group import ResourceGroupHandler, ResourceGroupSubject
from auth.models import ProjectData, ProjectRawData
from auth.models.auth_models import ProjectClusterGroupConfig
from auth.models.outer_models import AccessRawData, ResultTable
from auth.templates.template_manage import TemplateManager
from common.transaction import auto_meta_sync

QUERY_ACTION = "result_table.query_data"
WRITE_ACTION = "result_table.update_data"

RAWDATA_QUERY_ACTION = "raw_data.query_data"


class ProjectHandler:
    def __init__(self, project_id):
        self.project_id = project_id
        self.template_manager = TemplateManager()

    def get_data(self, bk_biz_id=None, with_queryset=False, action_id=QUERY_ACTION, extra_fields=False):
        """
        返回有权限的数据，支持按业务过滤，默认检索都是查询权限

        @returnExample action_id=result_table.query_data
            [
                {bk_biz_id: 1, result_table_id=1_xx},
                {bk_biz_id: 2, result_table_id=2_xx}
            ]
        @returnExample action_id=raw_data.query_data
            [
                {bk_biz_id: 1, raw_data_id=1},
                {bk_biz_id: 2, raw_data_id=2}
            ]
        """
        context_params = {"project_id": self.project_id, "extra_fields": extra_fields}
        # 支持按照业务维度过滤结果表
        if bk_biz_id is not None:
            context_params["bk_biz_id"] = bk_biz_id

        rt_templete = self.template_manager.get_template("/dgraph/project_rt.mako")
        rd_templete = self.template_manager.get_template("/dgraph/project_rd.mako")

        if action_id in [QUERY_ACTION, WRITE_ACTION]:
            context_params.update({"with_queryset": with_queryset, "with_project_data": False})
            # 0. 默认项目产生的结果表有任意权限
            # 1. 申请使用权限的结果表
            if action_id == QUERY_ACTION:
                context_params["with_project_data"] = True

            statement = rt_templete.render(**context_params)

        elif action_id == RAWDATA_QUERY_ACTION:
            statement = rd_templete.render(**context_params)

        data = MetaApi.entity_complex_search(
            {"statement": statement, "backend_type": "dgraph"}, raise_exception=True
        ).data["data"]["content"]

        return data

    def get_bizs(self):
        """
        项目有权限的数据 的业务，去重
        @return:
        """
        bizs = list(ProjectData.objects.filter(project_id=self.project_id).values_list("bk_biz_id", flat=True))

        rawdata_bizs = list(
            ProjectRawData.objects.filter(project_id=self.project_id).values_list("bk_biz_id", flat=True)
        )

        result_table_bizs = list(
            ResultTable.objects.filter(project_id=self.project_id).values_list("bk_biz_id", flat=True)
        )
        biz_ids = list(set(bizs + result_table_bizs + rawdata_bizs))
        return [{"bk_biz_id": bk_biz_id} for bk_biz_id in biz_ids]

    def check_data_perm(self, object_id, action_id=QUERY_ACTION):
        """
        检查项目对于目标对象有没有权限
        """
        obj = ObjectFactory.init_object(action_id)(object_id)
        for scope in self.get_data(with_queryset=True, action_id=action_id):
            if obj.is_in_scope(scope):
                return True

        return False

    def add_data(self, scope, action_id=QUERY_ACTION):
        """
        填充有权限的数据
        @param {dict} scope 数据集范围，专指 result_table
        @paramExample scope-example
            {'bk_biz_id': 111, 'result_table_id': '111_xx'}
        """
        if action_id == QUERY_ACTION:
            if scope.get("bk_biz_id") is None:
                scope["bk_biz_id"] = (
                    ResultTable.objects.filter(result_table_id=scope.get("result_table_id")).first().bk_biz_id
                )

            scopes = self.get_data()
            auth_biz_ids = [s["bk_biz_id"] for s in scopes if s.get("result_table_id") is None]
            auth_result_table_ids = [s["result_table_id"] for s in scopes if s.get("result_table_id") is not None]

            if scope["bk_biz_id"] in auth_biz_ids:
                return

            if "result_table_id" in scope and scope["result_table_id"] in auth_result_table_ids:
                return

            with auto_meta_sync(using="basic"):
                ProjectData.objects.create(
                    project_id=self.project_id,
                    bk_biz_id=scope["bk_biz_id"],
                    result_table_id=scope.get("result_table_id", None),
                )
        elif action_id == RAWDATA_QUERY_ACTION:
            if scope.get("bk_biz_id") is None:
                scope["bk_biz_id"] = (
                    AccessRawData.objects.filter(raw_data_id=scope.get("raw_data_id")).first().bk_biz_id
                )

            scopes = self.get_data(action_id=RAWDATA_QUERY_ACTION)
            auth_raw_data_ids = [s["raw_data_id"] for s in scopes if s.get("raw_data_id") is not None]

            if "raw_data_id" in scope and scope["raw_data_id"] in auth_raw_data_ids:
                return

            with auto_meta_sync(using="basic"):
                ProjectRawData.objects.create(
                    project_id=self.project_id, bk_biz_id=scope["bk_biz_id"], raw_data_id=scope["raw_data_id"]
                )

    def list_cluster_group(self):
        """
        返回有权限且区域标签相符的集群组
        """
        use_resource_center = getattr(settings, "USE_RESOURCE_CENTER", True)

        # 新版资源中心
        if use_resource_center:
            resource_group_handler = ResourceGroupHandler()
            subjects = [ResourceGroupSubject(subject_type=SubjectTypeChoices.PROJECT, subject_id=self.project_id)]
            return resource_group_handler.list_authorized_cluster_group(subjects, self.get_geo_tags())
        # 旧版资源中心
        else:
            project_groups = ProjectClusterGroupConfig.objects.filter(project_id=self.project_id).values_list(
                "cluster_group_id", flat=True
            )

            # 约定了项目和集群组必须由区域标签，若区域标签为空的情况，视为不满足需求的集群组
            groups = MetaApi.cluster_group_configs({"tags": self.get_geo_tags()}).data
            return [
                group for group in groups if group["cluster_group_id"] in project_groups or group["scope"] == "public"
            ]

    def check_cluster_group(self, cluster_group_id):
        """
        检查是否对集群组有权限
        """
        return cluster_group_id in [group["cluster_group_id"] for group in self.list_cluster_group()]

    def get_geo_tags(self):
        """
        获取地域标签
        """
        basic_info = self.get_basic_info()
        if basic_info is None:
            return []

        return [tag["code"] for tag in basic_info.get("tags", {}).get("manage", {}).get("geog_area", [])]

    @property
    def geo_tags(self):
        """
        获取地域标签
        """
        return self.get_geo_tags()

    def get_basic_info(self):
        """
        获取基项目信息
        """
        return MetaApi.get_project_info({"project_id": self.project_id}).data
