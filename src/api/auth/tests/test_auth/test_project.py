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

from unittest import mock

from auth.config.ticket import RESOURCE_GROUP_USE
from auth.constants import SubjectTypeChoices
from auth.core.ticket import TicketFactory
from auth.handlers.project import ProjectHandler
from auth.models.auth_models import ProjectData, ProjectRawData, UserRole
from auth.models.base_models import RoleConfig
from auth.tests.utils import BaseTestCase, compress_str, is_api_success
from common.api.base import DataResponse

PROJECT_ID_1 = 1


PROJECT_RT_TEMPLATE = """
{
    var(func: eq(ProjectInfo.project_id, 1)) {
        ~ResultTable.project {
            level0_uids as uid
        }
        ~ProjectData.project {
            pd_rt_uids as ProjectData.result_table
        }
    }
    var(func: uid(level0_uids, pd_rt_uids)) {
        level1_uids as uid
    }
    var(func: uid(level1_uids)) @filter(not eq(ResultTable.processing_type, "queryset")){
        level2_uids as uid
    }
    content(func: uid(level2_uids)){
        bk_biz_id: ResultTable.bk_biz_id
        result_table_id: ResultTable.result_table_id
    }
}
"""

PROJECT_RT_UPDATE_TEMPLATE = """
{
    var(func: eq(ProjectInfo.project_id, 1)) {
        ~ResultTable.project {
            level0_uids as uid
        }
    }
    var(func: uid(level0_uids)) @filter(eq(ResultTable.bk_biz_id, 591)){
        level1_uids as uid
    }
    var(func: uid(level1_uids)) @filter(not eq(ResultTable.processing_type, "queryset")){
        level2_uids as uid
    }
    content(func: uid(level2_uids)){
        bk_biz_id: ResultTable.bk_biz_id
        result_table_id: ResultTable.result_table_id
        result_table_name: ResultTable.result_table_name
        description: ResultTable.description
        processing_type: ResultTable.processing_type
        generate_type: ResultTable.generate_type
        created_by
        created_at
        updated_by
        updated_at
    }
}
"""


PROJECT_RD_TEMPLATE = """
{
    var(func: eq(ProjectInfo.project_id, 1)) {
        ~ProjectRawData.project
            @filter(eq(ProjectRawData.bk_biz_id, 591))
        {
            rd_uids as ProjectRawData.raw_data
        }
    }
    content(func: uid(rd_uids)) {
        bk_biz_id: AccessRawData.bk_biz_id
        raw_data_id: AccessRawData.id
        raw_data_name: AccessRawData.raw_data_name
        description: AccessRawData.description
        created_by
        created_at
        updated_by
        updated_at
    }
}
"""


class ProjectTestCase(BaseTestCase):
    def test_add_data(self):
        ProjectHandler(project_id=11).add_data({"bk_biz_id": 122, "result_table_id": None})
        d1 = ProjectData.objects.all()[0]
        self.assertEqual(d1.bk_biz_id, 122)
        self.assertEqual(d1.result_table_id, None)

    @mock.patch("auth.handlers.project.MetaApi.entity_complex_search")
    def test_get_data(self, patch_search):
        content = [
            {"result_table_id": "100616_unittest_table", "ResultTable.bk_biz_id": 100616},
            {"result_table_id": "100371_dadada", "ResultTable.bk_biz_id": 100371},
        ]
        patch_search.return_value = DataResponse(
            {"result": True, "data": {"extensions": {}, "data": {"content": content}}}
        )

        d = ProjectHandler(project_id=1).get_data()
        self.assertListEqual(d, content)

        self.assertEqual(compress_str(PROJECT_RT_TEMPLATE), compress_str(patch_search.call_args[0][0]["statement"]))

        ProjectHandler(project_id=1).get_data(bk_biz_id=591, action_id="raw_data.query_data", extra_fields=True)
        self.assertEqual(compress_str(PROJECT_RD_TEMPLATE), compress_str(patch_search.call_args[0][0]["statement"]))

        ProjectHandler(project_id=1).get_data(bk_biz_id=591, action_id="result_table.update_data", extra_fields=True)
        self.assertEqual(
            compress_str(PROJECT_RT_UPDATE_TEMPLATE), compress_str(patch_search.call_args[0][0]["statement"])
        )

    @mock.patch("auth.handlers.project.ProjectHandler.get_data")
    def test_get_data_tickets(self, patch_get_data):
        patch_get_data.return_value = [{"bk_biz_id": 591, "result_table_id": "591_test_rt"}]
        resp = self.client.get(f"/v3/auth/projects/{PROJECT_ID_1}/data_tickets/")
        data = is_api_success(self, resp)
        self.assertEqual(len(data), 1)
        self.assertEqual(data["591_test_rt"]["status"], "succeeded")
        self.assertEqual(data["591_test_rt"]["bk_biz_id"], 591)

    @mock.patch("auth.handlers.project.ProjectHandler.get_data")
    def test_get_biz_tickets_count(self, patch_get_data):
        patch_get_data.return_value = [{"bk_biz_id": 591, "result_table_id": "591_test_rt"}]
        resp = self.client.get(f"/v3/auth/projects/{PROJECT_ID_1}/biz_tickets_count/")
        data = is_api_success(self, resp)
        self.assertEqual(data[0]["bk_biz_id"], "591")
        self.assertEqual(data[0]["int_bk_biz_id"], 591)
        self.assertEqual(data[0]["count"]["succeeded"], 1)

        resp = self.client.get(
            "/v3/auth/projects/{}/biz_tickets_count/"
            "?action_ids=result_table.query_data,raw_data.query_data".format(PROJECT_ID_1)
        )
        data = is_api_success(self, resp)
        self.assertEqual(data[0]["bk_biz_id"], "591")
        self.assertEqual(data[0]["int_bk_biz_id"], 591)
        self.assertEqual(data[0]["count"]["succeeded"], 2)

    def test_get_bizs(self):
        resp = self.client.get(f"/v3/auth/projects/{PROJECT_ID_1}/bizs/")
        data = is_api_success(self, resp)
        self.assertEqual(data[0]["bk_biz_id"], 591)

    @mock.patch("auth.handlers.project.ProjectHandler.get_data")
    def test_check_data_perm(self, patch_get_data):
        patch_get_data.return_value = [{"bk_biz_id": 591, "result_table_id": "591_test_rt"}]

        self.assertTrue(ProjectHandler(project_id=11).check_data_perm("591_test_rt"))

        # 项目自身产生的结果表
        self.assertTrue(ProjectHandler(project_id=1).check_data_perm("591_test_rt"))

        resp = self.post("/v3/auth/projects/11/data/check/", {"result_table_id": "591_test_rt"})
        data = self.is_api_success(resp)
        self.assertTrue(data)

    def test_add_data_request(self):
        resp = self.post("/v3/auth/projects/11/data/add/", {"bk_biz_id": 591, "result_table_id": "591_test_rt"})
        self.is_api_success(resp)

        data = list(ProjectData.objects.filter(project_id=11).values())[0]
        self.assertEqual(data["bk_biz_id"], 591)
        self.assertEqual(data["result_table_id"], "591_test_rt")

        # test duplicated item
        resp = self.post("/v3/auth/projects/11/data/add/", {"bk_biz_id": 591, "result_table_id": "591_test_rt"})
        data = ProjectData.objects.filter(project_id=11)
        self.assertEqual(len(data), 1)

    def test_add_rawdata(self):
        ProjectHandler(project_id=11).add_data(
            {"bk_biz_id": 122, "raw_data_id": 11122}, action_id="raw_data.query_data"
        )
        d1 = ProjectRawData.objects.all()[0]
        self.assertEqual(d1.bk_biz_id, 122)
        self.assertEqual(d1.raw_data_id, 11122)

    @mock.patch("auth.handlers.project.ProjectHandler.get_data")
    def test_check_rawdata_perm(self, patch_get_data):
        patch_get_data.return_value = [{"bk_biz_id": 591, "raw_data_id": 4}]
        self.assertTrue(ProjectHandler(project_id=11).check_data_perm(4, "raw_data.query_data"))

        resp = self.post("/v3/auth/projects/11/data/check/", {"object_id": 4, "action_id": "raw_data.query_data"})
        data = self.is_api_success(resp)
        self.assertTrue(data)

    def test_add_data_perm(self):
        resp = self.post(
            "/v3/auth/projects/11/data/add/", {"object_id": 4, "action_id": "raw_data.query_data", "bk_biz_id": 591}
        )
        self.is_api_success(resp)


PROJECT_INFO = DataResponse(
    {
        "data": {
            "bk_biz_id": 2,
            "project_id": 2331,
            "project_name": "测试项目",
            "result_table_id": "2_output",
            "result_table_name": "output",
            "result_table_name_alias": "output_alias",
            "result_table_type": None,
            "processing_type": "clean",
            "generate_type": "user",
            "sensitivity": "output_alias",
            "count_freq": 60,
            "created_by": "trump",
            "created_at": "2018-09-19 17:03:50",
            "updated_by": None,
            "updated_at": "2018-09-19 17:03:50",
            "description": "输出",
            "concurrency": 0,
            "is_managed": 1,
            "platform": "tdw",
            "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
        },
        "result": True,
        "message": "",
        "code": 1500200,
        "errors": None,
    }
)

CLUSTER_GROUP_CONFIGS = DataResponse(
    {
        "data": [
            {
                "cluster_group_id": "mysql_lol_1",
                "cluster_group_name": "aiops",
                "cluster_group_alias": "\\u667a\\u80fd\\u8fd0\\u8425AIOPS",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
            {
                "cluster_group_id": "tredis_lol_1",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "mainland", "alias": "中国大陆"}]}},
            },
            {
                "cluster_group_id": "mysql_lol_2",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
            {
                "cluster_group_id": "public_group",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "public",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
        ],
        "result": True,
        "message": "",
        "code": 1500200,
        "errors": None,
    }
)


class ProjectClusterGroupTestCase(BaseTestCase):
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: PROJECT_INFO)
    @mock.patch("auth.api.MetaApi.cluster_group_configs", lambda return_value: CLUSTER_GROUP_CONFIGS)
    def test_query_cluster_group(self):
        groups = ProjectHandler(project_id=1).list_cluster_group()
        group_ids = [group["cluster_group_id"] for group in groups]
        self.assertTrue("mysql_lol_1" in group_ids)
        self.assertFalse("public_group" in group_ids)

    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: PROJECT_INFO)
    @mock.patch("auth.api.MetaApi.cluster_group_configs", lambda return_value: CLUSTER_GROUP_CONFIGS)
    def test_check_cluster_group(self):
        self.assertTrue(ProjectHandler(project_id=PROJECT_ID_1).check_cluster_group("mysql_lol_1"))
        self.assertTrue(ProjectHandler(project_id=PROJECT_ID_1).check_cluster_group("tredis_lol_1"))
        self.assertFalse(ProjectHandler(project_id=PROJECT_ID_1).check_cluster_group("mysql_lol_2"))
        self.assertFalse(ProjectHandler(project_id=PROJECT_ID_1).check_cluster_group("public_group"))

    def test_resource_group_tickets_api(self):
        UserRole.objects.create(
            user_id="user01",
            role_id=RoleConfig.objects.get(pk="resource_group.manager").role_id,
            scope_id="mysql_lol_2",
        )
        UserRole.objects.create(
            user_id="user02", role_id=RoleConfig.objects.get(pk="resource_group.manager").role_id, scope_id="test_xxx"
        )
        data = {
            "created_by": "user01",
            "reason": "hello world",
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "项目1",
                    "subject_class": SubjectTypeChoices.PROJECT,
                    "action": "resource_group.use",
                    "object_class": "resource_group",
                    "scope": {
                        "resource_group_id": "mysql_lol_2",
                    },
                }
            ],
        }
        TicketFactory(RESOURCE_GROUP_USE).generate_ticket_data(data)

        data = {
            "created_by": "user01",
            "reason": "hello world",
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "项目1",
                    "subject_class": SubjectTypeChoices.PROJECT,
                    "action": "resource_group.use",
                    "object_class": "resource_group",
                    "scope": {
                        "resource_group_id": "test_xxx",
                    },
                }
            ],
        }
        TicketFactory(RESOURCE_GROUP_USE).generate_ticket_data(data)

        resp = self.get("/v3/auth/projects/1/resource_group_tickets/")
        data = self.is_api_success(resp)

        def _get_resouce_group(data, resource_group_id):
            for _id, _content in list(data.items()):
                if resource_group_id == _id:
                    return _content
            raise Exception(f"No {resource_group_id}")

        mysql_lol_1 = _get_resouce_group(data, "mysql_lol_1")
        mysql_lol_2 = _get_resouce_group(data, "mysql_lol_2")
        tredis_lol_1 = _get_resouce_group(data, "tredis_lol_1")
        test_xxx = _get_resouce_group(data, "test_xxx")

        self.assertEqual(mysql_lol_1["status"], "succeeded")
        self.assertTrue(mysql_lol_1["ticket_id"] is None)

        self.assertEqual(mysql_lol_2["status"], "succeeded")
        self.assertFalse(mysql_lol_2["ticket_id"] is None)

        self.assertEqual(tredis_lol_1["status"], "succeeded")
        self.assertTrue(tredis_lol_1["ticket_id"] is None)

        self.assertEqual(test_xxx["status"], "processing")
        self.assertFalse(test_xxx["ticket_id"] is None)
