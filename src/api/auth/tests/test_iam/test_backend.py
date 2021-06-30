# -*- coding: utf-8 -*
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

from auth.bkiam import IAM_REGISTERED_SYSTEM
from auth.bkiam.backend import IAMBackend
from auth.handlers.resource.manager import (
    GlobalFilter,
    ResourceAttrFilter,
    ResourceFilter,
    ResourceIdFilter,
)
from auth.handlers.resource.resources import (
    BizResource,
    ProjectResource,
    RawDataResource,
    ResultTableResource,
)
from auth.models.auth_models import UserRole
from auth.tests.utils import BaseTestCase


# IAM 作为测试样例的返回结构，后续可以不断扩展这里的内容，以验证目前 bkdata 能支持的授权格式
DEMO_IAM_POLICIES = {
    "content": [
        {
            "content": [
                {"field": "raw_data._bk_iam_path_", "value": "/biz,873/", "op": "starts_with"},
                {"field": "raw_data.sensitivity", "value": "confidential", "op": "eq"},
            ],
            "op": "AND",
        },
        {
            "content": [
                {
                    "content": [
                        {"field": "raw_data.id", "value": ["101328", "101386"], "op": "in"},
                        {"field": "raw_data._bk_iam_path_", "value": "/biz,105/", "op": "starts_with"},
                    ],
                    "op": "AND",
                },
                {"field": "raw_data._bk_iam_path_", "value": "/biz,100413/", "op": "starts_with"},
            ],
            "op": "OR",
        },
        {
            "content": [
                {"field": "raw_data.bk_biz_id", "value": "639", "op": "eq"},
                {"field": "raw_data.sensitivity", "value": "private", "op": "eq"},
            ],
            "op": "AND",
        },
        {"field": "raw_data.sensitivity", "value": "public", "op": "eq"},
    ],
    "op": "OR",
}

DEMO_IAM_ANY_POLICIES = {
    "content": [
        {"field": "raw_data.id", "value": [], "op": "any"},
        {
            "content": [
                {"field": "raw_data._bk_iam_path_", "value": "/biz,873/", "op": "starts_with"},
                {"field": "raw_data.sensitivity", "value": "confidential", "op": "eq"},
            ],
            "op": "AND",
        },
    ],
    "op": "OR",
}

DEMO_IAM_EMPTY_POLICIES = {}


class IAMBackendTestCase(BaseTestCase):
    """
    IAM Backend 测试用例，IAM 的 mock 范围主要针对策略范围数据
    """

    def setUp(self):
        self.iam_backend = IAMBackend()

    @mock.patch("auth.bkiam.backend.iam.get_token")
    def test_get_token(self, patch_get_token):
        patch_get_token.return_value = "xxxxx"
        self.assertEqual(self.iam_backend.get_token(), "xxxxx")

    def test_to_iam_action(self):
        iam_action_id = self.iam_backend.to_iam_action("result_table.query_data")
        self.assertEqual(iam_action_id, "result_table-query_data")

    def test_to_iam_resource(self):
        auth_resource = RawDataResource(
            raw_data_id=11,
            raw_data_name="test",
            bk_biz_id=105,
            sensitivity="private",
            bk_biz=BizResource(bk_biz_id=105),
        )

        resource = self.iam_backend.to_iam_resource(auth_resource)
        self.assertEqual(resource.id, "11")
        self.assertEqual(resource.system, IAM_REGISTERED_SYSTEM)
        self.assertEqual(resource.type, "raw_data")
        self.assertEqual(resource.attribute["sensitivity"], "private")
        self.assertEqual(resource.attribute["raw_data_id"], 11)
        self.assertEqual(resource.attribute["_bk_iam_path_"], ["/biz,105/"])

        auth_resource = ResultTableResource(
            result_table_id="105_xxx",
            result_table_name="test",
            sensitivity="private",
            processing_type="realtime",
            bk_biz=BizResource(bk_biz_id=105),
            project=ProjectResource(project_id=111),
        )
        resource = self.iam_backend.to_iam_resource(auth_resource)
        self.assertEqual(resource.id, "105_xxx")
        self.assertEqual(resource.system, IAM_REGISTERED_SYSTEM)
        self.assertEqual(resource.type, "result_table")
        self.assertEqual(resource.attribute["sensitivity"], "private")
        self.assertEqual(resource.attribute["result_table_id"], "105_xxx")
        self.assertIn("/biz,105/", resource.attribute["_bk_iam_path_"])
        self.assertIn("/project,111/", resource.attribute["_bk_iam_path_"])

    @mock.patch("auth.bkiam.backend.iam.is_allowed")
    def test_check_for_no_resource(self, patch_is_allowed_for_no_resource):
        def side_effect(request):
            self.assertEqual(request.system, "bk_data")
            self.assertEqual(request.subject.id, "user01@dingyi")
            self.assertEqual(request.action.id, "project-create")
            self.assertEqual(request.resources, None)

            return True

        patch_is_allowed_for_no_resource.side_effect = side_effect
        self.assertTrue(self.iam_backend.check("user01@dingyi", "project.create"))

    @mock.patch("auth.bkiam.backend.AuthResourceManager.get")
    @mock.patch("auth.bkiam.backend.iam.is_allowed")
    def test_check_for_resource(self, patch_is_allowed_for_resource, patch_resource_get):
        def side_effect(request):
            self.assertEqual(request.system, "bk_bkdata")
            self.assertEqual(request.subject.id, "user01@dingyi")
            self.assertEqual(request.action.id, "project-update")
            self.assertEqual(len(request.resources), 1)

            resource = request.resources[0]
            self.assertEqual(resource.type, "project")
            self.assertEqual(resource.attribute["_bk_iam_path_"], [])
            self.assertEqual(resource.attribute["project_id"], 111)

            return True

        patch_is_allowed_for_resource.side_effect = side_effect
        patch_resource_get.return_value = ProjectResource(project_id=111, project_name="xxx")

        self.assertTrue(self.iam_backend.check("user01@dingyi", "project.update", "111"))

    def test_check_consider_rt_manager(self):
        UserRole.objects.create(user_id="user01@dingyi", role_id="result_table.manager", scope_id="1001_xxx")
        self.assertTrue(self.iam_backend.check("user01@dingyi", "result_table.query_data", "1001_xxx"))

    @mock.patch("auth.bkiam.backend.AuthResourceManager.get")
    @mock.patch("auth.bkiam.backend.iam.is_allowed")
    def test_check_biz_common_access(self, patch_is_allowed_for_resource, patch_resource_get):
        """
        测试业务接入权限
        """
        patch_resource_get.return_value = BizResource(bk_biz_id=738)
        self.iam_backend.check("user01@dingyi", "biz.common_access", 738)

        self.assertEqual(patch_is_allowed_for_resource.call_args[0][0].action.id, "biz-manage")

    @mock.patch("auth.bkiam.backend.AuthResourceManager.get")
    @mock.patch("auth.bkiam.backend.iam.is_allowed")
    def test_check_res_indirect_parents(self, patch_is_allowed_for_resource, patch_resource_get):
        from auth.models.auth_models import ProjectData
        from auth.models.outer_models import ResultTable

        ResultTable.objects.create(
            result_table_id="591_xxx",
            bk_biz_id=591,
            project_id=111,
            sensitivity="private",
            result_table_name="xxx",
            result_table_name_alias="xxx",
            processing_type="clean",
            description="test_desc",
        )
        ProjectData.objects.create(project_id=2222, result_table_id="591_xxx", bk_biz_id=591)

        patch_is_allowed_for_resource.return_value = False
        patch_resource_get.return_value = ResultTableResource(
            result_table_id="591_xxx",
            result_table_name="xxx",
            project=ProjectResource(project_id=111),
            bk_biz=BizResource(bk_biz_id=591),
        )
        self.assertFalse(self.iam_backend.check("user01@dingyi", "result_table.query_data", "591_xxx"))

        # 第一次比对传入的校验参数
        first_request = patch_is_allowed_for_resource.call_args_list[0][0][0]
        self.assertListEqual(first_request.resources[0].attribute["_bk_iam_path_"], ["/project,111/", "/biz,591/"])

        # 第二次比对传入的校验参数
        second_request = patch_is_allowed_for_resource.call_args_list[1][0][0]
        self.assertListEqual(second_request.resources[0].attribute["_bk_iam_path_"], ["/project,2222/"])

    @mock.patch("auth.bkiam.backend.iam._do_policy_query")
    def test_make_filters(self, patch__do_policy_query):
        patch__do_policy_query.return_value = DEMO_IAM_POLICIES

        filter_groups = [
            [
                ResourceFilter(resource_type="biz", resource_id="873"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="confidential"),
            ],
            [ResourceIdFilter(func="in", value=["101328", "101386"])],
            [ResourceFilter(resource_type="biz", resource_id="100413")],
            [
                ResourceAttrFilter(func="eq", key="bk_biz_id", value="639"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="private"),
            ],
            [ResourceAttrFilter(func="eq", key="sensitivity", value="public")],
        ]

        self.assertListEqual(self.iam_backend.make_filters("user01@dingyi", "raw_data.delete"), filter_groups)

    @mock.patch("auth.bkiam.backend.iam._do_policy_query")
    def test_make_filters_with_any(self, patch__do_policy_query):
        patch__do_policy_query.return_value = DEMO_IAM_ANY_POLICIES
        filter_groups = [
            [GlobalFilter()],
            [
                ResourceFilter(resource_type="biz", resource_id="873"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="confidential"),
            ],
        ]
        self.assertListEqual(self.iam_backend.make_filters("user01@dingyi", "raw_data.delete"), filter_groups)

    @mock.patch("auth.bkiam.backend.iam._do_policy_query")
    def test_make_filters_with_empty(self, patch__do_policy_query):
        patch__do_policy_query.return_value = DEMO_IAM_EMPTY_POLICIES
        filter_groups = []
        self.assertListEqual(self.iam_backend.make_filters("user01@dingyi", "raw_data.delete"), filter_groups)

    @mock.patch("auth.bkiam.backend.IAMBackend.make_filters")
    def test_get_scopes(self, patch_make_filters):
        patch_make_filters.return_value = [
            [
                ResourceFilter(resource_type="biz", resource_id="873"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="confidential"),
            ],
            [ResourceIdFilter(func="in", value=["101328", "101386"])],
            [ResourceFilter(resource_type="biz", resource_id="100413")],
            [
                ResourceAttrFilter(func="eq", key="bk_biz_id", value="639"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="private"),
            ],
            [ResourceAttrFilter(func="eq", key="sensitivity", value="public")],
        ]

        scopes = [
            {"bk_biz_id": 873, "sensitivity": "confidential"},
            {"raw_data_id": 101328},
            {"raw_data_id": 101386},
            {"bk_biz_id": 100413},
            {"bk_biz_id": 639, "sensitivity": "private"},
            {"sensitivity": "public"},
        ]

        self.assertListEqual(self.iam_backend.get_scopes("user01@dingyi", "raw_data.delete"), scopes)

    @mock.patch("auth.bkiam.backend.IAMBackend.make_filters")
    def test_get_scopes_consider_rt_manager(self, patch_make_filters):
        patch_make_filters.return_value = [
            [
                ResourceFilter(resource_type="biz", resource_id="873"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="confidential"),
            ],
            [ResourceFilter(resource_type="project", resource_id="100413")],
            [ResourceIdFilter(func="in", value=["101_xxxx1", "101_xxxx2"])],
            [ResourceAttrFilter(func="eq", key="sensitivity", value="public")],
        ]
        UserRole.objects.create(user_id="user01@dingyi", role_id="result_table.manager", scope_id="102_xxx1")
        UserRole.objects.create(user_id="user01@dingyi", role_id="result_table.manager", scope_id="102_xxx2")
        UserRole.objects.create(user_id="user01@dingyi", role_id="result_table.manager", scope_id="102_xxx3")

        scopes = [
            {"bk_biz_id": 873, "sensitivity": "confidential"},
            {"project_id": 100413},
            {"result_table_id": "101_xxxx1"},
            {"result_table_id": "101_xxxx2"},
            {"sensitivity": "public"},
            {"result_table_id": "102_xxx1"},
            {"result_table_id": "102_xxx2"},
            {"result_table_id": "102_xxx3"},
        ]

        cur_scopes = self.iam_backend.get_scopes("user01@dingyi", "result_table.query_data")
        self.assertListEqual(cur_scopes, scopes)

    @mock.patch("auth.bkiam.backend.IAMBackend.make_filters")
    def test_scopes_indirect_parent(self, patch_make_filters):
        patch_make_filters.return_value = [
            [
                ResourceFilter(resource_type="biz", resource_id="873"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="confidential"),
            ],
            [ResourceFilter(resource_type="project", resource_id="1")],
            [ResourceFilter(resource_type="raw_data", resource_id="1")],
            [
                ResourceAttrFilter(func="eq", key="bk_biz_id", value="639"),
                ResourceAttrFilter(func="eq", key="sensitivity", value="private"),
            ],
            [ResourceAttrFilter(func="eq", key="sensitivity", value="public")],
        ]

        scopes = [
            {"bk_biz_id": 873, "sensitivity": "confidential"},
            {"project_id": 1},
            {"raw_data_id": 1},
            {"bk_biz_id": 639, "sensitivity": "private"},
            {"sensitivity": "public"},
        ]

        self.assertListEqual(self.iam_backend.get_scopes("user01@dingyi", "result_table.query_data"), scopes)
