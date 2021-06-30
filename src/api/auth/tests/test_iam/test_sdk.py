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
import json
import traceback
from unittest import skip

from auth.bkiam.sync import RoleSync
from auth.management.handlers.do_migrate import Client
from auth.models.base_models import ActionConfig
from auth.tests.utils import BaseTestCase
from auth.bkiam import (
    IAM_API_HOST,
    IAM_API_PAAS_HOST,
    IAM_REGISTERED_APP_SECRET,
    IAM_REGISTERED_APP_CODE,
    IAM_REGISTERED_SYSTEM,
)
from iam import IAM, Action, Request, Subject
from iam.auth.models import (
    ApiAuthRequest,
    ApiAuthResourceWithId,
    ApiAuthResourceWithPath,
    ApiBatchAuthRequest,
    ApiBatchAuthResourceWithId,
    ApiBatchAuthResourceWithPath,
)

iam = IAM(IAM_REGISTERED_APP_CODE, IAM_REGISTERED_APP_SECRET, IAM_API_HOST, IAM_API_PAAS_HOST)


@skip("Here, testcases use the real iam-service")
class IAMSDKTestCase(BaseTestCase):
    def setUp(self):
        self.iam = iam
        self.system = IAM_REGISTERED_SYSTEM
        self.role_sync = RoleSync()

    def test_get_token(self):
        print(self.iam.get_token(IAM_REGISTERED_SYSTEM))

    def test_get_polices(self):
        request = self._make_request_without_resources("admin@dingyi", "raw_data-delete")
        print(json.dumps(self.iam._do_policy_query(request)))

    def _make_request_with_resources(self, user_id, action_id, resources):
        request = Request(self.system, Subject("user", user_id), Action(action_id), resources, None)
        return request

    def _make_request_without_resources(self, user_id, action_id):
        request = Request(self.system, Subject("user", user_id), Action(action_id), None, None)
        return request

    def test_grant_by_instance(self):
        """
        单个资源的资源授权/回收
        """
        # resources = [ApiAuthResourceWithId(IAM_REGISTERED_SYSTEM, 'project', '249', name='project1')]
        request = ApiAuthRequest(
            self.system, Subject("user", "user01@dingyi"), Action("project-create"), None, None, operate="revoke"
        )
        self.iam.grant_or_revoke_instance_permission(request, bk_username="user01@dingyi")

    def test_grant_by_path(self):
        """
        单个资源的资源授权/回收
        """
        resources = [
            ApiAuthResourceWithPath(
                IAM_REGISTERED_SYSTEM, "project", path=[{"type": "project", "id": "1111", "name": "project1"}]
            )
        ]
        request = ApiAuthRequest(
            self.system, Subject("user", "user01@dingyi"), Action("flow-create"), resources, None, operate="grant"
        )
        self.iam.grant_or_revoke_path_permission(request, bk_username="user01@dingyi")

    def test_revoke_by_instance(self):
        """
        单个资源的资源授权/回收
        """
        resources = [ApiAuthResourceWithId(IAM_REGISTERED_SYSTEM, "flow", "flow", name="dddss")]
        request = ApiAuthRequest(
            self.system, Subject("user", "user01@dingyi"), Action("flow-retrieve"), resources, None, operate="revoke"
        )
        self.iam.grant_or_revoke_instance_permission(request, bk_username="user01@dingyi")

    def test_revoke_by_path(self):
        """
        单个资源的资源授权/回收
        """
        resources = [
            ApiAuthResourceWithPath(
                IAM_REGISTERED_SYSTEM,
                "flow",
                path=[
                    {"type": "project", "id": "1111", "name": "1111"},
                    {"type": "flow", "id": "1234", "name": "1234"},
                ],
            )
        ]
        request = ApiAuthRequest(
            self.system, Subject("user", "user01@dingyi"), Action("flow-delete"), resources, None, operate="grant"
        )
        self.iam.grant_or_revoke_path_permission(request, bk_username="user01@dingyi")

    def test_revoke_by_paths(self):
        resources = [
            ApiAuthResourceWithPath(
                IAM_REGISTERED_SYSTEM, "result_table", path=[{"type": "project", "id": 147, "name": "小泽创建项目"}]
            )
        ]
        request = ApiAuthRequest(
            self.system,
            Subject("user", "user01@dingyi"),
            Action("result_table-update_data"),
            resources,
            None,
            operate="revoke",
        )
        self.iam.grant_or_revoke_path_permission(request, bk_username="user01@dingyi")

    def test_role_sync(self):
        sync_controller = RoleSync()
        user_id = "user01@dingyi"
        role_id = "raw_data.manager"
        object_id = "1"
        sync_controller.grant(user_id, role_id, object_id)

    def test_grant_by_instance_batch(self):
        instances = [{"id": 297, "name": "flow1"}]
        resources = [ApiBatchAuthResourceWithId(IAM_REGISTERED_SYSTEM, "flow", instances)]
        request = ApiBatchAuthRequest(
            self.system, Subject("user", "user01@dingyi"), [Action("flow-delete")], resources, operate="grant"
        )

        self.iam.batch_grant_or_revoke_instance_permission(request, bk_username="user02")

    def test_revoke_by_instance_batch(self):
        instances = [{"id": 297, "name": "flow1"}]

        resources = [ApiBatchAuthResourceWithId(IAM_REGISTERED_SYSTEM, "flow", instances)]
        request = ApiBatchAuthRequest(
            self.system, Subject("user", "admin@dingyi"), [Action("flow-delete")], resources, operate="revoke"
        )

        self.iam.batch_grant_or_revoke_instance_permission(request, bk_username="user02")

    def test_grant_by_path_batch(self):
        paths = [
            [{"type": "project", "id": "275", "name": "tracy test"}],
        ]
        resources = [ApiBatchAuthResourceWithPath(IAM_REGISTERED_SYSTEM, "result_table", paths)]
        request = ApiBatchAuthRequest(
            self.system,
            Subject("user", "user01@dingyi"),
            [Action("result_table-update_data")],
            resources,
            operate="grant",
        )
        self.iam.batch_grant_or_revoke_path_permission(request, bk_username="user01@dingyi")

    def test_revoke_by_path_batch(self):
        paths = [[{"type": "project", "id": "1111", "name": "project1"}]]
        resources = [ApiBatchAuthResourceWithPath(IAM_REGISTERED_SYSTEM, "flow", paths)]
        request = ApiBatchAuthRequest(
            self.system, Subject("user", "user01@dingyi"), [Action("flow-delete")], resources, operate="revoke"
        )
        self.iam.batch_grant_or_revoke_path_permission(request, bk_username="user02")

    def test_sync_role(self):
        user_id = "user01@dingyi"
        role_id = "project.manager"
        object_id = 192
        operate = "grant"
        self.role_sync.sync(user_id, role_id, object_id, operate)

    def test_query_polices_with_action_id(self):
        actions = ActionConfig.objects.filter(to_iam=True)
        user_actions_map = {}
        for action in actions:
            action_id = action.action_id.replace(".", "-")
            data = {
                "action_id": action_id,
                "page": 1,
                "page_size": 100,
            }
            try:
                polices = self.iam.query_polices_with_action_id(self.system, data)
                print(polices)
                for item in polices["results"]:
                    if item["subject"].get("type") == "user":
                        user_id = item["subject"]["id"]
                        if "content" in list(item["expression"].keys()):
                            for resource in item["expression"]["content"]:
                                user_actions_map.setdefault(user_id + "-" + action_id, []).append(
                                    self.get_path(resource["value"])
                                )
                        elif item["expression"]["op"] == "starts_with":
                            user_actions_map.setdefault(user_id + "-" + action_id, []).append(
                                self.get_path(item["expression"]["value"])
                            )
                        else:
                            if item["expression"]["op"] == "in":
                                print(item["expression"]["value"])
                                user_actions_map.setdefault(user_id + "-" + action_id, []).extend(
                                    item["expression"]["value"]
                                )
                            if item["expression"]["op"] == "eq":
                                user_actions_map.setdefault(user_id + "-" + action_id, []).append(
                                    item["expression"]["value"]
                                )
            except Exception:
                traceback.print_exc()
                continue

    def get_path(self, value):
        data = value.replace("/", "").split(",")
        return data[0] + "-" + data[1]

    def test_query_policy_details(self):
        result = self.iam.query_policy_details(self.system, 967)
        print(result)

    def test_query_user_list_by_polices(self):
        data = {"ids": "967,968"}
        result = self.iam.query_user_list_by_polices(self.system, data)
        print(result)


@skip("Here, testcases use the real iam-service")
class DoMigrateTestCase(BaseTestCase):
    def setUp(self):
        self.client = Client(IAM_REGISTERED_APP_CODE, IAM_REGISTERED_APP_SECRET, IAM_API_HOST)

    def test_query_all_model(self):
        # data = self.client.query_all_models('bk_data')
        data = self.client.api_query(IAM_REGISTERED_SYSTEM)
        print(data)
        print(data[2]["feature_shield_rules"])

    def test_api_upsert_feature(self):
        params = [
            {
                "effect": "deny",
                "feature": "user_permission.custom_permission.delete",
                "action": {"id": "raw_data.retrieve"},
            }
        ]
        ok, message = self.client.api_update_feature_shield_rules(IAM_REGISTERED_SYSTEM, params)
        print(1111)
        print((ok, message))
