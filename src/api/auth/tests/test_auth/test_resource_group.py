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

from auth.constants import SubjectTypeChoices
from auth.handlers.resource_group import ResourceGroupHandler, ResourceGroupSubject
from auth.models.auth_models import AuthResourceGroupRecord
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse


class ResourceGroupTestCase(BaseTestCase):
    def test_add_authorization(self):
        handler = ResourceGroupHandler()
        handler.add_authorization(SubjectTypeChoices.PROJECT, "1", "mysql_lol_2", "user01")
        handler.add_authorization(SubjectTypeChoices.PROJECT, "1", "mysql_lol_2", "user01")

        self.assertEqual(AuthResourceGroupRecord.objects.all().count(), 1)

    @mock.patch("auth.api.MetaApi.cluster_group_configs")
    def test_list_authorized_cluster_group(self, patch_cluster_group_configs):
        patch_cluster_group_configs.return_value = DataResponse(
            {
                "result": True,
                "data": [
                    {
                        "tags": {"manage": {"geog_area": [{"alias": "中国内地", "code": "inland"}]}},
                        "cluster_group_id": "mysql_lol_2",
                        "cluster_group_alias": "mysql_lol_2",
                        "cluster_group_name": "mysql_lol_2",
                    },
                    {
                        "tags": {"manage": {"geog_area": [{"alias": "中国内地", "code": "inland"}]}},
                        "cluster_group_id": "mysql_lol_1",
                        "cluster_group_alias": "mysql_lol_1",
                        "cluster_group_name": "mysql_lol_1",
                    },
                    {
                        "tags": {"manage": {"geog_area": [{"alias": "中国内地", "code": "inland"}]}},
                        "cluster_group_id": "mysql_lol_3",
                        "cluster_group_alias": "mysql_lol_3",
                        "cluster_group_name": "mysql_lol_3",
                    },
                ],
            }
        )

        handler = ResourceGroupHandler()
        handler.add_authorization(SubjectTypeChoices.PROJECT, "1", "mysql_lol_2", "user01")

        groups = handler.list_authorized_cluster_group(
            [ResourceGroupSubject(SubjectTypeChoices.PROJECT, "1")], ["inland"]
        )
        self.assertEqual([g["cluster_group_id"] for g in groups], ["mysql_lol_2", "mysql_lol_1"])
        self.assertEqual([g["resource_group_id"] for g in groups], ["mysql_lol_2", "mysql_lol_1"])

    def test_list_authorized_subjects_api(self):
        handler = ResourceGroupHandler()
        handler.add_authorization(SubjectTypeChoices.PROJECT, "1", "mysql_lol_1", "user01")

        resp = self.client.get("/v3/auth/resource_groups/mysql_lol_1/authorized_subjects/")
        data = self.is_api_success(resp)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["subject_id"], "1")
        self.assertEqual(data[0]["subject_type"], "project_info")
