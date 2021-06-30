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

from auth.handlers.resource import DashboardResource, RawDataResource
from auth.handlers.resource.manager import (
    AuthResourceManager,
    ResourceAttrFilter,
    ResourceFilter,
    ResourceIdFilter,
)
from auth.handlers.resource.storages import DgraphStorage, MySQLStorage
from auth.models.outer_models import DashboardInfo
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse


class ResourceManagerTestCase(BaseTestCase):
    """
    IAM Manager 测试用例
    """

    def setUp(self):
        self.resource_type = "raw_data"
        self.manager = AuthResourceManager.init(self.resource_type)

    def test_list_scope_attributes(self):
        attributes = [
            {"display_name": "业务ID", "id": "bk_biz_id"},
            {"display_name": "敏感度", "id": "sensitivity"},
        ]
        self.assertListEqual(self.manager.list_scope_attributes(), attributes)

    def test_get_resource_class(self):
        self.assertEqual(self.manager.get_resource_class(self.resource_type), RawDataResource)

    def test_query_scope_attribute_value(self):
        value = self.manager.query_scope_attribute_value("sensitivity", search="私有", ids=["public", "private"])
        self.assertEqual(value["count"], 1)
        self.assertEqual(value["results"][0]["id"], "private")

    @mock.patch("auth.handlers.resource.storages.MetaApi.entity_complex_search")
    def test_query(self, patch_entity_complex_search):
        patch_entity_complex_search.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "data": {
                        "results": [
                            {
                                "raw_data_id": 100312,
                                "raw_data_name": "fg555",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                            },
                            {
                                "raw_data_id": 100779,
                                "raw_data_name": "xxx555",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                            },
                        ],
                        "statistics": [{"total": 2}],
                    }
                },
            }
        )

        statement = """
            {
                var(func: eq(BKBiz.id, "591")) {
                    raw_uids as ~AccessRawData.bk_biz
                }
                var(func: uid(raw_uids))
                        @filter(
                            regexp(AccessRawData.raw_data_name, /555/i)
                            OR
                            eq(AccessRawData.id, 555)
                        ) {
                    search_uids as uid
                }
                var(func: uid(search_uids))
                        @filter(
                            eq(AccessRawData.sensitivity, "private")
                            AND
                            eq(AccessRawData.id, [100312, 100779])
                        ) {
                    attr_uids as uid
                }
                results(func: uid(attr_uids), offset: 0, first: 2) {
                        raw_data_id : AccessRawData.id
                        raw_data_name : AccessRawData.raw_data_name
                        bk_biz_id : AccessRawData.bk_biz_id
                        sensitivity : AccessRawData.sensitivity
                }
                statistics(func: uid(attr_uids)) {
                    total: count(uid)
                }
            }
        """

        result = self.manager.query(
            search="555",
            resource_filter=ResourceFilter("biz", 591),
            attr_filters=[ResourceAttrFilter("eq", "sensitivity", "private")],
            ids=[100312, 100779],
            limit=2,
        )
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["results"][0]["raw_data_id"], 100312)
        self.assertEqual(result["results"][0]["raw_data_name"], "fg555")

        self.assertEqual(
            compress_space(patch_entity_complex_search.call_args[0][0]["statement"]), compress_space(statement)
        )

    @mock.patch("auth.handlers.resource.storages.MetaApi.entity_complex_search")
    def test_get(self, patch_entity_complex_search):
        statement = """
            {
                results(func: eq(AccessRawData.id, "101404")) {
                        raw_data_id : AccessRawData.id
                        raw_data_name : AccessRawData.raw_data_name
                        bk_biz_id : AccessRawData.bk_biz_id
                        sensitivity : AccessRawData.sensitivity
                        bk_biz : AccessRawData.bk_biz {
                            bk_biz_id: BKBiz.id
                        }
                }
            }
        """
        patch_entity_complex_search.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "data": {
                        "results": [
                            {
                                "raw_data_id": 101404,
                                "raw_data_name": "pulsar_test",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                                "bk_biz": [{"bk_biz_id": 591}],
                            }
                        ]
                    }
                },
            }
        )
        resource = self.manager.get(101404)
        self.assertEqual(resource.raw_data_id, 101404)
        self.assertEqual(resource.raw_data_name, "pulsar_test")
        self.assertEqual(resource.bk_biz_id, 591)
        self.assertEqual(resource.bk_biz.bk_biz_id, 591)

        self.assertEqual(
            compress_space(patch_entity_complex_search.call_args[0][0]["statement"]), compress_space(statement)
        )

    def test_to_scopes(self):
        self.assertListEqual(
            self.manager.to_scopes(ResourceIdFilter(func="in", value=["101328", "101386"])),
            [{"raw_data_id": "101328"}, {"raw_data_id": "101386"}],
        )

        self.assertListEqual(
            self.manager.to_scopes(ResourceAttrFilter(func="eq", key="sensitivity", value="confidential")),
            [{"sensitivity": "confidential"}],
        )

        self.assertListEqual(
            self.manager.to_scopes(ResourceAttrFilter(func="eq", key="sensitivity", value="confidential")),
            [{"sensitivity": "confidential"}],
        )

        self.assertListEqual(
            self.manager.to_scopes(ResourceFilter(resource_type="biz", resource_id="873")), [{"bk_biz_id": "873"}]
        )


class DgraphStorageTestCase(BaseTestCase):
    def setUp(self):
        self.storage = DgraphStorage(RawDataResource)

    @mock.patch("auth.handlers.resource.storages.MetaApi.entity_complex_search")
    def test_query(self, patch_entity_complex_search):
        patch_entity_complex_search.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "data": {
                        "results": [
                            {
                                "raw_data_id": 100312,
                                "raw_data_name": "fg555",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                            },
                            {
                                "raw_data_id": 100779,
                                "raw_data_name": "xxx555",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                            },
                        ],
                        "statistics": [{"total": 2}],
                    }
                },
            }
        )

        statement = """
            {
                var(func: eq(BKBiz.id, "591")) {
                    raw_uids as ~AccessRawData.bk_biz
                }
                var(func: uid(raw_uids))
                        @filter(
                            regexp(AccessRawData.raw_data_name, /555/i)
                            OR
                            eq(AccessRawData.id, 555)
                        ) {
                    search_uids as uid
                }
                var(func: uid(search_uids))
                        @filter(
                            eq(AccessRawData.sensitivity, "private")
                            AND
                            eq(AccessRawData.id, [100312, 100779])
                        ) {
                    attr_uids as uid
                }
                results(func: uid(attr_uids), offset: 0, first: 2) {
                        raw_data_id : AccessRawData.id
                        raw_data_name : AccessRawData.raw_data_name
                        sensitivity : AccessRawData.sensitivity
                        bk_biz_id : AccessRawData.bk_biz_id
                }
                statistics(func: uid(attr_uids)) {
                    total: count(uid)
                }
            }
        """

        result = self.storage.query(
            search_filters=[
                ResourceAttrFilter("like", "raw_data_name", "555"),
                ResourceAttrFilter("eq", "raw_data_id", "555"),
            ],
            resource_filter=ResourceFilter("biz", 591),
            attr_filters=[
                ResourceAttrFilter("eq", "sensitivity", "private"),
                ResourceAttrFilter("in", "raw_data_id", [100312, 100779]),
            ],
            limit=2,
        )
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["results"][0]["raw_data_id"], 100312)
        self.assertEqual(result["results"][0]["raw_data_name"], "fg555")

        call_statement = compress_space(patch_entity_complex_search.call_args[0][0]["statement"])
        for s in statement.split():
            self.assertIn(s, call_statement)

    @mock.patch("auth.handlers.resource.storages.MetaApi.entity_complex_search")
    def test_get(self, patch_entity_complex_search):
        statement = """
            {
                results(func: eq(AccessRawData.id, "101404")) {
                        raw_data_id : AccessRawData.id
                        raw_data_name : AccessRawData.raw_data_name
                        sensitivity : AccessRawData.sensitivity
                        bk_biz_id : AccessRawData.bk_biz_id
                        bk_biz : AccessRawData.bk_biz {
                            bk_biz_id: BKBiz.id
                        }
                }
            }
        """
        patch_entity_complex_search.return_value = DataResponse(
            {
                "result": True,
                "data": {
                    "data": {
                        "results": [
                            {
                                "raw_data_id": 101404,
                                "raw_data_name": "pulsar_test",
                                "sensitivity": "private",
                                "bk_biz_id": 591,
                                "bk_biz": [{"bk_biz_id": 591}],
                            }
                        ]
                    }
                },
            }
        )
        resource = self.storage.get(101404)
        self.assertEqual(resource.raw_data_id, 101404)
        self.assertEqual(resource.raw_data_name, "pulsar_test")
        self.assertEqual(resource.bk_biz_id, 591)
        self.assertEqual(resource.bk_biz.bk_biz_id, 591)

        call_statement = compress_space(patch_entity_complex_search.call_args[0][0]["statement"])
        for s in statement.split():
            self.assertTrue(s in call_statement)

    def test_to_dgraph_filter(self):
        filter = ResourceAttrFilter(func="like", key="bk_biz_id", value="1")
        dgraph_filter = self.storage.to_dgraph_filter(filter)
        self.assertEqual(dgraph_filter.func, "regexp")
        self.assertEqual(dgraph_filter.key, "AccessRawData.bk_biz_id")
        self.assertEqual(dgraph_filter.value, "/1/i")


class MySQLStorageTestCase(BaseTestCase):
    def setUp(self):
        self.storage = MySQLStorage(DashboardResource)
        DashboardInfo.objects.create(
            dashboard_id=101404, dashboard_name="pulsar_test", project_id=100, description="xxxx"
        )

        DashboardInfo.objects.create(
            dashboard_id=100999, dashboard_name="pulsar_test999", project_id=111, description="xxxx"
        )

        DashboardInfo.objects.create(
            dashboard_id=100312, dashboard_name="pulsar_test", project_id=111, description="xxxx"
        )

        DashboardInfo.objects.create(
            dashboard_id=100779, dashboard_name="pulsar_test", project_id=123, description="xxxx"
        )

    def test_get(self):
        resource = self.storage.get(101404)
        self.assertEqual(resource.dashboard_id, 101404)
        self.assertEqual(resource.dashboard_name, "pulsar_test")
        self.assertEqual(resource.project.project_id, 100)

    def test_query(self):
        result = self.storage.query(
            search_filters=[
                ResourceAttrFilter("like", "dashboard_name", "999"),
                ResourceAttrFilter("eq", "dashboard_id", "999"),
            ],
            resource_filter=ResourceFilter("project", 123),
            attr_filters=[ResourceAttrFilter("in", "dashboard_id", [100312])],
            limit=4,
        )
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["results"][0]["dashboard_id"], 100312)
        self.assertEqual(result["results"][0]["dashboard_name"], "pulsar_test")


def compress_space(s):
    return " ".join(s.strip().split())
