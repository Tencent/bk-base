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
import datetime
import json
from unittest import mock

from django.conf import settings

from auth.config.ticket import PROJECT_BIZ, RESOURCE_GROUP_USE
from auth.constants import PROCESSING, PROJECT, STOPPED, SUCCEEDED, SubjectTypeChoices
from auth.core.ticket import TicketFactory
from auth.core.ticket_objects import DataTokenTicketObj
from auth.models import (
    AuthDataTokenPermission,
    DataTicketPermission,
    ProjectData,
    ProjectRawData,
    Ticket,
    TicketState,
    TokenTicketPermission,
)
from auth.models.auth_models import AuthResourceGroupRecord, UserRole
from auth.models.base_models import RoleConfig
from auth.models.outer_models import ResultTable
from auth.tests.tools.mock import create_data_token
from auth.tests.utils import BaseTestCase, build_ticket_notification, is_api_success
from common.api.base import DataResponse

GET_PROJECT_INFO = DataResponse(
    {
        "data": {
            "project_id": 1,
            "project_name": "test1",
            "bk_app_code": "data",
            "description": "dddd",
            "created_by": "trump",
            "created_at": "2018-01-01 00:00:00",
            "updated_by": "lisi",
            "updated_at": "2018-01-01 00:00:01",
            "active": 1,
            "deleted_by": "admin",
            "deleted_at": "2018-01-01 00:00:02",
            "tdw_app_groups": ["test", "test2"],
            "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
        },
        "result": True,
        "message": "",
        "code": 1500200,
        "errors": None,
    }
)

GET_RESULT_TABLE_INFO = DataResponse(
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
        "code": 1500201,
        "errors": None,
    }
)


class TicketTestCase(BaseTestCase):
    multi_db = True

    def setUp(self):
        super().setUp()
        self.dataflow_send_confirm = DataResponse({"result": True, "message": "被调用", "errors": None, "code": "1500200"})

    def test_ticket_list(self):
        self.test_generate_ticket_data_no_auto()
        resp = self.client.get("/v3/auth/tickets/?is_creator=true&bk_username=admin1234&page=1&page_size=5")
        data = is_api_success(self, resp)
        self.assertGreater(len(data), 0)

    @mock.patch("auth.api.MetaApi.get_result_table_info")
    @mock.patch("auth.api.MetaApi.get_project_info")
    def test_create_ticket_api(self, get_project_info=None, get_result_table_info=None):
        get_project_info.return_value = GET_PROJECT_INFO
        get_result_table_info.return_value = GET_RESULT_TABLE_INFO
        api_params = {
            "bk_username": "admin666",
            "reason": "hello world",
            "ticket_type": PROJECT_BIZ,
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "数据平台对内版",
                    "subject_class": PROJECT,
                    "action": "result_table.query_data",
                    "object_class": "result_table",
                    "scope": {
                        "result_table_id": "591_test_rt",
                    },
                }
            ],
        }

        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_success(resp)
        self.assertGreater(Ticket.objects.filter(created_by="admin666").count(), 0)

        # 这里仅验证结果表、项目的区域标签的合法性，简化mock数据
        get_project_info.return_value = DataResponse(
            {
                "data": {"tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}}},
                "result": True,
                "message": "",
                "code": 1500202,
                "errors": None,
            }
        )
        get_result_table_info.return_value = DataResponse(
            {
                "data": {"tags": {"manage": {"geog_area": [{"code": "CN", "alias": "中国"}]}}},
                "result": True,
                "message": "",
                "code": 1500203,
                "errors": None,
            }
        )
        api_params["permissions"][0]["scope"]["result_table_id"] = "592_test_rt"
        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_failure(resp)

        # 考虑未来可能有多区域标签的情况
        api_params["permissions"][0]["scope"]["result_table_id"] = "593_test_rt"
        get_project_info.return_value = DataResponse(
            {
                "data": {
                    "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}, {"code": "CN", "alias": "中国"}]}}
                },
                "result": True,
                "message": "",
                "code": 1500204,
                "errors": None,
            }
        )
        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_success(resp)

        # 考虑未来可能有多区域标签的情况
        api_params["permissions"][0]["scope"]["result_table_id"] = "592_test_rt"
        get_project_info.return_value = DataResponse(
            {"data": {}, "result": True, "message": "", "code": 1500205, "errors": None}
        )
        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_failure(resp)

    def test_create_data_ticket(self):
        ticket = Ticket.objects.create(
            ticket_type=PROJECT_BIZ,
            created_by="admin1234",
            created_at=datetime.datetime.now(),
            reason="hello world",
            status="processing",
            process_length=1,
            extra={},
        )
        DataTicketPermission.objects.create(
            ticket=ticket,
            subject_id="1",
            subject_name="asd",
            subject_class="wer",
            action="result_table.query_data",
            object_class="project",
            scope={"key": "value"},
        )
        self.assertEqual(1, 1)

    @mock.patch("auth.api.MetaApi.get_result_table_info", lambda return_value: GET_RESULT_TABLE_INFO)
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: GET_PROJECT_INFO)
    def test_generate_ticket_data_no_auto(self):
        data = {
            "created_by": "admin1234",
            "reason": "hello world",
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "数据平台对内版",
                    "subject_class": PROJECT,
                    "action": "result_table.query_data",
                    "object_class": "result_table",
                    "scope": {
                        "result_table_id": "591_test_rt",
                    },
                }
            ],
        }
        tickets = TicketFactory(PROJECT_BIZ).generate_ticket_data(data)
        for ticket_data in tickets:
            self.assertEqual(ticket_data.get("status"), PROCESSING)
            self.assertGreater(Ticket.objects.filter(pk=ticket_data.get("id")).count(), 0)
            self.assertGreater(DataTicketPermission.objects.filter(ticket_id=ticket_data.get("id")).count(), 0)
        return tickets

    @mock.patch("auth.api.MetaApi.get_result_table_info", lambda return_value: GET_RESULT_TABLE_INFO)
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: GET_PROJECT_INFO)
    def test_generate_ticket_data_auto(self):
        data = {
            "created_by": "processor666",
            "reason": "hello world",
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "数据平台对内版",
                    "subject_class": PROJECT,
                    "action": "result_table.query_data",
                    "object_class": "result_table",
                    "scope": {
                        "result_table_id": "591_test_rt2",
                    },
                }
            ],
        }
        tickets = TicketFactory(PROJECT_BIZ).generate_ticket_data(data)
        for ticket_data in tickets:
            self.assertEqual(ticket_data.get("status"), SUCCEEDED)
            self.assertGreater(Ticket.objects.filter(pk=ticket_data.get("id")).count(), 0)
            self.assertGreater(DataTicketPermission.objects.filter(ticket_id=ticket_data.get("id")).count(), 0)

        self.assertGreater(ProjectData.objects.filter(result_table_id="591_test_rt2", project_id="1").count(), 0)
        return tickets

    def test_query_ticket_state_api(self):
        self.test_generate_ticket_data_no_auto()
        resp = self.client.get("/v3/auth/ticket_states/?bk_username=processor666&is_processor=true&page=1&page_size=5")
        data = is_api_success(self, resp)
        self.assertGreater(len(data), 0)

    def test_ticket_state_retrive_api(self):
        tickets = self.test_generate_ticket_data_no_auto()
        state = TicketState.objects.filter(ticket=tickets[0].get("id"), status=PROCESSING).first()

        resp = self.client.get(f"/v3/auth/ticket_states/{state.pk}/")
        data = is_api_success(self, resp)
        self.assertTrue("states" in list(data.keys()))
        self.assertTrue("ticket" in list(data.keys()))

    def test_approve_state_api(self):
        tickets = self.test_generate_ticket_data_no_auto()
        state = TicketState.objects.filter(ticket=tickets[0].get("id"), status=PROCESSING).first()
        resp = self.client.post(
            f"/v3/auth/ticket_states/{state.pk}/approve/",
            {"bk_username": "processor666", "status": SUCCEEDED, "process_message": "ok"},
            format="json",
        )
        data = is_api_success(self, resp)
        self.assertEqual(data.get("status"), SUCCEEDED)
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, SUCCEEDED)

    @mock.patch("auth.handlers.project.ProjectHandler.get_data")
    def test_distinct_biz(self, patch_get_data):
        patch_get_data.return_value = [{"bk_biz_id": 591, "result_table_id": "591_test_rt22"}]
        ticket = Ticket.objects.create(
            ticket_type=PROJECT_BIZ,
            created_by="admin1234",
            created_at=datetime.datetime.now(),
            reason="hello world",
            status="processing",
            process_length=1,
            extra={},
        )
        DataTicketPermission.objects.create(
            ticket=ticket,
            subject_id="22",
            subject_name="demo_project",
            subject_class="project",
            action="result_table.query_data",
            object_class="result_table",
            scope={"result_table_id": "105_one_min_apply11"},
        )
        result = TicketFactory(PROJECT_BIZ).ticket_obj.distinct_data(22)

        self.assertEqual(len(result), 2)
        self.assertEqual(result["105_one_min_apply11"]["status"], "processing")
        self.assertEqual(result["105_one_min_apply11"]["bk_biz_id"], 105)
        self.assertTrue(type(result["105_one_min_apply11"]["ticket_id"]) == int)
        self.assertEqual(result["591_test_rt22"]["status"], "succeeded")
        self.assertEqual(result["591_test_rt22"]["bk_biz_id"], 591)
        self.assertTrue(result["591_test_rt22"]["ticket_id"] is None)

        patch_get_data.return_value = [{"bk_biz_id": 592, "raw_data_id": 333}]
        ticket = Ticket.objects.create(
            ticket_type=PROJECT_BIZ,
            created_by="admin1234",
            created_at=datetime.datetime.now(),
            reason="hello world",
            status="processing",
            process_length=1,
            extra={},
        )
        DataTicketPermission.objects.create(
            ticket=ticket,
            subject_id="22",
            subject_name="demo_project",
            subject_class="project",
            action="raw_data.query_data",
            object_class="raw_data",
            scope={"raw_data_id": "4"},
        )
        result = TicketFactory(PROJECT_BIZ).ticket_obj.distinct_data(22, action_id="raw_data.query_data")
        self.assertEqual(len(result), 2)
        self.assertEqual(result["4"]["status"], "processing")
        self.assertEqual(result["4"]["bk_biz_id"], 591)
        self.assertTrue(type(result["4"]["ticket_id"]) == int)
        self.assertEqual(result["333"]["status"], "succeeded")
        self.assertEqual(result["333"]["bk_biz_id"], 592)
        self.assertTrue(result["333"]["ticket_id"] is None)

    def test_list_ticket_types(self):
        url = "/v3/auth/tickets/ticket_types/"
        extraparam = {"HTTP_BLUEKING_LANGUAGE": "en"}
        resp = self.client.get(url, **extraparam)
        data = is_api_success(self, resp)
        self.assertGreater(len(data), 0)

    @mock.patch("auth.api.MetaApi.get_result_table_info", lambda return_value: GET_RESULT_TABLE_INFO)
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: GET_PROJECT_INFO)
    def test_create_ticket_api_autoapprove(self):
        resp = self.client.post(
            "/v3/auth/tickets/",
            {
                "bk_username": "processor666",
                "reason": "bye bye world",
                "ticket_type": PROJECT_BIZ,
                "permissions": [
                    {
                        "subject_id": "1",
                        "subject_name": "数据平台对内版",
                        "subject_class": PROJECT,
                        "action": "result_table.query_data",
                        "object_class": "result_table",
                        "scope": {
                            "result_table_id": "591_test_rt",
                        },
                    }
                ],
            },
            format="json",
        )
        is_api_success(self, resp)
        ticket = Ticket.objects.filter(created_by="processor666")
        self.assertGreater(ticket.count(), 0)
        self.assertEqual(ticket[0].status, SUCCEEDED)

    @mock.patch("auth.api.MetaApi.get_result_table_info", lambda return_value: GET_RESULT_TABLE_INFO)
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: GET_PROJECT_INFO)
    def test_create_ticket_no_approve(self):
        """
        创建不需要审批的单据
        """
        ResultTable.objects.create(
            result_table_id="592_test_rt_111111",
            bk_biz_id="592",
            project_id="111111",
            sensitivity="public",
            result_table_name="test_name",
            result_table_name_alias="test_name",
            processing_type="realtime",
            description="test_desc",
        )

        resp = self.client.post(
            "/v3/auth/tickets/",
            {
                "bk_username": "user01",
                "reason": "bye bye world",
                "ticket_type": PROJECT_BIZ,
                "permissions": [
                    {
                        "subject_id": "1",
                        "subject_name": "数据平台对内版",
                        "subject_class": PROJECT,
                        "action": "result_table.query_data",
                        "object_class": "result_table",
                        "scope": {
                            "result_table_id": "592_test_rt_111111",
                        },
                    }
                ],
            },
            format="json",
        )
        is_api_success(self, resp)
        ticket = Ticket.objects.filter(created_by="user01")
        self.assertGreater(ticket.count(), 0)
        self.assertEqual(ticket[0].states.count(), 1)
        self.assertEqual(ticket[0].status, SUCCEEDED)

    def test_withdraw_state_api(self):
        tickets = self.test_generate_ticket_data_no_auto()
        state = TicketState.objects.filter(ticket=tickets[0].get("id"), status=PROCESSING).first()
        resp = self.client.post(
            "/v3/auth/tickets/{}/withdraw/".format(tickets[0].get("id")),
            {"bk_username": "admin1234", "process_message": "withdraw"},
            format="json",
        )
        data = is_api_success(self, resp)
        self.assertEqual(data.get("status"), STOPPED)
        state = TicketState.objects.filter(pk=state.pk).first()
        self.assertEqual(state.status, STOPPED)

    @mock.patch("common.api.CmsiApi.wechat_approve_api")
    @mock.patch("common.api.CmsiApi.send_msg")
    @mock.patch("auth.api.MetaApi.get_result_table_info", lambda return_value: GET_RESULT_TABLE_INFO)
    @mock.patch("auth.api.MetaApi.get_project_info", lambda return_value: GET_PROJECT_INFO)
    def test_create_ticket_send_notice(self, wechat_approve_api, send_msg):
        # 单据测试默认是关闭通知信号的，这里重新绑定
        build_ticket_notification()
        send_result = DataResponse({"data": None, "result": True, "message": "", "code": 1500200, "errors": None})
        wechat_approve_api.return_value = send_result
        send_msg.return_value = send_result
        # 创建一个对应的结果表审核人，完成发送单据请求的测试
        ResultTable.objects.create(
            result_table_id="123_test_rt", project_id=10, bk_biz_id=123, description="test", sensitivity="private"
        )
        rt_manager = RoleConfig.objects.get(pk="result_table.manager")
        # 如需接收消息，请更改user
        UserRole.objects.create(user_id="admin", role_id=rt_manager.role_id, scope_id="123_test_rt")

        api_params = {
            "bk_username": "admin666",
            "reason": "hello world",
            "ticket_type": PROJECT_BIZ,
            "permissions": [
                {
                    "subject_id": "10",
                    "subject_name": "数据平台对内版",
                    "subject_class": PROJECT,
                    "action": "result_table.query_data",
                    "object_class": "result_table",
                    "scope": {
                        "result_table_id": "123_test_rt",
                    },
                }
            ],
        }

        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_success(resp)
        self.assertEqual(send_msg.called, True)

        if settings.SMCS_WEIXIN_SWITCH:
            self.assertEqual(wechat_approve_api.called, True)


class DataTokenTicketTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()

    def create_ticket(self):
        o_token = create_data_token()

        o_ticket = Ticket.objects.create(
            ticket_type="token_data",
            created_by="admin",
            created_at=datetime.datetime.now(),
            reason="xxx",
            process_length=1,
        )
        TokenTicketPermission.objects.create(
            ticket=o_ticket,
            subject_id=o_token.id,
            subject_name="testapp",
            subject_class="token",
            action="result_table.query_queue",
            object_class="result_table",
            key="result_table_id",
            value="1_xxx",
        )

        return o_ticket

    @mock.patch("auth.models.AuthDataToken.update_permission_status")
    @mock.patch("auth.tasks.sync_queue_auth.add_queue_auth.delay")
    def test_update_permission(self, patch1, patch2):
        patch1.result_value = None
        patch2.result_value = None

        o_ticket = self.create_ticket()
        DataTokenTicketObj("token_data").update_permission(o_ticket, "active")

        perms = TokenTicketPermission.objects.filter(ticket=o_ticket)
        data_token_id = perms[0].subject_id

        patch1.assert_called_with([[data_token_id, "1_xxx"]])
        patch2.assert_called_once()

    def test_multi_biz_token_permission(self):
        """
        授权码申请多个业务的结果表查询权限，且仅部分表有权限自动审批
        """

        # DataToken 已经改造为申请模式，没权限也可以申请
        # data = {
        #     "bk_username": "592_biz_manager",
        #     "data_token_bk_app_code": "test_app",
        #     "data_scope": {
        #         "is_all": False,
        #         "permissions": [
        #             {
        #                 "action_id": "result_table.query_data",
        #                 "object_class": "result_table",
        #                 "scope_id_key": "result_table_id",
        #                 "scope_name_key": "result_table_name",
        #                 "scope_object_class": "result_table",
        #                 "scope": {
        #                     "result_table_id": "592_test_rt"
        #                 },
        #                 "scope_display": {
        #                     "result_table_name": "592_test_rt_display"
        #                 }
        #             },
        #             # 用户592_biz_manager没有结果表591_test_rt的权限
        #             {
        #                 "action_id": "result_table.query_data",
        #                 "object_class": "result_table",
        #                 "scope_id_key": "result_table_id",
        #                 "scope_name_key": "result_table_name",
        #                 "scope_object_class": "result_table",
        #                 "scope": {
        #                     "result_table_id": "591_test_rt"
        #                 },
        #                 "scope_display": {
        #                     "result_table_name": "591_test_rt_display"
        #                 }
        #             }
        #         ]
        #     },
        #     "reason": "xxx",
        #     "expire": 365
        # }
        # resp = self.client.post('/v3/auth/tokens/', data, format='json')
        # self.is_api_failure(resp)

        data = {
            "bk_username": "processor666",
            "data_token_bk_app_code": "test_app",
            "data_scope": {
                "is_all": False,
                "permissions": [
                    {
                        "action_id": "result_table.query_data",
                        "object_class": "result_table",
                        "scope_id_key": "result_table_id",
                        "scope_name_key": "result_table_name",
                        "scope_object_class": "result_table",
                        "scope": {"result_table_id": "591_test_rt"},
                        "scope_display": {"result_table_name": "591_test_rt_display"},
                    },
                    # 用户processor666有结果表666_test_rt的查看权限，但是没有审批权限
                    {
                        "action_id": "result_table.query_data",
                        "object_class": "result_table",
                        "scope_id_key": "result_table_id",
                        "scope_name_key": "result_table_name",
                        "scope_object_class": "result_table",
                        "scope": {"result_table_id": "666_test_rt"},
                        "scope_display": {"result_table_name": "666_test_rt_display"},
                    },
                ],
            },
            "reason": "xxx",
            "expire": 365,
        }

        resp = self.client.post("/v3/auth/tokens/", data, format="json")
        self.is_api_success(resp)
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(
                action_id="result_table.query_data",
                scope_id="591_test_rt",
                status="active",
                data_token__data_token=json.loads(resp.content)["data"],
            ).count()
            == 1
        )
        self.assertTrue(
            AuthDataTokenPermission.objects.filter(
                action_id="result_table.query_data",
                scope_id="666_test_rt",
                status="applying",
                data_token__data_token=json.loads(resp.content)["data"],
            ).count()
            == 1
        )


class ResourceGroupTestCase(BaseTestCase):
    def test_create_ticket(self):
        UserRole.objects.create(user_id="user02", role_id="resource_group.manager", scope_id="mysql_lol_2")

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
        tickets = TicketFactory(RESOURCE_GROUP_USE).generate_ticket_data(data)
        for ticket in tickets:
            self.assertEqual(ticket.get("status"), PROCESSING)
            self.assertGreater(Ticket.objects.filter(pk=ticket.get("id")).count(), 0)
            self.assertGreater(DataTicketPermission.objects.filter(ticket_id=ticket.get("id")).count(), 0)
        return tickets

    def test_create_ticket_api(self):

        UserRole.objects.create(user_id="user01", role_id="resource_group.manager", scope_id="mysql_lol_2")

        api_params = {
            "bk_username": "user01",
            "reason": "hello world",
            "ticket_type": RESOURCE_GROUP_USE,
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

        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_success(resp)
        self.assertEqual(Ticket.objects.filter(created_by="user01", status="succeeded").count(), 1)
        self.assertEqual(AuthResourceGroupRecord.objects.all().count(), 1)


class ProjectDataTestCase(BaseTestCase):
    @mock.patch("auth.core.ticket_serializer.RawDataHandler")
    def test_create_ticket_api(self, patch_handler):
        # patch_handler.geo_tags = ['inland']

        UserRole.objects.create(user_id="user01", role_id="biz.manager", scope_id=591)

        api_params = {
            "bk_username": "user01",
            "reason": "hello world",
            "ticket_type": PROJECT_BIZ,
            "permissions": [
                {
                    "subject_id": "1",
                    "subject_name": "项目1",
                    "subject_class": PROJECT,
                    "action": "raw_data.query_data",
                    "object_class": "raw_data",
                    "scope": {
                        "raw_data_id": 4,
                    },
                }
            ],
        }

        resp = self.client.post("/v3/auth/tickets/", api_params, format="json")
        self.is_api_success(resp)
        self.assertEqual(Ticket.objects.filter(created_by="user01", status="succeeded").count(), 1)
        self.assertEqual(ProjectRawData.objects.all().count(), 1)
