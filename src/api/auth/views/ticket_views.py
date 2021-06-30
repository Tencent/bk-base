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
from auth.constants import FAILED, PROCESSING, SUCCEEDED
from auth.core.ticket import TicketFactory
from auth.core.ticket_notification import TicketNotice
from auth.core.ticket_objects import BaseTicketSerializer
from auth.core.ticket_serializer import TicketStateSerializer
from auth.models import Ticket
from auth.models.ticket_models import TicketState
from auth.utils.serializer import DataPageNumberPagination
from auth.views.ticket_serializer import (
    CommonTicketCreateSerializer,
    TicketQuerySerializer,
    TicketStateApproveSerializer,
    TicketStateQuerySerializer,
    TicketStateRetrieveSerializer,
    TicketWithdrawSerializer,
    WXRecallTicketStateApproveSerializer,
)
from common.decorators import detail_route, list_route, params_valid
from common.views import APIModelViewSet
from django.db.models import F
from rest_framework.response import Response

from common import local


class TicketViewSet(APIModelViewSet):
    queryset = Ticket.objects.none()
    serializer_class = BaseTicketSerializer
    pagination_class = DataPageNumberPagination

    # 单据内容暂时不需要加上权限控制
    def get_queryset(self):
        return Ticket.objects.all()

    def serialize_queryset(self, query_params):
        """
        序列化query_set
        @param query_params:
        @return:
        """
        queryset = self.get_queryset().filter(**query_params).all()

        page = self.paginate_queryset(queryset)
        if page is not None:
            data = TicketFactory.serialize_ticket_queryset(page, many=True)
            return self.get_paginated_response(data)

        data = TicketFactory.serialize_ticket_queryset(queryset, many=True)
        return Response(data)

    @params_valid(TicketQuerySerializer, add_params=False)
    def list(self, request, *args, **kwargs):
        """
        @api {get} /v3/auth/tickets/ 获取单据列表
        @apiName ticket_list
        @apiGroup Ticket
        @apiParam {boolean} [is_creator] 是否为创建人
        @apiParam {boolean} [is_processor] 是否为处理候选人
        @apiParam {boolean} [is_processed_by] 是否为处理人
        @apiParam {string} [process_id] 流程ID，仅对通用单据生效，比如 verify_tdm_data、batch_recalc
        @apiSuccessExample {Json} 成功返回
        [
            {
                "id": 41,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-18 11:59:22",
                "reason": "hello world",
                "status": "succeeded",
                "end_time": "2018-12-18 12:06:25",
                "extra": "{}",
                "process_step": 1,
                "permissions": [
                    {
                        "id": 54,
                        "ticket": "Ticket object",
                        "key": "result_table_id",
                        "value": "591_test_rt",
                        "scope": {
                            "result_table_name": "test_rt",
                            "result_table_info": {
                                "id": "591_test_rt",
                                "name": "test_rt"
                            },
                            "result_table_id": "591_test_rt"
                        },
                        "subject_id": "1",
                        "subject_name": "数据平台对内版",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "result_table"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "成功"
            }
        ]
        """
        username = local.get_request_username()
        status = request.cleaned_params.get("status")
        ticket_type = request.cleaned_params.get("ticket_type")
        process_id = request.query_params.get("process_id")

        query_params = {}
        if request.cleaned_params.get("is_creator"):
            query_params["created_by"] = username
        if request.query_params.get("is_processor"):
            query_params["states__processors__contains"] = Ticket.generate_sub_member(username)
        if request.query_params.get("is_processed_by"):
            query_params["states__processed_by"] = username
        if request.query_params.get("present"):
            query_params["states__process_step__lte"] = F("process_step")
        if status:
            query_params["status"] = status
        if ticket_type:
            query_params["ticket_type"] = ticket_type
        if process_id:
            query_params["process_id"] = process_id

        return self.serialize_queryset(query_params)

    def create(self, request, *args, **kwargs):
        """
        @api {post} /v3/auth/tickets/ 创建单据
        @apiName create_ticket
        @apiGroup Ticket
        @apiParam {String} reason 创建原因
        @apiParam {String} ticket_type 单据类型
        @apiParam {Json} permissions 申请的权限列表
        @apiParamExample {Json} 项目申请结果数据样例
            {
                "reason": "hello world",
                "ticket_type": "project_biz",
                "permissions": [
                    {
                        "subject_id": "1",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "result_table",
                        "scope": {
                            "result_table_id": "591_test_rt"
                        }
                    }
                ]
            }
        @apiParamExample {Json} 项目申请资源组样例
            {
                "reason": "hello world",
                "ticket_type": "use_resource_group",
                "permissions": [
                    {
                        "subject_id": "1",
                        "subject_class": "project_info",
                        "action": "resource_group.use",
                        "object_class": "resource_group",
                        "scope": {
                            "resource_group_id": "tgqa"
                        }
                    }
                ]
            }
        @apiSuccessExample {Json} 项目申请结果数据返回样例结果
            {
                "id": 45,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-21 11:55:39",
                "reason": "hello world",
                "status": "processing",
                "end_time": null,
                "extra": null,
                "process_step": 0,
                "permissions": [
                    {
                        "id": 58,
                        "ticket": "Ticket object",
                        "key": "result_table_id",
                        "value": "591_test_rt",
                        "scope": {
                            "result_table_name": "test_rt",
                            "result_table_info": {
                                "id": "591_test_rt",
                                "name": "test_rt"
                            },
                            "result_table_id": "591_test_rt"
                        },
                        "subject_id": "1",
                        "subject_name": "数据平台对内版",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "result_table"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "处理中"
            }
        """
        data = request.data.copy()
        data["created_by"] = local.get_request_username()
        return Response(TicketFactory(data["ticket_type"]).generate_ticket_data(data))

    def retrieve(self, request, *args, **kwargs):
        """
        @api {get} /v3/auth/tickets/:ticket_id/ 查询单据
        @apiName ticket_query
        @apiGroup Ticket
        @apiSuccessExample {Json} 成功返回 Response.data
            {
                "id": 45,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-21 11:55:39",
                "reason": "hello world",
                "status": "processing",
                "end_time": null,
                "extra": "{}",
                "process_step": 0,
                "permissions": [
                    {
                        "id": 58,
                        "ticket": "Ticket object",
                        "key": "result_table_id",
                        "value": "591_test_rt",
                        "scope": {
                            "result_table_name": "test_rt",
                            "result_table_info": {
                                "id": "591_test_rt",
                                "name": "test_rt"
                            },
                            "result_table_id": "591_test_rt"
                        },
                        "subject_id": "1",
                        "subject_name": "数据平台对内版",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "result_table"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "处理中"
            }
        """
        ticket = self.get_object()
        data = TicketFactory.serialize_ticket_queryset(ticket)
        return Response(data)

    @list_route(methods=["get"])
    def ticket_types(self, request):
        """
        @api {get} /v3/auth/tickets/ticket_types/ 获取单据类型
        @apiName ticket_types
        @apiGroup Ticket
        @apiSuccessExample {Json} 成功返回
        [
            {id: 'APP', name: '业务数据'},
        ]
        """
        return Response(TicketFactory.list_ticket_types())

    @list_route(methods=["get"])
    def ticket_status(self, request):
        """
        @api {get} /v3/auth/tickets/ticket_status/ 获取单据状态列表
        @apiName ticket_types
        @apiGroup Ticket
        @apiSuccessExample {Json} 成功返回
        [
            {id: 'processing', name: '处理中'},
        ]
        """
        return Response(TicketFactory.list_ticket_status())

    @list_route(methods=["post"])
    @params_valid(CommonTicketCreateSerializer, add_params=False)
    def create_common(self, request):
        """
        @api {post} /v3/auth/tickets/create_common/ 创建单据
        @apiName create_common_ticket
        @apiGroup Ticket
        @apiParam {String} reason 创建原因
        @apiParam {String} ticket_type 单据类型
        @apiParam {String} process_id 任务ID
        @apiParam {String} content 任务描述
        @apiParam {String} callback_url 回调 URL，审批结果会通过 callback_url 进行回调
        @apiParam {Dict} [ticket_step_params] 单据步骤参数
        @apiParam {String} [auto_approve] 是否自动审批
        @apiParamExample {Json} verify_tdm_data 请求示例
            {
                "reason": "hello world",
                "ticket_type": "verify_tdm_data",
                "process_id": "111",
                "content": "用户要接入 TDM XX 表数据，是否允许",
                "callback_url": "http://xxx.com/xxx"
            }
        @apiParamExample {Json} create_resource_group 请求示例
            {
                "reason": "hello world",
                "ticket_type": "create_resource_group",
                "ticket_step_params": {
                    "0&process_scope_id": "591"   # 表示第一步的 process_scope_id 参数赋值为 591
                },
                "process_id": "111",
                "content": "现在要创建 XXX 资源组",
                "callback_url": "http://xxx.com/xxx"
            }
        @apiSuccessExample {Json} 成功返回
            {
                "id": 420,
                "ticket_type_display": "TDM原始数据接入申请",
                "status_display": "处理中",
                "process_length": 1,
                "ticket_type": "verify_tdw_data",
                "created_by": "request_user",
                "created_at": "2019-06-24 15:57:42",
                "reason": "test",
                "status": "processing",
                "end_time": null,
                "process_step": 0,
                "processors": ["admin"],
                "state_id": 358,
                "extra_info": {
                    "content": "用户要接入 TDM XX 表数据，是否允许",
                    "process_id": "111",
                    "callback_url": "http://xxx.com/xxx"
                },
                "permission": []
            }
        @apiSuccessExample {Json} 回调示例
            curl -XPOST http://xxx.com/xxx -d
            {
                "operator": "user01",
                "message": "ok",
                "process_id": "111",
                "status": "succeeded"  # succeeded=通过 | failed=拒绝 | stopped=中止
            }
        """
        data = request.cleaned_params
        data_restr = {}
        data_restr["created_by"] = local.get_request_username()
        data_restr["ticket_type"] = data["ticket_type"]
        data_restr["reason"] = data["reason"]
        data_restr["ticket_step_params"] = data.get("ticket_step_params", {})
        data_restr["auto_approve"] = data.get("auto_approve", True)
        extra_info = {
            "process_id": data["process_id"],
            "ticket_step_params": data_restr["ticket_step_params"],
            "content": data["content"],
            "callback_url": data.get("callback_url"),
        }
        data_restr["extra"] = extra_info
        data_restr["permission"] = []
        res = TicketFactory(data_restr["ticket_type"]).generate_ticket_data(data_restr)[0]
        return Response(res)

    @detail_route(methods=["post"], url_path="withdraw")
    @params_valid(TicketWithdrawSerializer, add_params=False)
    def withdraw(self, request, pk):
        """
        @api {post} /v3/auth/tickets/:id/withdraw/ 撤销单据
        @apiName withdraw_ticket
        @apiGroup Ticket
        @apiParam {String} process_message 处理信息
        @apiSuccessExample {json}
        {
            "id": 74,
            "extra": {
                "content": "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
                "process_id": "111_xx"
            },
            "ticket_type_display": "离线补录申请",
            "status_display": "已终止",
            "process_length": 1,
            "ticket_type": "batch_recalc",
            "created_by": "request_user",
            "created_at": "2019-06-28 18:10:50",
            "reason": "test",
            "status": "stopped",
            "end_time": "2019-06-28 18:10:50",
            "process_step": 0,
            "processors": ["admin"],
            "state_id": 74,
            "extra_info": {
                "content": "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
                "process_id": "111_xx"
            }
        }
        """
        ticket = self.get_object()
        username = local.get_request_username()
        result = TicketFactory.withdraw(
            ticket,
            request.cleaned_params["process_message"],
            username,
        )
        return Response(result)


class TicketStateViewSet(APIModelViewSet):
    queryset = TicketState.objects.none()
    serializer_class = TicketStateSerializer
    pagination_class = DataPageNumberPagination

    # 单据内容暂时不需要做权限控制
    def get_queryset(self):
        """
        返回所有审批步骤，需要考虑下是否加上用户过滤环节
        """
        return TicketState.objects.all()

    def serialize_queryset(self, query_params, show_display=False):
        """
        序列化query_set
        @param query_params: 查询参数
        @param show_display: 是否返回显示字段
        @return:
        """
        queryset = self.get_queryset().filter(**query_params).all()

        paged_queryset = self.paginate_queryset(queryset)
        if paged_queryset is not None:
            data = TicketFactory.serialize_state_queryset(paged_queryset, many=True, show_display=show_display)
            return self.get_paginated_response(data)

        data = TicketFactory.serialize_state_queryset(queryset, many=True, show_display=show_display)
        return Response(data)

    @params_valid(TicketStateQuerySerializer, add_params=False)
    def list(self, request, *args, **kwargs):
        """
        @api {get} /v3/auth/ticket_states/ 获取单据列表
        @apiName ticket_state_list
        @apiGroup TicketState
        @apiParam {int} ticket_id 单据标识
        @apiParam {boolean} is_processor 是否为处理候选人
        @apiParam {boolean} is_processed_by 是否为处理人
        @apiSuccessExample {json} 查询结果样例
        [
            {
                "id": 1,
                "processed_at": "2018-12-18 12:06:26",
                "processors": [
                    "processor666"
                ],
                "processed_by": "processor666",
                "process_step": 0,
                "process_message": "hello you are succeeded",
                "status": "succeeded",
                "ticket": {
                    "id": 41,
                    "process_length": 1,
                    "ticket_type": "project_biz",
                    "created_by": "admin1234",
                    "created_at": "2018-12-18 11:59:22",
                    "reason": "hello world",
                    "status": "succeeded",
                    "end_time": "2018-12-18 12:06:25",
                    "extra": "{}",
                    "process_step": 1,
                    "permissions": [
                        {
                            "id": 54,
                            "ticket": "Ticket object",
                            "key": "result_table_id",
                            "value": "591_test_rt",
                            "scope": {
                                "result_table_name": "test_rt",
                                "result_table_info": {
                                    "id": "591_test_rt",
                                    "name": "test_rt"
                                },
                                "result_table_id": "591_test_rt"
                            },
                            "subject_id": "1",
                            "subject_name": "数据平台对内版",
                            "subject_class": "project",
                            "action": "result_table.query_data",
                            "object_class": "result_table"
                        }
                    ],
                    "ticket_type_display": "项目申请业务数据",
                    "status_display": "成功"
                },
                "status_display": "成功"
            }
        ]
        """
        query_params = {}
        username = local.get_request_username()
        status = request.cleaned_params.get("status")
        ticket__ticket_type = request.cleaned_params.get("ticket__ticket_type")
        is_processor = request.cleaned_params.get("is_processor")
        is_processed_by = request.cleaned_params.get("is_processed_by")
        ticket_id = request.cleaned_params.get("ticket_id")
        if ticket_id:
            query_params["ticket_id"] = ticket_id
        if is_processor:
            query_params["processors__contains"] = Ticket.generate_sub_member(username)
            query_params["status__in"] = [PROCESSING, SUCCEEDED, FAILED]
        if status:
            query_params["status"] = status
        if ticket__ticket_type:
            query_params["ticket__ticket_type"] = ticket__ticket_type
        if is_processed_by:
            query_params["processed_by"] = username

        return self.serialize_queryset(query_params)

    @params_valid(TicketStateRetrieveSerializer, add_params=False)
    def retrieve(self, request, pk):
        """
        @api {get} /v3/auth/ticket_states/:ticket_state_id/ 查询单据及关联状态
        @apiName approve_state
        @apiGroup TicketState
        @apiParam {String} status 审批状态
        @apiParam {String} process_message 处理信息
        @apiSuccessExample {json} 审批结果样例
        {
            states: [
                {
                    id: 1702,
                    status_display: "已同意",
                    processed_at: "2019-09-17 15:19:35",
                    processors: [
                        "user01"
                    ],
                    processed_by: "user01",
                    process_step: 1,
                    process_message: "同意",
                    status: "succeeded"
                }
            ],
            "ticket": {
                "id": 42,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-18 19:47:00",
                "reason": "adf",
                "status": "processing",
                "end_time": null,
                "extra": "{}",
                "process_step": 0,
                "permissions": [
                    {
                        "id": 55,
                        "ticket": "Ticket object",
                        "key": "bk_biz_id",
                        "value": "591",
                        "scope": {
                            "bk_biz_id": "591",
                            "bk_biz_info": {
                                "id": "591",
                                "name": "unknown biz"
                            },
                            "bk_biz_name": "unknown biz"
                        },
                        "subject_id": "sdf",
                        "subject_name": "",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "biz"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "处理中"
            },
            "status_display": "处理中"
        }
        """
        instance = self.get_object()
        instance_data = TicketFactory.serialize_state_queryset(
            instance, ticket_type=instance.ticket.ticket_type, many=False
        )
        data = self.serialize_queryset({"ticket_id": instance_data["ticket"]["id"]}, show_display=True).data
        ticket = {}
        for _data in data:
            ticket = _data.pop("ticket")
        return Response({"ticket": ticket, "states": data})

    @detail_route(methods=["post"], url_path="approve")
    @params_valid(TicketStateApproveSerializer, add_params=False)
    def approve(self, request, pk):
        """
        @api {post} /v3/auth/ticket_states/:id/approve/ 审批单据
        @apiName approve_state
        @apiGroup TicketState
        @apiParam {String} status 审批状态
        @apiParam {String} process_message 处理信息
        @apiSuccessExample {json} 审批结果样例
        {
            "id": 3,
            "processed_at": "2018-12-21 12:10:02",
            "processors": [
                "processor666"
            ],
            "processed_by": "processor666",
            "process_step": 0,
            "process_message": "hello you are succeeded",
            "status": "succeeded",
            "ticket": {
                "id": 43,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-18 20:02:52",
                "reason": "asdf",
                "status": "succeeded",
                "end_time": "2018-12-21 12:10:02",
                "extra": "{}",
                "process_step": 1,
                "permissions": [
                    {
                        "id": 56,
                        "ticket": "Ticket object",
                        "key": "bk_biz_id",
                        "value": "591",
                        "scope": {
                            "bk_biz_id": "591",
                            "bk_biz_info": {
                                "id": "591",
                                "name": "unknown biz"
                            },
                            "bk_biz_name": "unknown biz"
                        },
                        "subject_id": "213",
                        "subject_name": "goodjob",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "biz"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "成功"
            },
            "status_display": "成功"
        }
        """
        instance = self.get_object()
        username = local.get_request_username()
        result = TicketFactory.approve(
            instance,
            request.cleaned_params["status"],
            request.cleaned_params["process_message"],
            username,
            add_biz_list=True,
        )
        return Response(result)

    @list_route(methods=["post"])
    @params_valid(WXRecallTicketStateApproveSerializer, add_params=False)
    def wx_recall(self, request):
        """
        @api {post} /v3/auth/ticket_states/wx_recall/ 审批单据
        @apiName approve_state
        @apiGroup TicketState
        @apiParam {String} status 审批状态
        @apiParam {String} taskid bkdata:ticket_id:md5(ticket_id+APP_TOKEN)
        @apiSuccessExample {json} 审批结果样例
        {
            "id": 3,
            "processed_at": "2018-12-21 12:10:02",
            "processors": [
                "processor666"
            ],
            "processed_by": "processor666",
            "process_step": 0,
            "process_message": "hello you are succeeded",
            "status": "succeeded",
            "ticket": {
                "id": 43,
                "process_length": 1,
                "ticket_type": "project_biz",
                "created_by": "admin1234",
                "created_at": "2018-12-18 20:02:52",
                "reason": "asdf",
                "status": "succeeded",
                "end_time": "2018-12-21 12:10:02",
                "extra": "{}",
                "process_step": 1,
                "permissions": [
                    {
                        "id": 56,
                        "ticket": "Ticket object",
                        "key": "bk_biz_id",
                        "value": "591",
                        "scope": {
                            "bk_biz_id": "591",
                            "bk_biz_info": {
                                "id": "591",
                                "name": "unknown biz"
                            },
                            "bk_biz_name": "unknown biz"
                        },
                        "subject_id": "213",
                        "subject_name": "goodjob",
                        "subject_class": "project",
                        "action": "result_table.query_data",
                        "object_class": "biz"
                    }
                ],
                "ticket_type_display": "项目申请业务数据",
                "status_display": "成功"
            },
            "status_display": "成功"
        }
        """
        taskid = request.cleaned_params["taskid"]
        ticket_id = int(taskid.split(":")[1])
        TicketNotice().check_taskid(ticket_id, taskid)
        instance = TicketState.objects.filter(ticket=ticket_id, status=PROCESSING).first()
        username = request.cleaned_params["verifier"]
        status = request.cleaned_params["result"]
        result_status = FAILED
        if status == "1":
            result_status = SUCCEEDED
        result = TicketFactory.approve(
            instance, result_status, request.cleaned_params["message"], username, add_biz_list=True
        )
        return Response(result)

    @list_route(methods=["get"])
    def todo_count(self, request):
        return Response(TicketState.todo_count(local.get_request_username()))
