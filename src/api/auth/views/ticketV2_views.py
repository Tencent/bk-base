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
from auth.itsm.ticket import ItsmTicketManager
from auth.models.ticketV2_models import AuthCommonTicket
from auth.utils.serializer import DataPageNumberPagination
from auth.views.ticketV2_serializer import (
    CommonTicketSerializer,
    ItsmCallBackSerializer,
    OperateTicketSerializer,
)
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIModelViewSet
from rest_framework.response import Response


class ItsmTicketViewSet(APIModelViewSet):
    queryset = AuthCommonTicket.objects.all()
    pagination_class = DataPageNumberPagination
    serializer_class = CommonTicketSerializer

    @params_valid(CommonTicketSerializer, add_params=False)
    def create(self, request, *args, **kwargs):
        """
        @api {post} /v3/auth/itsm_ticket/ 创建单据
        @apiName create_ticket
        @apiGroup Ticket
        @apiParam {String} creator 创建人
        @apiParam {String} ticket_type 单据类型
        @apiParam {Json} fields 申请的权限列表
        @apiParam {int} itsm_service_id itsm服务id
        @apiParam {String} callback_url 回调URL
        @apiParamExample {Json} 角色申请结果数据样例
          {
            "ticket_type": "apply_role",
            "creator": "admin",
            "itsm_service_id": "77",
            "callback_url" : "http://xxx,
            "fields" : {
                "title": "角色申请",
                "role_id": "result_table.manager",
                "role_name": "",
                "user_id": "admin",
                "scope_id": "591_test_rt",
                "apply_reason": "hello",
                "action":"add_role"
            }
        }
        @apiSuccessExample {Json} 角色权限申请结果数据返回样例结果
          {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "id": 82,
                "ticket_type_display": "申请角色",
                "created_by": "admin",
                "fields": {
                    "apply_reason": "hello",
                    "user_id": "admin",
                    "title": "角色申请",
                    "role_id": "result_table.manager",
                    "scope_id": "591_test_rt",
                    "action": "add_role"
                },
                "ticket_type": "apply_role",
                "itsm_service_id": 77,
                "itsm_sn": "CRQ2021000000014",
                "callback_url": "http:/xxx",
                "itsm_status": "RUNNING",
                "approve_result": false,
                "created_at": "2021-02-01 20:26:13",
                "end_time": "2021-02-01 20:26:13"
            },
            "result": true
        }
        """
        data = request.cleaned_params
        ticket = CommonTicketSerializer(ItsmTicketManager(data["ticket_type"]).generate_ticket(data)).data
        return Response(ticket)

    @detail_route(methods=["post"], url_path="operate")
    @params_valid(OperateTicketSerializer, add_params=False)
    def operate(self, request, pk):
        """
        @api {post} /v3/auth/itsm_ticket/:id/operate/ 操作单据
        @apiName operate_ticket
        @apiGroup Ticket
        @apiParam {String} action_type 操作类型
        @apiParam {String} action_message 操作备注
        @apiParamExample {Json} 单据操作数据样例
        {
            "action_type": "SUSPEND",
            "action_message": "test"
         }
        @apiSuccessExample {Json} 单据操作结果数据返回样例结果
        {
            "message": "success",
            "code": 0,
            "data": null,
            "result": true
        }
        """
        instance = self.get_object()
        request.data["operator"] = get_request_username()
        ItsmTicketManager(instance.ticket_type).operate_ticket(instance, request.data)
        return Response("ok")

    @list_route(methods=["POST"])
    @params_valid(serializer=ItsmCallBackSerializer, add_params=False)
    def call_back(self, request, *args, **kwargs):
        """
        @api {post} /v3/auth/itsm_call_back/ itsm回调接口
        @apiName
        @apiGroup Ticket
        @apiParam {String} sn 单号
        @apiParam {String} approve_result 操作备注
        @apiParamExample {Json} ITSM回调操作数据样例
        {
            "sn": "REQ20200831000005",
            "title": "测试内置审批",
            "ticket_url": "https://localhost//#/commonInfo?id=934&amp;activeNname=all&amp;router=request",
            "current_status": "FINISHED",
            "updated_by": "admin",
            "update_at": "2020-08-31 20:57:22",
            "approve_result": true,
            "token": "abcasdsdsds"
        }
        @apiSuccessExample {Json} ITSM回调结果数据返回样例结果
        {
            "message": "success",
            "code": 0,
            "data": null,
            "result": true
        }
        """
        data = request.cleaned_params
        ticket = AuthCommonTicket.objects.get(itsm_sn=data.get("sn"))
        ItsmTicketManager(ticket.ticket_type).handle_call_back(ticket, request.data)
        return Response()
