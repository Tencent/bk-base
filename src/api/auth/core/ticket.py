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

from auth.config.ticket import TICKET_FLOW_CONFIG, TICKET_TYPE_CHOICES
from auth.constants import DISPLAY_STATUS, FAILED, STOPPED, SUCCEEDED
from auth.core.ticket_objects import (
    ApprovalController,
    BatchReCalcObj,
    CommonTicketObj,
    DataTokenTicketObj,
    ProjectDataTicketObj,
    ResourceGroupTicketObj,
    RoleTicketObj,
)
from auth.core.ticket_serializer import TicketStateSerializer
from auth.exceptions import UnexpectedTicketTypeErr
from common.business import Business
from django.db import transaction


class TicketFactory:
    TICKET_OBJECTS = [
        ProjectDataTicketObj,
        ResourceGroupTicketObj,
        RoleTicketObj,
        DataTokenTicketObj,
        BatchReCalcObj,
        CommonTicketObj,
    ]
    TICKET_OBJECT_CONFIG = {cls.__name__: cls for cls in TICKET_OBJECTS}

    def __init__(self, ticket_type):
        self.ticket_type = ticket_type

        if ticket_type not in TICKET_FLOW_CONFIG:
            raise UnexpectedTicketTypeErr()

        _ticket_flow = TICKET_FLOW_CONFIG[ticket_type]
        self.ticket_obj = self.TICKET_OBJECT_CONFIG[_ticket_flow.ticket_object](ticket_type)

    def generate_ticket_data(self, data):
        """
        生成ticket数据
        @param data:
        @return:
        """
        return self.ticket_obj.generate_ticket_data(data)

    def add_permission(self, ticket):
        """
        添加权限
        @param ticket:
        @return:
        """
        self.ticket_obj.add_permission(ticket)

    def after_terminate(self, ticket, status):
        """
        单据终止后的回调
        @param ticket:
        @param status:
        @return:
        """
        if hasattr(self.ticket_obj, "after_terminate"):
            self.ticket_obj.after_terminate(ticket, status)

    @classmethod
    def list_ticket_types(cls):
        return [{"id": item[0], "name": item[1]} for item in TICKET_TYPE_CHOICES]

    @classmethod
    def list_ticket_status(cls):
        return [{"id": item[0], "name": item[1]} for item in DISPLAY_STATUS]

    @classmethod
    def serialize_ticket_queryset(cls, data, many=False, show_display=False):
        """
        序列化单据
        @param data:
        @param many:
        @param show_display:
        @return:
        """
        if many:
            # 查询列表时暂时不需要列出所有显示名
            results = []
            for item in data:
                results.append(cls(item.ticket_type).ticket_obj.SERIALIZER(item).data)
            return results
        else:
            # 查询详情时把对象的显示名补充
            result = cls(data.ticket_type).ticket_obj.SERIALIZER(data).data
            if show_display:
                result = cls(data.ticket_type).ticket_obj.SERIALIZER(data).wrap_permissions_display(result)
            return result

    @classmethod
    def serialize_state_queryset(cls, data, ticket_type=None, many=True, show_display=False):
        """
        序列化单据状态节点
        @param data:
        @param ticket_type: 单据类型
        @param many:
        @param show_display:
        @return:
        """
        if many:
            result = []

            # 指定单据类型时，同一state下的单据类型一定是一致的，不指定单据类型时则从各自state下取值
            ticket_types = list({state.ticket.ticket_type for state in data})
            if ticket_type:
                if len(ticket_types) != 1:
                    raise UnexpectedTicketTypeErr()
                else:
                    if ticket_type != ticket_types[0]:
                        raise UnexpectedTicketTypeErr()
            else:
                if len(ticket_types) == 1:
                    ticket_type = ticket_types[0]

            for state in data:
                item = TicketStateSerializer(state).data
                item["ticket"] = cls(ticket_type or state.ticket.ticket_type).ticket_obj.SERIALIZER(state.ticket).data
                result.append(item)

            # 暂不支持不统一单据类型的权限显示名展示（效率较慢且暂无需求
            if ticket_type and show_display:
                result = cls(ticket_type).ticket_obj.SERIALIZER.wrap_permissions_display(result)

        else:
            result = TicketStateSerializer(data).data
            result["ticket"] = cls(ticket_type).ticket_obj.SERIALIZER(data.ticket).data

            if show_display:
                result["ticket"]["permissions"] = (
                    cls(ticket_type).ticket_obj.SERIALIZER(data.ticket).wrap_permissions_display(result)
                )

        return result

    @classmethod
    def approve(cls, state, status, process_message, processed_by, add_biz_list=False):
        """
        审批
        @param state:
        @param status:
        @param process_message:
        @param processed_by:
        @param add_biz_list:
        @return:
        """
        context = None
        if add_biz_list:
            context = {"business": Business.get_name_dict()}
        ApprovalController.approve(state, status, process_message, processed_by)
        state_data = TicketStateSerializer(state).data
        state_data["ticket"] = cls(state.ticket.ticket_type).ticket_obj.SERIALIZER(state.ticket, context=context).data
        return state_data

    @classmethod
    def withdraw(cls, ticket, process_message, processed_by):
        ticket.has_owner_permission(processed_by, raise_exception=True)
        with transaction.atomic(using="basic"):
            if not ticket.is_process_finish():
                ticket.withdraw(process_message, processed_by)

        return cls.serialize_ticket_queryset(ticket)

    def get_content_for_notice(self, ticket):
        """
        获取通知消息内容
        @param [Ticket] ticket:
        @return:
        """
        return self.ticket_obj.get_content_for_notice(ticket)


def approve_finished(ticket, status, **kwargs):
    if status == SUCCEEDED:
        TicketFactory(ticket.ticket_type).add_permission(ticket)

    if status in [FAILED, STOPPED]:
        TicketFactory(ticket.ticket_type).after_terminate(ticket, status)
