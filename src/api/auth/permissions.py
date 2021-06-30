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
from auth.core.permission import UserPermission
from auth.exceptions import NoPermissionError
from auth.models import AuthDataToken, Ticket, TicketState
from django.http import Http404
from django.utils.translation import ugettext_lazy as _
from rest_framework.permissions import BasePermission

from common import local


class TicketPermission(BasePermission):
    """
    审批页面，操作单据权限使用的是业务权限
    """

    def has_permission(self, request, view):
        """
        判断是否有权限
        @param request:
        @param view:
        @return:
        """
        username = local.get_request_username()

        ticket_id = view.kwargs.get("pk", None)
        if ticket_id is None:
            return True

        if not Ticket.objects.filter(pk=ticket_id).count():
            raise Http404(_("单据不存在"))

        return Ticket.has_permission(username, ticket_id)


class TicketStatePermission(BasePermission):
    """
    审批状态权限
    """

    def has_permission(self, request, view):
        username = local.get_request_username()

        ticket_state_id = view.kwargs.get("pk", None)
        if ticket_state_id is None:
            return True

        if not TicketState.objects.filter(pk=ticket_state_id).count():
            raise Http404(_("单据状态不存在"))

        result = TicketState.has_permission(username, ticket_state_id)
        if not result:
            raise NoPermissionError()
        return result


class RolePermission(BasePermission):
    """
    添加角色操作权限校验
    """

    def has_permission(self, request, view):
        return True


class TokenPermission(BasePermission):
    """
    添加角色操作权限校验
    """

    def has_permission(self, request, view):
        username = local.get_request_username()

        token_id = view.kwargs.get("pk", None)
        if token_id is None:
            return True

        try:
            token = AuthDataToken.objects.get(id=token_id)
        except AuthDataToken.DoesNotExist:
            raise Http404(_("授权码不存在"))

        if view.action in ("update", "retrieve"):
            return True

        ret = UserPermission(username).check("data_token.manage", token.id, raise_exception=False)

        if not ret:
            raise NoPermissionError()

        return True
