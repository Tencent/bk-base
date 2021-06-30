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
from abc import abstractmethod

from auth.core.ticket_notification import MailNotice
from auth.itsm.backend.backend import CallBackend
from auth.itsm.config.contants import TICKET_TYPE


class BaseTicket:
    @abstractmethod
    def create_ticket(self, ticket):
        """
        添加权限的函数，继承的子类必须要实现
        @param ticket:
        @return:
        """
        pass

    @abstractmethod
    def add_permission(self, ticket):
        """
        添加权限的函数，继承的子类必须要实现
        @param ticket:
        @return:
        """
        pass


class ModuleTicketObj(BaseTicket):
    """
    第三方模块
    """

    model = None
    ticket_type = TICKET_TYPE.MODULE_APPLY

    def create_ticket(self, ticket):
        """第三方模块不需要创建子单据"""
        pass

    def add_permission(self, ticket):
        # 回调机制应该增加重试
        if ticket.callback_url:
            # 默认回调三次，发现成功退出循环
            for call_back_num in range(0, 3):
                if CallBackend.callback(ticket):
                    return
            # 执行微信通知
            content = f"数据平台回调失败，请您处理，单据id:{ticket.id}"
            MailNotice().send_msg(content, [ticket.created_by], "数据平台回调失败通知")
