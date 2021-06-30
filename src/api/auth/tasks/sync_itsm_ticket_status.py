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
from auth.itsm.config.contants import TICKET_STATUS
from auth.itsm.ticket import ItsmTicketManager
from auth.models.ticketV2_models import AuthCommonTicket
from celery.schedules import crontab
from celery.task import periodic_task


# 每600秒执行一次
@periodic_task(run_every=crontab(minute="*/10"), queue="auth")
def sync_itsm_ticket_status():
    """
    更新单据状态
    """

    tickets = AuthCommonTicket.objects.filter(itsm_status__in=[TICKET_STATUS.RUNNING, TICKET_STATUS.SUSPENDED])
    for ticket in tickets:
        itsm_ticket_manager = ItsmTicketManager(ticket_type=ticket.ticket_type)
        itsm_ticket_manager.update_ticket_status(ticket)

        if ticket.itsm_status == TICKET_STATUS.FINISHED:
            itsm_ticket_manager.add_permission(ticket)
