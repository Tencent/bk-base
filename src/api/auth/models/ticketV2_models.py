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
from auth.itsm.config.contants import TICKET_CALLBACK_STATUS, TICKET_STATUS, TICKET_TYPE
from auth.models.fields import JsonField
from django.db import models
from django.utils.translation import ugettext_lazy as _


class AuthCommonTicket(models.Model):
    """
    Itsm单据信息
    """

    ticket_type = models.CharField(_("单据类型"), max_length=255, choices=TICKET_TYPE.CHOICES)
    itsm_service_id = models.IntegerField(_("流程id"), null=False)
    itsm_sn = models.CharField(_("单号"), max_length=255, null=False)
    callback_url = models.CharField(_("回调地址"), max_length=255, null=True)
    itsm_status = models.CharField(
        _("当前申请状态"), max_length=255, choices=TICKET_STATUS.CHOICES, default=TICKET_STATUS.RUNNING
    )
    fields = JsonField(_("请求参数"), max_length=512)
    approve_result = models.NullBooleanField(_("审批类型"), null=True, default=None)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    end_time = models.DateTimeField(_("单据结束时间"), null=True)
    callback_status = models.CharField(
        _("回调状态"), max_length=32, choices=TICKET_CALLBACK_STATUS.CHOICES, default=TICKET_CALLBACK_STATUS.WAITING
    )

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_common_ticket"
        ordering = ["-created_at"]
