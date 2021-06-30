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
from django.conf import settings
from django.utils.translation import ugettext_lazy as _


class TICKET_TYPE:
    MODULE_APPLY = "module_apply"

    CHOICES = [
        (MODULE_APPLY, "第三方模块申请"),
    ]


class TICKET_STATUS:
    # itsm 单据状态
    RUNNING = "RUNNING"  # 处理中
    FINISHED = "FINISHED"  # 已结束
    TERMINATED = "TERMINATED"  # 被终止
    SUSPENDED = "SUSPENDED"  # 被挂起
    REVOKED = "REVOKED"  # 被撤销

    CHOICES = (
        (RUNNING, _("处理中")),
        (FINISHED, _("已结束")),
        (TERMINATED, _("被终止")),
        (SUSPENDED, _("被挂起")),
        (REVOKED, _("被撤销")),
    )


class TICKET_CALLBACK_STATUS:
    SUCCESS = "SUCCESS"  # 回调成功
    FAIL = "FAIL"  # 回调失败
    WAITING = "WAITING"  # 等待回调

    CHOICES = (
        (SUCCESS, _("回调成功")),
        (FAIL, _("回调错误")),
        (WAITING, _("等待回调")),
    )


ITSM_CATALOG_NAME = "蓝鲸计算平台"

ITSM_SERVICE_NAME_MAP = {}

CALL_BACK_URL = settings.AUTH_API_URL + "tickets_v2/call_back/"
