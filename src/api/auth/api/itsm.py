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

from common.api.base import DataAPI, ProxyBaseApi


class _ITSMApi(ProxyBaseApi):

    MODULE = "ITSM"

    def __init__(self):
        itsm_api_url = settings.ISTM_API_URL
        self.create_ticket = DataAPI(
            method="POST",
            url=itsm_api_url + "create_ticket/",
            module=self.MODULE,
            description="创建单据",
        )
        self.callback_failed_ticket = DataAPI(
            method="GET",
            url=itsm_api_url + "callback_failed_ticket/",
            module=self.MODULE,
            description="查询回调失败的单据",
        )
        self.get_ticket_status = DataAPI(
            method="GET",
            url=itsm_api_url + "get_ticket_status/",
            module=self.MODULE,
            description="查询单据状态",
        )
        self.operate_ticket = DataAPI(
            method="POST",
            url=itsm_api_url + "operate_ticket/",
            module=self.MODULE,
            description="操作单据",
        )

        self.ticket_approval_result = DataAPI(
            method="POST",
            url=itsm_api_url + "ticket_approval_result/",
            module=self.MODULE,
            description="查询单据审批状态",
        )
        self.token_verify = DataAPI(
            method="POST",
            url=itsm_api_url + "token/verify/",
            module=self.MODULE,
            description="token校验",
        )
        self.get_service_catalogs = DataAPI(
            method="POST",
            url=itsm_api_url + "get_service_catalogs",
            module=self.MODULE,
            description="获取服务目录",
        )
        self.get_services = DataAPI(
            method="POST",
            url=itsm_api_url + "get_services",
            module=self.MODULE,
            description="获取服务列表",
        )


ITSMApi = _ITSMApi()
