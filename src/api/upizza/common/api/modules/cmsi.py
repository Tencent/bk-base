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

from common.api.base import DataAPI, ProxyBaseApi, ProxyDataAPI, DataAPISet


def before_send_cmsi_api(params):
    if "bk_username" not in params:
        params["bk_username"] = "admin"
        params["bk_ticket"] = "XHPDzEhNhIKY2GroPEM0Nb8H6XsIJtRD-7WHweo2adA"

    receivers = params.pop("receivers", [])
    if "receiver__username" not in params:
        if isinstance(receivers, list):
            params["receiver__username"] = ",".join(receivers)
        elif isinstance(receivers, str):
            params["receiver__username"] = receivers
    return params


def before_send_cmsi_sms(params):
    params = before_send_cmsi_api(params)
    if "message" in params and "content" not in params:
        params["content"] = params["message"]
    return params


def before_send_cmsi_wechat(params):
    params = before_send_cmsi_api(params)
    if "message" in params:
        params["data"] = {
            "message": params.pop("message", ""),
            "heading": params.pop("title", "") or params.pop("heading", ""),
        }
    return params


def before_send_cmsi_voice_msg(params):
    params = before_send_cmsi_api(params)
    if "message" in params:
        params["auto_read_message"] = params["message"]
    return params


def before_send_cmsi_eewechat(params):
    params = before_send_cmsi_api(params)
    if "message" in params and "content" not in params:
        params["content"] = params["message"]
    if "title" not in params:
        params["title"] = "消息通知"
    return params


class _CmsiApi(DataAPISet):
    def __init__(self):
        pass


class _CmsiApiProxy(ProxyBaseApi):

    MODULE = "CMSI"

    def __init__(self, default, proxy):
        super().__init__(default, proxy)

        cmsi_api_url = settings.CMSI_API_URL

        self.send_mail = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_mail/",
            module="CMSI",
            description="发送邮件",
            before_request=before_send_cmsi_api,
        )
        self.send_sms = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_sms/",
            module="CMSI",
            description="发送短信通知",
            before_request=before_send_cmsi_sms,
        )
        self.send_voice_msg = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_voice_msg/",
            module="CMSI",
            description="发送语音通知",
            before_request=before_send_cmsi_voice_msg,
        )
        self.send_wechat = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_weixin/",
            module="CMSI",
            description="发送微信通知",
            before_request=before_send_cmsi_wechat,
        )
        self.send_eewechat = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_rtx/",
            module="CMSI",
            before_request=before_send_cmsi_eewechat,
            description="发送企业微信",
        )
        self.get_msg_type = DataAPI(
            method="GET",
            url=cmsi_api_url + "get_msg_type/",
            module="CMSI",
            description="获取蓝鲸支持的消息通知方式",
        )
        self.send_msg = DataAPI(
            method="POST",
            url=cmsi_api_url + "send_msg/",
            module="CMSI",
            before_request=before_send_cmsi_api,
            description="通用消息发送接口",
        )
        self.wechat_approve_api = ProxyDataAPI(
            description="微信通知审批接口",
            params=["app_name", "app_code", "app_secret", "verifier", "message", "task_id", "url"],
        )


CmsiApi = _CmsiApiProxy(default=_CmsiApi(), proxy={"tencent": "extend.tencent.common.api.modules.cmsi._CmsiApi"})
