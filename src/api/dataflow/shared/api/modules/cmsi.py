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

from common.api.base import DataAPI
from common.local import get_request_username
from conf.dataapi_settings import APP_ID, APP_TOKEN, RUN_VERSION

from dataflow.pizza_settings import BASE_CMSI_URL, SMCS_API_URL, TOF_API_URL

from .test.test_call_cmsi import TestCmsi


def add_app_info_before_request(params):
    """
    统一请求模块必须带上的参数
    """
    if "bk_app_code" not in params:
        params["bk_app_code"] = APP_ID
    if "bk_app_secret" not in params:
        params["bk_app_secret"] = APP_TOKEN
    if "bk_username" not in params:
        params["bk_username"] = get_request_username()

    # 消息通知模块所需参数
    params["app_code"] = APP_ID
    params["app_secret"] = APP_TOKEN
    if "operator" not in params:
        params["operator"] = params["bk_username"]
    return params


# 根据部署环境选择接口地址
if RUN_VERSION in ["ieod"]:
    # 若为内部版
    CMSI_RTX_URL = TOF_API_URL + "send_rtx/"
    # 目前内部版不会进行普通微信的告警，这里用到的是消息通知审批服务接口(SMCS)
    CMSI_WEIXIN_URL = SMCS_API_URL + "send_weixin/"
    CMSI_EMAIL_URL = TOF_API_URL + "send_mail/"
else:
    # 若为其它(企业版)
    CMSI_RTX_URL = BASE_CMSI_URL + "send_weixin/"
    CMSI_WEIXIN_URL = BASE_CMSI_URL + "send_weixin/"
    CMSI_EMAIL_URL = BASE_CMSI_URL + "send_mail/"


class _CmsiApi(object):

    test_cmsi = TestCmsi()

    def __init__(self):

        self.send_rtx = DataAPI(
            method="POST",
            url=CMSI_RTX_URL,
            module="cmsi",
            description="发送企业微信",
            default_return_value=self.test_cmsi.set_return_value("send_rtx"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.send_weixin = DataAPI(
            method="POST",
            url=CMSI_WEIXIN_URL,
            module="cmsi",
            description="发送微信",
            default_return_value=self.test_cmsi.set_return_value("send_weixin"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.send_mail = DataAPI(
            method="POST",
            url=CMSI_EMAIL_URL,
            module="cmsi",
            description="发送邮件",
            default_return_value=self.test_cmsi.set_return_value("send_mail"),
            before_request=add_app_info_before_request,
            after_request=None,
        )


Cmsi = _CmsiApi()
