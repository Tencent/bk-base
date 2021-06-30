# -*- coding=utf-8 -*-
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
import json
import sys
from typing import Dict

from django.conf import settings
from django.utils import translation

from common.local import get_request_username


def is_celery():
    """
    判断当前环境是否celery运行
    @return:
    """
    for _argv in sys.argv:
        if "celery" in _argv:
            return True
    return False


def is_shell():
    """
    判断当前环境是否shell运行
    @return:
    """
    for _argv in sys.argv:
        if "shell" in _argv:
            return True
    return False


def add_app_info_before_request(params: Dict):
    """
    请求前默认添加应用信息

    :param params: 原始的请求参数
    :return: 已封装的请求参数
    """
    # 必要的认证参数补充默认值
    if "bk_app_code" not in params:
        params["bk_app_code"] = settings.APP_ID
        params["bk_app_secret"] = settings.APP_TOKEN

    # 使用内部默认 APP 传递，认定调用方式为内部调用
    if "bkdata_authentication_method" not in params and params["bk_app_code"] == settings.APP_ID:
        params["bkdata_authentication_method"] = "inner"

    # 后台任务 & 测试任务调用接口默认 ADMIN 用户
    if "bk_username" not in params:
        if is_celery() or is_shell():
            bk_username = params.get("bk_username") or "admin"
        else:
            bk_username = get_request_username()
        params["bk_username"] = bk_username
    return params


def add_header_before_request():
    """
    请求前默认添加 Header
    """
    return {
        "blueking-language": translation.get_language(),
        "x-bkdata-module": settings.APP_NAME,
        "x-bkdata-authorization": json.dumps({"bk_app_code": settings.APP_ID, "bk_app_secret": settings.APP_TOKEN}),
    }
