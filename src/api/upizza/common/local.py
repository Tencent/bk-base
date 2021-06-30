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
from threading import local

_local = local()


class LocalVariable:
    APPENV = "appenv"


def activate_request(request, request_id=None):
    """
    将 Request 相关信息记录至线程变量，检查必要参数
    """
    if not request_id:
        from common.base_utils import generate_request_id

        request_id = generate_request_id()
    request.request_id = request_id

    _local.request = request

    return request


def get_request():
    """
    获取线程请求request
    """
    return _local.request


def get_request_id():
    """
    获取request_id
    """
    try:
        return get_request().request_id
    except AttributeError:
        return ""


def get_request_username():
    """
    获取线程请求中的 USERNAME，非线程请求返回空字符串
    """
    try:
        return _local.bk_username
    except AttributeError:
        return ""


def get_request_app_code():
    """
    获取线程请求中的 APP_CODE，非线程请求返回空字符串
    """
    try:
        return _local.bk_app_code
    except AttributeError:
        return ""


def set_local_param(key, value):
    """
    设置自定义线程变量
    """
    setattr(_local, key, value)


def del_local_param(key):
    """
    删除自定义线程变量
    """
    if hasattr(_local, key):
        delattr(_local, key)


def get_local_param(key, default=None):
    """
    获取线程变量
    """
    return getattr(_local, key, default)
