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
import json

from django.http import JsonResponse
from django.middleware.common import MiddlewareMixin

from common.local import get_local_param, set_local_param

from .exceptions import BaseAuthError
from .identities import (
    InnerIdentity,
    InnerUserIdentity,
    TokenIdentity,
    UserIdentity,
    extract_value,
)


def _convert_exception_to_response(exc):
    """
    将权限模块的异常输出转换为标准响应

    @param {BaseAuthError} exc
    """
    return JsonResponse({"result": False, "message": exc.message, "data": None, "code": exc.code, "errors": None})


class AuthenticationMiddleware(MiddlewareMixin):

    # 身份认证器，次序决定认证次序
    IdentityBackends = [InnerIdentity, InnerUserIdentity, TokenIdentity, UserIdentity]

    @classmethod
    def match_backend(cls, request, bkdata_authentication_method):
        # 兼容逻辑，如果是内部调用，并且期望使用用户认证，则自动切换为 inner-user 认证器
        inner_ret, _ = InnerIdentity.verify_inner(request)
        if inner_ret and bkdata_authentication_method == UserIdentity.NAME:
            return InnerUserIdentity

        for backend in cls.IdentityBackends:
            if backend.NAME == bkdata_authentication_method:
                return backend

        return None

    def process_request(self, request):
        bkdata_force_authentication = extract_value(request, "bkdata_force_authentication")
        bkdata_authentication_method = extract_value(request, "bkdata_authentication_method")
        identity = None
        try:
            # 主动传入 bkdata_authentication_method 指定身份认证器
            backend = self.match_backend(request, bkdata_authentication_method)
            if backend:
                identity = backend.authenticate(request)

            # 带有 bkdata_data_token 参数以授权码进行认证
            if identity is None:
                data_token = extract_value(request, "bkdata_data_token")
                if data_token:
                    identity = TokenIdentity.authenticate(request)

            if identity is None:
                identity = UserIdentity.authenticate(request)

        except BaseAuthError as e:
            if bkdata_force_authentication:
                return _convert_exception_to_response(e)

        set_local_param("identity", identity)

        if identity is not None:
            bk_username = identity.bk_username
            bk_app_code = identity.bk_app_code
        else:
            # 保留老逻辑获取用户名和 APP_CODE 的方式，避免业务代码逻辑获取不到用户名和 APP_CODE
            bk_username = extract_value(request, "bk_username")
            bk_app_code = extract_value(request, "bk_app_code")

        # 为了兼容框架中从 local 文件中获取当前访问的 APP_CODE 和 USERNAME
        set_local_param("bk_username", bk_username)
        set_local_param("bk_app_code", bk_app_code)

        # 认证信息，用于直接透传给esb
        try:
            auth_info = json.loads(extract_value(request, "auth_info"))
        except (ValueError, TypeError):
            auth_info = {}

        set_local_param("auth_info", auth_info)

        return None


def get_identity():
    """
    获取当前访问的身份
    """
    return get_local_param("identity")


def get_request_username():
    """
    获取当前访问身份的用户名
    """
    return get_identity().bk_username


def get_request_app_code():
    """
    获取当前访问身份的APP
    """
    return get_identity().bk_app_code
