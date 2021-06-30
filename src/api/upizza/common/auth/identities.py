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

from django.conf import settings
from django.utils.translation import ugettext as _

from common.api import AuthApi

from .exceptions import (
    InvalidSecretError,
    InvalidTokenError,
    JWTVerifyError,
    ParamMissError,
)
from .jwt import JWTClient, is_nocheck_gateway


def extract_parmas(request):
    """
    根据请求类型，提取请求参数
    @return {Dict} 字典
    """
    if request.method == "GET":
        return request.GET

    # 请求方法 POST，需要兼容各种旧版请求方式
    elif request.method == "POST":
        # 优先使用 application/json 解析
        try:
            return json.loads(request.body)
        except ValueError:
            pass

        # 其次使用 application/form-data 解析
        try:
            return request.POST.dict()
        except Exception:
            pass

    # 请求方法 PUT、PATCH、DELETE 从 body 中获取参数内容，并且仅支持 json 格式
    else:
        try:
            return json.loads(request.body)
        except ValueError:
            pass

    # 避免data为None的情况导致返回的数据结构不一致
    return {}


def extract_header_authorization(request):
    """
    从 header 提取授权信息
    """
    BK_BKAPI_AUTHORIZATION = "HTTP_X_BKDATA_AUTHORIZATION"

    auth_info = request.META.get(BK_BKAPI_AUTHORIZATION, "")

    if auth_info:
        return json.loads(auth_info)

    return {}


def extract_value(request, key):
    params = extract_parmas(request)
    header_authorization_params = extract_header_authorization(request)

    # 优先从 header 中获取目标认证数值

    if key in header_authorization_params:
        return header_authorization_params[key]

    if key in params:
        return params[key]

    return ""


class Identity:
    NAME = ""

    def __init__(self, bk_app_code, bk_username):
        self.bk_app_code = bk_app_code
        self.bk_username = bk_username

    @classmethod
    def authenticate(cls, request):
        """
        校验来自外部的接口是否合法，是否符合认证要求
        """
        raise NotImplementedError

    @classmethod
    def verify_bk_jwt_request(cls, request, verify_app=True, verify_user=False):
        """
        验证 BK_JWT 请求，如果解析有问题，直接抛出 JWTVerifyError 异常
        @param {string} x_bkapi_jwt JWT请求头
        @return {dict}
            {
                'bk_username': '调用方用户',
                'bk_app_code': '调用方app'
            }
        """
        # 暂时不处理的网关
        if is_nocheck_gateway(request):
            return {
                "bk_app_code": extract_value(request, "bk_app_code"),
                "bk_username": extract_value(request, "bk_username"),
            }

        jwt = JWTClient(request)
        if not jwt.is_valid:
            raise JWTVerifyError("Invalid JWT: {}".format(jwt.error_message))

        # verify: user && app
        app = jwt.get_app_model()
        bk_app_code = app.get("app_code", "")
        if verify_app:
            if not app["verified"] or not bk_app_code:
                raise JWTVerifyError("No verified app in JWT: {}".format(app.get("valid_error_message")))

        user = jwt.get_user_model()
        bk_username = user.get("username", "")
        if verify_user:
            if settings.FORCE_USER_VERIFY and not user["verified"]:
                raise JWTVerifyError("No verified user in JWT: {}".format(user.get("valid_error_message")))

            # 目前认定从 JWT 提取出来的 username 都是经过验证的
            if not bk_username:
                raise JWTVerifyError("No verified username in JWT")

        return {"bk_app_code": bk_app_code, "bk_username": bk_username}

    def __str__(self):
        return "[Identity] bk_app_code:{} && bk_username:{}".format(self.bk_app_code, self.bk_username)


class InnerIdentity(Identity):
    NAME = "inner"

    @classmethod
    def authenticate(cls, request):
        ret, bk_app_code = cls.verify_inner(request)
        if not ret:
            raise InvalidSecretError()

        bk_username = extract_value(request, "bk_username")
        return cls(bk_app_code, bk_username)

    @classmethod
    def verify_inner(cls, request):
        """
        检查是否为内部调用

        :param request:
        :return:
        """
        bk_app_code = extract_value(request, "bk_app_code")
        bk_app_secret = extract_value(request, "bk_app_secret")

        inner_app_id = settings.APP_ID
        inner_app_token = settings.APP_TOKEN

        # 当传入的 APP_CODE 为内部统一的 APP_CODE，则认定为内部调用
        if bk_app_code != inner_app_id or bk_app_secret != inner_app_token:
            return False, None

        return True, bk_app_code


class InnerUserIdentity(InnerIdentity):
    NAME = "inner-user"

    @classmethod
    def authenticate(cls, request):
        ret, bk_app_code = cls.verify_inner(request)
        if not ret:
            raise InvalidSecretError()

        bk_username = extract_value(request, "bk_username")

        # 内部调用需要支持使用用户进行校验，期望不自动通过
        if not bk_username:
            raise ParamMissError(_("参数 bk_username 不可为空"))

        return UserIdentity(bk_app_code, bk_username)


class TokenIdentity(Identity):

    NAME = "token"

    def __init__(self, bk_app_code, bk_username, data_token):
        super(TokenIdentity, self).__init__(bk_app_code, bk_username)

        self.data_token = data_token

    @classmethod
    def authenticate(cls, request):
        data = cls.verify_bk_jwt_request(request, verify_app=True, verify_user=False)

        bk_app_code = data["bk_app_code"]
        bk_username = data["bk_username"]

        data_token = extract_value(request, "bkdata_data_token")

        # 当传入有效的 data_token，则强执行 data_token 校验
        token_info = cls.get_token_info(data_token)

        if token_info is None:
            raise InvalidTokenError()

        if not bk_username:
            bk_username = token_info["bk_username"]

        return cls(bk_app_code, bk_username, data_token)

    @staticmethod
    def get_token_info(data_token):

        response = AuthApi.get_token_info({"search_data_token": data_token})
        if not response.is_success():
            return None

        return {"bk_username": response.data["created_by"]}


class UserIdentity(Identity):

    NAME = "user"

    @classmethod
    def authenticate(cls, request):
        data = cls.verify_bk_jwt_request(request, verify_app=True, verify_user=True)

        bk_username = data["bk_username"]
        bk_app_code = data["bk_app_code"]

        if not bk_username:
            raise ParamMissError(_("参数 bk_username 不可为空"))

        return cls(bk_app_code, bk_username)


def exchange_default_token_identity(request):
    """
    检查 request 中的 bk_app_code，置换出默认 token，仅适用于兼容老的 API
    """
    data = Identity.verify_bk_jwt_request(request, verify_app=True, verify_user=False)
    bk_app_code = data["bk_app_code"]

    token_info = AuthApi.exchange_default_data_token({"data_token_bk_app_code": bk_app_code}, raise_exception=True).data

    if token_info is None:
        return None

    bk_username = token_info["created_by"]
    data_token = token_info["data_token"]

    return TokenIdentity(bk_app_code, bk_username, data_token)
