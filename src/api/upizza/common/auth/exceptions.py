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

from django.utils.translation import ugettext as _


class BaseAuthError(Exception):
    CODE = "1511000"
    MESSAGE = "权限基类"

    def __init__(self, message=None):
        self.message = message if message else self.MESSAGE
        self.code = self.CODE

    def __str__(self):
        return "[{}] {}".format(self.code, self.message)


class PermissionDeniedError(BaseAuthError):
    CODE = "1511001"
    MESSAGE = _("资源访问权限不足")


class ParamMissError(BaseAuthError):
    CODE = "1511002"
    MESSAGE = _("认证参数缺失")


class AuthenticateError(BaseAuthError):
    CODE = "1511003"
    MESSAGE = _("认证不通过，请提供合法的 BKData 认证信息")


class InvalidSecretError(BaseAuthError):
    CODE = "1511004"
    MESSAGE = _("内部模块调用请传递准确的 bk_app_code 和 bk_app_secret")


class InvalidTokenError(BaseAuthError):
    CODE = "1511005"
    MESSAGE = _("授权码不正确")


class NotWhiteAppCallError(BaseAuthError):
    CODE = "1511006"
    MESSAGE = _("非白名单 APP 不可直接访问接口")


class NoIdentityError(BaseAuthError):
    CODE = "1511007"
    MESSAGE = _("未检测到有效的认证信息")


class WrongIdentityError(BaseAuthError):
    CODE = "1511008"
    MESSAGE = _("错误的认证方式")


class JWTVerifyError(BaseAuthError):
    CODE = "1511009"
    MESSAGE = _("ESB 传递的 JWT 字符串解析失败")
