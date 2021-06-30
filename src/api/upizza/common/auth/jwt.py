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
import re

# 非强制安装PyJWT
try:
    import jwt
    from jwt import exceptions as jwt_exceptions

    has_jwt = True
except ImportError:
    has_jwt = False

try:
    import cryptography  # noqa

    has_crypto = True
except ImportError:
    has_crypto = False

from common.api import AuthApi
from common.log import logger

UIN_PATTERN = re.compile(r"^o(?P<uin>\d{3,32})$")


def transform_uin(uin):
    """
    将腾讯云的uin转换为字符型的qq号
    就是去掉第一个字符然后转为整形
    o0836324475 -> 836324475
    o2459422247 -> 2459422247
    """
    match = UIN_PATTERN.match(uin)
    if match:
        uin = str(int(match.groupdict()["uin"]))
    return uin


class FancyDict(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError as k:
            raise AttributeError(k)


def get_client_ip(request):
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if x_forwarded_for:
        ip = x_forwarded_for
    else:
        ip = request.META["REMOTE_ADDR"]
    return ip


GLOBAL_PUBLIC_KEYS = None


class JWTClient(object):
    JWT_KEY_NAME = "HTTP_X_BKAPI_JWT"
    JWT_PUBLIC_KEY_HEADER_NAME = "HTTP_X_BKAPI_PUBLIC_KEY"
    GATEWAY_INDEX_HEADER_NAME = "HTTP_GATEWAY_NAME"

    def __init__(self, request):
        self.request = request
        self.raw_content = request.META.get(self.JWT_KEY_NAME, "")
        self.error_message = ""
        self.is_valid = False

        self.payload = {}
        self.headers = {}
        self.get_jwt_info()

        self.app = self.get_app_model()
        self.user = self.get_user_model()

    def get_app_model(self):
        return FancyDict(self.payload.get("app", {}))

    def get_user_model(self):
        return FancyDict(self.payload.get("user", {}))

    def get_jwt_info(self):
        if has_jwt is False:
            self.error_message = "[PyJWT] SDK not installed, please add PyJWT to requirements.txt"
            return False
        if has_crypto is False:
            self.error_message = "[cryptography] SDK not installed，please add cryptography to requirements.txt"
            return False

        if not self.raw_content:
            self.error_message = "[X_BKAPI_JWT] Not in header or empty, please confirm request is from APIGateway/ESB"
            return False
        try:
            self.headers = jwt.get_unverified_header(self.raw_content)

            public_key = self._get_jwt_public_key()
            if public_key == "":
                gateway_name = self._get_gateway_name()
                self.error_message = f"[X_BKAPI_JWT] No public keys for {gateway_name} in AuthESB controller."
                return

            self.payload = jwt.decode(self.raw_content, public_key, issuer="APIGW")

            self.is_valid = True
        except jwt_exceptions.InvalidKeyError:
            self.error_message = "[X_BKAPI_JWT] Wrong PublicKey, please fetch PublicKey from AUTH module."
        except jwt_exceptions.DecodeError:
            self.error_message = "[X_BKAPI_JWT] Invalid JWT, please confirm format and PrivateKey signature"
        except jwt_exceptions.ExpiredSignatureError:
            self.error_message = "[X_BKAPI_JWT] Invalid JWT, its expired"
        except jwt_exceptions.InvalidIssuerError:
            self.error_message = "[X_BKAPI_JWT] Invalid JWT, issuer isnot APIGW/ESB"
        except Exception as error:
            logger.exception("Fail to decode jwt...")
            self.error_message = error.message

    def _get_gateway_name(self):
        """
        获取来源网关
        """
        if type(self.headers) is dict and "kid" in self.headers:
            return self.headers["kid"]

        return self.request.META.get(self.GATEWAY_INDEX_HEADER_NAME, "default")

    def _get_jwt_public_key(self):
        """
        默认从缓存中获取，基本配置保存在权限模块中
        """
        global GLOBAL_PUBLIC_KEYS

        if GLOBAL_PUBLIC_KEYS is None:
            public_keys = AuthApi.list_public_keys(raise_exception=True).data
            GLOBAL_PUBLIC_KEYS = public_keys
        else:
            public_keys = GLOBAL_PUBLIC_KEYS

        gateway_name = self._get_gateway_name()

        if gateway_name not in public_keys.keys():
            return ""

        return public_keys[gateway_name]

    def __unicode__(self):
        return "<{}, {}>".format(self.headers, self.payload)


def is_nocheck_gateway(request):
    """
    无需检查的网关，后续需要移除，兼容逻辑
    """
    gateway_name = request.META.get(JWTClient.GATEWAY_INDEX_HEADER_NAME, "default")
    return gateway_name in ["bkdata", "bkdata-tencent"]
