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
from typing import Optional

from bkbase_crypt.crypt import BaseCrypt as BKBaseCrypt
from django.conf import settings


class BaseCrypt:
    def __init__(
        self, instance_key: Optional[str] = None, root_iv: Optional[str] = None, root_key: Optional[str] = None
    ):
        """
        初始化加解密模块，内部封装 bkbase_crypt.crypt.BaseCrypt 类实例
        """
        instance_key = instance_key if instance_key else settings.CRYPT_INSTANCE_KEY
        root_iv = root_iv if root_iv else settings.CRYPT_ROOT_IV
        root_key = root_key if root_key else settings.CRYPT_ROOT_KEY

        if not (instance_key and root_iv and root_key):
            raise Exception("instance_key、root_iv、root_key 均不可为空")

        self._crypt = BKBaseCrypt(instance_key=instance_key, root_iv=root_iv, root_key=root_key)

    @staticmethod
    def bk_crypt() -> "BaseCrypt":
        """
        通过静态方法快速获取默认配置下的加解密工具类，为了兼容旧版的使用方式
        """
        return BaseCrypt()

    def encrypt(self, plaintext: str) -> str:
        """
        加密
        :param plaintext: 需要加密的内容
        """
        return self._crypt.encrypt(plaintext)

    def decrypt(self, ciphertext: str) -> str:
        """
        解密
        :param ciphertext: 需要解密的内容
        """
        return self._crypt.decrypt(ciphertext)
