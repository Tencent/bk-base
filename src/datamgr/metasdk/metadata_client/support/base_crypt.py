# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from __future__ import absolute_import, unicode_literals

import base64

from Crypto import Random
from Crypto.Cipher import AES


class BaseCrypt(object):
    _bk_crypt = False
    ROOT_KEY = "TencentBkdataKEY"
    ROOT_IV = "TencentBkdata IV"

    def __init__(self, instance_key=""):
        self.INSTANCE_KEY = instance_key

    @staticmethod
    def instance(instance_key=""):
        return BaseCrypt(instance_key)

    def generate_instance_key(self):
        """
        生成秘钥，加密解密时需要传入
        :return:
        """
        random = Random.new().read(AES.block_size)
        return base64.b64encode(self.__encrypt(random))

    def set_instance_key(self, instance_key):
        self.INSTANCE_KEY = instance_key

    def encrypt(self, plaintext):
        """
        加密
        :param plaintext: 需要加密的内容
        :return:
        """
        decrypt_key = self.__parse_key()
        secret_txt = AES.new(decrypt_key, AES.MODE_CFB, self.ROOT_IV).encrypt(plaintext)
        base64_txt = base64.b64encode(secret_txt)
        return base64_txt

    def decrypt(self, ciphertext):
        """
        解密
        :param ciphertext: 需要解密的内容
        :return:
        """
        decrypt_key = self.__parse_key()
        # 先解base64
        secret_txt = base64.b64decode(ciphertext)
        # 再解对称加密
        plain = AES.new(decrypt_key, AES.MODE_CFB, self.ROOT_IV).decrypt(secret_txt)
        return plain

    def __encrypt(self, plaintext):
        """
        根据私钥加密，内部方法，请勿调用
        :param plaintext: 需要加密的内容
        :return:
        """
        return AES.new(self.ROOT_KEY, AES.MODE_CFB, self.ROOT_IV).encrypt(plaintext)

    def __decrypt(self, ciphertext):
        """
        根据私钥解密，内部方法，请勿调用
        :param ciphertext: 需要加密的内容
        :return:
        """
        return AES.new(self.ROOT_KEY, AES.MODE_CFB, self.ROOT_IV).decrypt(ciphertext)

    def __parse_key(self):
        decode_key = base64.b64decode(self.INSTANCE_KEY)
        return self.__decrypt(decode_key)
