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

import base64

from Crypto import Random
from Crypto.Cipher import AES

from metadata.runtime import rt_g


class TokenManager(object):

    public_key = ''

    def __init__(self, private_key=''):
        normal_conf = rt_g.config_collection.normal_config
        self.token_dict = normal_conf.TOKEN_DICT
        self.filling_chr = normal_conf.TOKEN_FILLING_CHR
        self.public_key = normal_conf.TOKEN_PUBLIC_KEY
        self.private_key = self.private_key_format(private_key)
        self.is_legal = self.check_key_illegal()

    def private_key_format(self, pk):
        """
        格式化private_key为标准AES格式<#pk###...>右侧#补满16位
        :param pk: 私钥
        :return: 格式化之后的标准16位AES私钥
        """
        pk = ("{:" + self.filling_chr + "<16}").format(self.filling_chr + pk)
        return pk[:16]

    def check_key_illegal(self):
        """
        检查private_key是否合法注册
        :return: boolean 合法与否
        """
        if not self.private_key or self.private_key.strip(self.filling_chr) not in self.token_dict:
            return False
        return True

    def generate_token(self):
        if not self.is_legal:
            raise
        secret_bin = AES.new(self.private_key, AES.MODE_CBC, self.public_key).encrypt(self.private_key)
        return base64.urlsafe_b64encode(secret_bin).decode('utf-8').strip('=')

    def check_shunt(self, custom_token):
        """
        根据token判断用户请求是否分配到主集群
        :param custom_token: 用户上传的token信息
        :return: True - 分流到备集群 / False - 不分流
        """
        if not self.is_legal:
            return False
        fill_cnt = 4 - (len(custom_token) % 4)
        custom_token = "{}{}".format(custom_token, '=' * fill_cnt)
        secret_bin = base64.urlsafe_b64decode(str(custom_token))
        msg = AES.new(self.private_key, AES.MODE_CBC, self.public_key).decrypt(secret_bin)
        # token检查通过则返回False，表示不分流，继续访问主集群
        return False if msg == self.private_key else True


class BaseCrypt(object):
    _bk_crypt = False

    def __init__(self):
        db_conf = rt_g.config_collection.db_config
        self.INSTANCE_KEY = db_conf.CRYPT_INSTANCE_KEY
        self.ROOT_KEY = db_conf.CRYPT_ROOT_KEY
        self.ROOT_IV = db_conf.CRYPT_ROOT_IV

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
        secret_txt = base64.decodebytes(ciphertext)
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
        decode_key = base64.decodebytes(self.INSTANCE_KEY)
        return self.__decrypt(decode_key)
