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

import uuid
from datetime import datetime

SAFEHASH = [x for x in "0123456789-abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ"]


def compress_uuid():
    r"""
    根据http://www.ietf.org/rfc/rfc1738.txt,由uuid编码扩大字符域生成串
    包括: [0-9a-zA-Z\-_] 共64个
    长度: (32-2)/3*2 = 20 (不用到最后两位)
    @return:
    """
    # 初始32位16进制数
    row = str(uuid.uuid4()).replace("-", "")
    safe_code = ""
    for i in range(10):
        # 获取12位随机二进制数
        enbin = "%012d" % int(bin(int(row[i * 3] + row[i * 3 + 1] + row[i * 3 + 2], 16))[2:], 10)
        # 每次获取 enbin 前6位和后6位二进制数映射的字符拼接的字符串
        # 循环10次
        safe_code += SAFEHASH[int(enbin[0:6], 2)] + SAFEHASH[int(enbin[6:12], 2)]
    return safe_code


def gene_version():
    """
    生成唯一版本号，格式如 V20181029124231#xVsD3JS3i8AUbB4k7Ytl
    """
    base_ver = "V%s" % (datetime.now()).strftime("%Y%m%d%H%M%S")
    version = "{}#{}".format(base_ver, compress_uuid())
    return version
