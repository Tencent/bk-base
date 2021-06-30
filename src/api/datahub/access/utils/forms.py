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

import datetime
import json

from common.log import logger


def check_ip_rule(ipaddr):
    ipaddr = str(ipaddr)
    addr = ipaddr.strip().split(".")  # 切割IP地址为一个列表
    # print addr
    if len(addr) != 4:  # 切割后列表必须有4个参数
        return False
    for i in range(4):
        try:
            addr[i] = int(addr[i])  # 每个参数必须为数字，否则校验失败
        except Exception:
            logger.warning("check ip rule failed, %s not a nums" % i)
            return False
        if 0 <= int(addr[i]) <= 255:  # 每个参数值必须在0-255之间
            pass
        else:
            return False
        i += 1
    else:
        # 屏蔽回网地址
        if addr[0] == 127:
            return False
        return True


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()
            return str(obj)
        else:
            encoded_object = json.JSONEncoder.default(self, obj)
        return encoded_object
