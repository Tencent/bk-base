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


class MagicException(Exception):
    def __init__(self, code, message, info):
        self.code = code
        self.message = message
        self.info = info

    def __str__(self):
        return "{}: {}".format(self.message, self.info)


class AuthException(MagicException):
    code = "01"
    message = "权限校验失败"

    def __init__(self, info):
        self.info = info
        super(MagicException, self).__init__(self.code, self.message, info)


class ApiRequestException(MagicException):
    code = "02"
    message = "周边接口调用失败"

    def __init__(self, info):
        self.info = info
        super(MagicException, self).__init__(self.code, self.message, info)


class ExecuteException(MagicException):
    code = "03"
    message = "执行失败"

    def __init__(self, info):
        self.info = info
        super(MagicException, self).__init__(self.code, self.message, info)
