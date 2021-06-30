# coding=utf-8
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

# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有

from __future__ import absolute_import, print_function, unicode_literals


class ErrorCodes(object):
    # 数据平台的平台代码--7位错误码的前2位
    BKDATA_PLAT_CODE = "15"
    # 数据平台子模块代码--7位错误码的3，4位
    BKDATA_METADATA = "30"


class MetaDataErrorCodes(object):
    COMMON = 000
    CLIENT = 500
    TASK = 550

    def __init_(
        self,
        parent_err_codes=ErrorCodes,
    ):
        self.parent_err_codes = parent_err_codes
        for k, v in vars(self.__class__).items():
            if k.isupper():
                setattr(self, k, self.full_error_code(v))

    def full_error_code(self, sub_code):
        return int(
            self.parent_err_codes.BKDATA_PLAT_CODE
            + self.parent_err_codes.BKDATA_METADATA
            + str(sub_code)
        )


class StandardErrorMixIn(Exception):
    code = None
    msg_template = None
    default_msg = None
    data = None

    def __init__(self, *args, **kwargs):
        if "message_kv" in kwargs:
            if not args:
                message = self.msg_template.format(**kwargs[str("message_kv")])
            elif len(args) == 1:
                message = args[0].format(**kwargs[str("message_kv")])
            else:
                raise TypeError(
                    "Multi args and message_kv is not allowed at same time in standard error."
                )
        elif args:
            if len(args) == 1:
                message = args[0]
            else:
                message = self.msg_template.format(*args)
        else:
            message = self.default_msg

        self.data = {}
        if "data" in kwargs:
            data = kwargs.pop(str("data"))
            if not isinstance(data, dict):
                self.data["content"] = data
            else:
                self.data.update(data)

        if message:
            super(StandardErrorMixIn, self).__init__(
                message.encode("utf8") if isinstance(message, str) else message
            )
        else:
            super(StandardErrorMixIn, self).__init__()


class MetaDataError(StandardErrorMixIn, Exception):
    code = MetaDataErrorCodes().COMMON
    msg_template = "MetaData System Error: {error}."


class MetaDataTaskError(MetaDataError):
    code = MetaDataErrorCodes().TASK + 1
    msg_template = "MetaData Task Error: {error}."
