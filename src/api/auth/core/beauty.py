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
import sys
from functools import wraps

import attr
from auth.exceptions import AuthAPIError


@attr.s
class AuthenticationResult:
    result = attr.ib(type=bool, default=True)
    message = attr.ib(type=str, default="")
    code = attr.ib(type=str, default="1500200")


def hanlder_auth_check_exception(check_func):
    """
    对鉴权函数进行封装，优化返回结果
    """

    def _deoc(*args, **kwargs):
        raise_exception = kwargs.get("raise_exception", False)
        display_detail = kwargs.get("display_detail", False)

        try:
            ret = check_func(*args, **kwargs)
            # 如果返回结果已经是被格式化对象，则不需要在重新格式化
            if not isinstance(ret, bool):
                return ret

            auth_result = AuthenticationResult(ret)

        except AuthAPIError as e:
            if raise_exception:
                raise e.with_traceback(sys.exc_info()[2])

            auth_result = AuthenticationResult(False, e.message, e.code)

        if display_detail:
            return attr.asdict(auth_result)

        return auth_result.result

    return wraps(check_func)(_deoc)
