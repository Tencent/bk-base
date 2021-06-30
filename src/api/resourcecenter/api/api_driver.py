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
import inspect
import sys

from django.utils.translation import ugettext as _

from common.exceptions import ApiRequestError, ApiResultError


class APIResponseUtil(object):
    @staticmethod
    def check_response(res, check_success=True, self_message=True):
        """
        res.data         ==> 接口返回的data内容
        res.message      ==> 接口返回的message
        res.is_success() ==> 接口返回的result是否为True
        res.code         ==> 接口返回的code
        """
        stack = inspect.stack()
        code_obj = stack[1][0].f_code
        check_response_idx = code_obj.co_names.index(sys._getframe().f_code.co_name)
        #  基于当前方法名倒推API类名位置，eg:
        #  res = StorekitApi.storage_result_tables.get_physical_table_name()
        #  res_util.check_response(res)
        #     ('StorekitApi', 'storage_result_tables', 'get_physical_table_name', 'res_util', 'check_response', 'data')
        class_name_idx = check_response_idx - 4 if check_response_idx - 4 >= 0 else 0
        class_name = code_obj.co_names[class_name_idx]

        if not res:
            raise ApiRequestError(message=_("API返回为空"))
        if check_success and not res.is_success():
            raise ApiRequestError(
                message=_("调用%(api_name)s失败(%(code)s) - %(message)s")
                % {"api_name": class_name, "code": res.code, "message": res.message}
                if self_message
                else res.message,
                code=res.code,
            )
        if not hasattr(res, "data"):
            raise ApiResultError(message=_("API返回格式错误"))
