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

from common.errorcodes import ErrorCode
from common.views import APIView
from datalab.constants import CODE, MESSAGE, NAME, SOLUTION
from datalab.exceptions import DatalabCode
from rest_framework.response import Response


class ErrorCodesView(APIView):

    """
    @api {get} /v3/datalab/errorcodes 错误码接口
    @apiName errorcodes
    @apiGroup codes
    @apiVersion 1.0.0
    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": [
                    {
                        "message": "笔记任务不存在",
                        "code": "1580004",
                        "name": "NOTEBOOK_TASK_NOT_FOUND",
                        "solution": "检查笔记任务是否存在"
                    },
                    {
                        "message": "笔记用户不存在",
                        "code": "1580005",
                        "name": "NOTEBOOK_USER_NOT_FOUND",
                        "solution": "检查笔记用户是否存在"
                    }
                ],
                "message": "ok",
                "code": "1500200",
                "errors": null
            }
    """

    def get(self, request):
        return Response(get_codes(DatalabCode, ErrorCode.BKDATA_DATAAPI_DATALAB))


def get_codes(code_cls, module_code):
    """
    从框架的标准错误码管理类中提取标准化信息
    """
    error_codes = []
    for name in dir(code_cls):
        if name.startswith("__"):
            continue
        value = getattr(code_cls, name)
        if type(value) == tuple:
            error_codes.append(
                {
                    CODE: "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, module_code, value[0]),
                    NAME: name,
                    MESSAGE: value[1],
                    SOLUTION: value[2] if len(value) > 2 else "-",
                }
            )
    error_codes = sorted(error_codes, key=lambda _c: _c[CODE])
    return error_codes
