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


from rest_framework.response import Response

from meta import exceptions as meta_errors
from meta.basic.common import RPCViewSet


class ErrorCodesViewSet(RPCViewSet):
    """
    @api {get} /v3/meta/errorcodes/ 错误码
    @apiName ErrorCodes
    @apiGroup Common
    @apiVersion 1.0.0
    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": [
                    {
                        "code": "1582000",
                        "message": "MetaApi通用异常”,
                        "name': "MetaApiCommonError",
                        "solution": "-"
                    }
                ],
                "message": "ok",
                "code": "1500200",
                "errors": null
            }
    """

    def list(self, request):
        errors = []
        for error_name in dir(meta_errors):
            error_class = getattr(meta_errors, error_name)
            if (
                isinstance(error_class, type)
                and issubclass(error_class, meta_errors.MetaError)
                and error_class is not meta_errors.MetaError
            ):
                errors.append(
                    {
                        "code": error_class().code,
                        "message": error_class().message,
                        "name": error_name,
                        "solution": error_class().solution or "-",
                    }
                )
        return Response(errors)
