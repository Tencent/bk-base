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
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.shared.utils.error_codes import get_codes
from dataflow.udf.exceptions.comp_exceptions import UdfCode


class BaseViewSet(APIViewSet):
    """
    format return
    """

    def return_ok(self, data=None):
        return Response(data)


class HealthzViewSet(BaseViewSet):
    def get(self, request):
        """
        @api {post} /dataflow/udf/healthz 测试
        @apiName healthz
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        return self.return_ok("Api server is ok.")


class UDFErrorCodesView(BaseViewSet):
    def get(self, request):
        """
        @api {post} /dataflow/udf/errorcodes/ 获取UDF的errorcodes
        @apiName errorcodes
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [[
                        {
                        message: "函数开发界面被锁定",
                        code: "1581001",
                        name: "DEV_LOCK_ERR",
                        solution: "-",
                        }]
                    "result": true
                }
        """
        return self.return_ok(get_codes(UdfCode, ErrorCode.BKDATA_DATAAPI_UDF))
