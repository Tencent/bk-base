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

from dataflow.batch.error_code.errorcodes import DataapiBatchCode
from dataflow.flow.exceptions import FlowCode
from dataflow.shared.utils.error_codes import get_codes
from dataflow.stream.exceptions.comp_execptions import StreamCode
from dataflow.udf.exceptions.comp_exceptions import UdfCode


class ErrorCodeView(APIViewSet):
    """
    @api {get} /dataflow/errorcodes dataflow错误码汇总
    @apiName HealthCheck
    @apiGroup DataFlow
    @apiVersion 1.0.0
    @apiSuccessExample {json} 成功返回
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": "ok",
                "message": "",
                "code": "1500200",
            }
    """

    def errorcodes(self, request):
        rtn = []
        rtn.extend(get_codes(StreamCode, ErrorCode.BKDATA_DATAAPI_STREAM))
        rtn.extend(get_codes(UdfCode, ErrorCode.BKDATA_DATAAPI_UDF))
        rtn.extend(get_codes(FlowCode, ErrorCode.BKDATA_DATAAPI_FLOW))
        rtn.extend(get_codes(DataapiBatchCode, ErrorCode.BKDATA_DATAAPI_BATCH))
        return Response(rtn)
