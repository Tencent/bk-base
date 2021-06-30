# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from rest_framework.response import Response

from codecheck.error_code.errorcodes import get_codes
from codecheck.exceptions.base_exception import CodeCheckCommonCode
from common.errorcodes import ErrorCode
from common.views import APIViewSet


class CodeCheckErrorCodesView(APIViewSet):
    def get(self, request):
        """
        @api {post} /coodecheck/errorcodes/ 获取codecheck的errorcodes
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
        return Response(data=get_codes(CodeCheckCommonCode, ErrorCode.BKDATA_DATAAPI_CODECHECK))
