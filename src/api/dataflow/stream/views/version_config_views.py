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

from common.decorators import params_valid

from dataflow.stream.version_config.version_config_driver import create_version
from dataflow.stream.version_config.version_config_serializers import CreateVersionConfig
from dataflow.stream.views.base_views import BaseViewSet


class VersionConfigViewSet(BaseViewSet):
    """
    版本控制 api
    """

    @params_valid(serializer=CreateVersionConfig)
    def create(self, request, params):
        """
        @api {post} /dataflow/stream/version_config/ 注册集群元信息
        @apiName version_config/
        @apiGroup Stream
        @apiParam {string} component_type
        @apiParam {string} branch
        @apiParam {string} version
        @apiParamExample {json} 参数样例:
            {
                "component_type": "storm",
                "branch": "stormRT2.0",
                "version": "0.0.1"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": "",
                    "result": true
                }
        """
        create_version(params)
        return self.return_ok()
