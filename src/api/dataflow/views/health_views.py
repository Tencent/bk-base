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

from common.views import APIViewSet
from django.db import connections
from django.db.utils import OperationalError
from rest_framework.response import Response

from dataflow.batch.check.check_driver import check as batch_check
from dataflow.flow.check import flow_check
from dataflow.stream.check.check_driver import check as stream_check


def mysql_check():
    db_conn = connections["default"]
    try:
        db_conn.cursor()
    except OperationalError:
        return {"mysql": {"status": False, "message": "DataFlow Mysql实例不可用"}}
    else:
        return {"mysql": {"status": True, "message": ""}}


class HealthCheckView(APIViewSet):
    """
    @api {get} /dataflow/healthz 健康状态
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

    def healthz(self, request):
        # 1 检查组件
        rtn = {}
        rtn.update(batch_check())
        rtn.update(stream_check())
        rtn.update(mysql_check())
        rtn.update(flow_check())
        # todo 2 检查全流程
        return Response(rtn)
