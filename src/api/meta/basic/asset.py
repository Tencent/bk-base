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
from rest_framework import serializers
from rest_framework.response import Response

from meta.basic.common import RPCView


class ERPSerializer(serializers.Serializer):
    retrieve_args = serializers.JSONField()
    backend_type = serializers.ChoiceField(("dgraph", "dgraph_backup", "dgraph_cold"), default="dgraph")
    version = serializers.IntegerField(default=2)


class QueryViaERPView(RPCView):
    @params_valid(serializer=ERPSerializer)
    def post(self, request, params):
        if params["backend_type"] == "dgraph":
            params.pop("backend_type")
        ret = self.query(request, params)
        return Response(ret.result)

    def query(self, request, params, **kwargs):
        kwargs.update(params)
        return self.entity_query_via_erp(**kwargs)
