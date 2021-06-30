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


from datamanage.lite.datamart.handlers.fields import update_orig_fields
from datamanage.lite.datamart.serializers import ResultTableFieldsUpdateSerializer
from datamanage.utils.api import MetaApi
from rest_framework.response import Response

from common.auth import check_perm
from common.decorators import detail_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet


class ResultTableViewSet(APIViewSet):
    lookup_field = "result_table_id"

    @detail_route(methods=["put"], url_path="fields")
    @params_valid(serializer=ResultTableFieldsUpdateSerializer)
    def update_fields(self, request, result_table_id, params):
        """
        @api {post} /datamanage/datamart/result_tables/:result_table_id/fields/ 修改rt字段中文名&描述

        @apiVersion 3.5.0
        @apiGroup ResultTable
        @apiName result_table_update_fields
        @apiDescription 修改rt字段中文名&描述

        @apiParam {List} fields 字段列表

        @apiParamExample {json} 参数样例:
        {
            "fields": [
                {
                    "field_alias":"client_ip_test_test",
                    "field_name":"client_ip",
                    "description":"client_ip_test"
                }
            ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": "591_xx"
            }
        """
        bk_username = get_request_username()
        # 1)权限校验
        check_perm("result_table.update", result_table_id)

        # 2)变更原始字段列表
        # 原始字段列表
        orig_fields = MetaApi.result_tables.fields({"result_table_id": result_table_id}, raise_exception=True).data
        # 按照params['fields']修改原始字段列表的field_alias&description
        update_orig_fields(orig_fields, params["fields"])

        # 3) 变更字段中文名&描述
        edit_ret = MetaApi.result_tables.update(
            {
                "fields": orig_fields,
                "bk_username": bk_username,
                "result_table_id": result_table_id,
            },
            raise_exception=True,
        ).data
        return Response(edit_ret)
