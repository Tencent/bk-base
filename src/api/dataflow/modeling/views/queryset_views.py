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

from common.decorators import list_route, params_valid
from common.django_utils import DataResponse
from common.local import get_request_username
from rest_framework.response import Response

from dataflow.flow.handlers.generic import APIViewSet
from dataflow.modeling.basic_model.model_controller import ModelingDDLOperator
from dataflow.modeling.exceptions.comp_exceptions import DEFAULT_MODELING_ERR, TableNotExistsError
from dataflow.modeling.queryset.queryset_serializer import DropQuerySetSerializer, TruncateQuerySetSerializer
from dataflow.shared.permission import require_username


class QuerySetViewSet(APIViewSet):
    @list_route(methods=["delete"], url_path="delete_table")
    @require_username
    @params_valid(serializer=DropQuerySetSerializer)
    def delete_table(self, request, params):
        """
        @api {delete} /dataflow/modeling/model/delete_table 删除表
        @apiName delete table
        @apiGroup Flow
        @apiParam {string} result_table_id
        @apiParam {int} notebook_id
        @apiParam {int} cell_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 'table deleted',
                "result": true
            }
        """
        notebook_id = params["notebook_id"]
        cell_id = params["cell_id"]
        result_table_id = params["result_table_id"]
        try:
            ddl_oper = ModelingDDLOperator(result_table_id, notebook_id, cell_id, get_request_username())
            ddl_oper.drop()
            return Response("表已删除")
        except TableNotExistsError as e:
            return DataResponse(result=False, code=e.code, message=e.message, data=e.message)
        except Exception as e:
            error = "删除表异常:%s" % e
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message=error, data=error)

    @list_route(methods=["delete"], url_path="truncate_table")
    @require_username
    @params_valid(serializer=TruncateQuerySetSerializer)
    def truncate_table(self, request, params):
        """
        @api {delete} /dataflow/modeling/queryset/:result_table_id/truncate_table 清空表
        @apiName truncate table
        @apiGroup Flow
        @apiParam {string} result_table_id
        @apiParam {int} notebook_id
        @apiParam {int} cell_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 'table truncated',
                "result": true
            }
        """
        notebook_id = params["notebook_id"]
        cell_id = params["cell_id"]
        result_table_id = params["result_table_id"]
        try:
            ddl_oper = ModelingDDLOperator(result_table_id, notebook_id, cell_id, get_request_username())
            ddl_oper.truncate()
            return Response("表已清空")
        except TableNotExistsError as e:
            error = "请检查表是否存在"
            return DataResponse(result=False, code=e.code, message=error, data=error)
        except Exception as e:
            error = "清空表异常:%s" % e
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message=error, data=error)
