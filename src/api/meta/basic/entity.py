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

from __future__ import absolute_import, print_function, unicode_literals

import json

from rest_framework.response import Response

from meta.basic.common import RPCView


class ComplexSearchView(RPCView):
    """
    @apiDefine EntityComplexSearch
    """

    """
    @api {post} /meta/basic/entity/complex_search 原生后端查询语言搜索。
    @apiName EntityComplexSearch
    @apiGroup Basic
    @apiVersion 0.1.0
    @apiUse EntityComplexSearch

    @apiParam {String} [statement] 查询语句。
    @apiParam {String="mysql","atlas"} [backend_type] 查询后端。

    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
          "errors": null,
          "message": "ok",
          "code": "1500200",
          "data": [
            {
              "project_name": "测试项目1",
              "updated_by": null,
              "created_at": "2019-01-07T16:55:46+00:00",
              "updated_at": null,
              "created_by":"admin",
              "deleted_by": null,
              "bk_app_code": "data",
              "project_id": 19,
              "active": 1,
              "deleted_at": null,
              "description": "test"
            }
          ],
          "result": true
        }
    """

    def post(self, request):
        json_obj = request.data
        backend_type = json_obj.get("backend_type", "mysql")
        statement = json_obj["statement"]
        token_pkey = json_obj.get("token_pkey", "")
        token_msg = json_obj.get("token_msg", "")
        ret = self.query(request, backend_type, statement, token_pkey, token_msg)
        return Response(ret.result)

    def query(self, request, backend_type, statement, token_pkey, token_msg, **kwargs):
        kwargs["backend_type"] = backend_type
        kwargs["token_pkey"] = token_pkey
        kwargs["token_msg"] = token_msg
        return self.entity_complex_search(statement, **kwargs)


class LineageView(RPCView):
    """
    @apiDefine EntityLineage
    """

    """
    @api {get} /meta/basic/entity/lineage 血缘查询。
    @apiName QueryEntityLineage
    @apiGroup Basic
    @apiVersion 0.1.0
    @apiUse EntityLineage

    @apiParam {String="RawData","ResultTable"} type_name 实体类型
    @apiParam {String} qualified_name 检索属性名
    @apiParam {String="BOTH","INPUT","OUTPUT"} [direction="BOTH"] 血缘查找方向
    @apiParam {Int} [depth=3] 血缘查找深度
    @apiParam {String="mysql","atlas"} [backend_type] 查询后端；若不选择，由后端配置文件控制。

    @apiSuccessExample {json} Atlas-Backend-Success-Response:
    HTTP/1.1 200 OK
    {
      "errors": null,K
      "message": "ok",
      "code": "1500200",
      "data": {
        "lineage": {
          "guidEntityMap": {},
          "lineageDepth": 3,
          "lineageDirection": "BOTH",
          "relations": [],
          "baseEntityGuid": "2bdc52e6-b555-4641-af82-5517a440450b"
        },
        "backend_type": "atlas"
      },
      "result": true
    }
    @apiSuccessExample {json} MySQL-Backend-Success-Response:
    HTTP/1.1 200 OK
     {
      "errors": null,
      "message": "ok",
      "code": "1500200",
      "data": {
        "lineage": {
          "nodes": [
            {
              "type": "data_processing",
              "processing_id": "591_devin_stream2",
              "qualified_name": "591_devin_stream2"
            },
            {
              "data_set_id": "591_devin_stream2",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_devin_stream2"
            },
            {
              "type": "data_processing",
              "processing_id": "591_test2",
              "qualified_name": "591_test2"
            },
            {
              "type": "data_processing",
              "processing_id": "591_devin_stream",
              "qualified_name": "591_devin_stream"
            },
            {
              "data_set_id": "591_test2",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_test2"
            },
            {
              "data_set_id": "591_ziptest",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_ziptest"
            },
            {
              "data_set_id": "591_durant1221",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_durant1221"
            },
            {
              "type": "data_processing",
              "processing_id": "591_ziptest",
              "qualified_name": "591_ziptest"
            },
            {
              "type": "data_processing",
              "processing_id": "clean-table_591_durant1221",
              "qualified_name": "clean-table_591_durant1221"
            },
            {
              "data_set_id": "591_ddrtest4",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_ddrtest4"
            },
            {
              "data_set_id": "591_devin_stream",
              "data_set_type": "result_table",
              "type": "result_table",
              "qualified_name": "591_devin_stream"
            },
            {
              "data_set_id": "100163",
              "data_set_type": "raw_data",
              "type": "raw_data",
              "qualified_name": "100163"
            },
            {
              "type": "data_processing",
              "processing_id": "591_ddrtest4",
              "qualified_name": "591_ddrtest4"
            }
          ],
          "relations": [
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "clean-table_591_durant1221"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "591_test2"
              },
              "from": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "591_ziptest"
              },
              "from": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "591_devin_stream"
              },
              "from": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              }
            },
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_ddrtest4"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "591_ddrtest4"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "591_ddrtest4"
              },
              "from": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              }
            },
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_devin_stream"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "591_devin_stream"
              }
            },
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_devin_stream2"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "591_devin_stream2"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "clean-table_591_durant1221"
              },
              "from": {
                "type": "raw_data",
                "qualified_name": "100163"
              }
            },
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_ziptest"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "591_ziptest"
              }
            },
            {
              "to": {
                "type": "result_table",
                "qualified_name": "591_test2"
              },
              "from": {
                "type": "data_processing",
                "qualified_name": "591_test2"
              }
            },
            {
              "to": {
                "type": "data_processing",
                "qualified_name": "591_devin_stream2"
              },
              "from": {
                "type": "result_table",
                "qualified_name": "591_durant1221"
              }
            }
          ],
          "criteria": {
            "data_set_id": "591_durant1221",
            "depth": 1,
            "direction": "BOTH",
            "entity_type": "ResultTable"
          }
        },
        "backend_type": "mysql"
      },
      "result": true
    }
    """

    def get(self, request):
        query_params = {
            "entity_type": request.query_params["type_name"],
            "attr_value": request.query_params["qualified_name"],
        }
        for key in request.query_params:
            if key in ("direction", "depth", "backend_type", "only_user_entity"):
                query_params[key] = request.query_params[key]
            if key == "extra_retrieve":
                query_params[key] = json.loads(request.query_params[key])

        ret = self.entity_query_lineage(**query_params)
        return Response(ret.result)


class NotFoundView(RPCView):
    """
    @apiDefine NotFoundView
    """

    """
    not found view
    """

    @staticmethod
    def list(request):
        not_found_ret = "your query target is not found"
        return Response(not_found_ret)
