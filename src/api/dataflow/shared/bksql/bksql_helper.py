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

from dataflow.modeling.exceptions.comp_exceptions import SqlParseError
from dataflow.shared.api.modules.bksql import BksqlApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class BksqlApiHelper(object):
    @staticmethod
    def list_result_tables_by_mlsql_parser(params, sql_index=0):
        res = BksqlApi.list_result_tables_by_mlsql_parser(params)
        res_util.check_response(res, self_message=False)
        mlsql_parse_result = res.data
        if "result" in mlsql_parse_result and not mlsql_parse_result["result"]:
            raise SqlParseError(
                message_kv={
                    "number": sql_index,
                    "content": mlsql_parse_result["content"],
                }
            )
        return mlsql_parse_result

    @staticmethod
    def list_result_entity_by_mlsql_parser(params):
        res = BksqlApi.list_result_entity_by_mlsql_parser(params)
        res_util.check_response(res)
        return res.data
