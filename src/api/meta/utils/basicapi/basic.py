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

import json

from meta.basic.common import RPCMixIn


class _BasicApi(RPCMixIn):
    def get_lineage(self, params):
        try:
            query_params = {
                "entity_type": params["type_name"],
                "attr_value": params["qualified_name"],
            }
            for key in params:
                if key in ("direction", "depth", "backend_type", "only_user_entity"):
                    query_params[key] = params[key]
                if key == "extra_retrieve":
                    query_params[key] = json.loads(params[key])

            ret = self.entity_query_lineage(**query_params)
            return ret.result
        except Exception as e:
            raise e

    def get_complex_search(self, statement, backend_type="mysql", **kwargs):
        query_params = {
            "token_pkey": kwargs.get("token_pkey", ""),
            "token_msg": kwargs.get("token_msg", ""),
        }

        try:
            rpc_response = self.entity_complex_search(statement, backend_type=backend_type, **query_params)
            return rpc_response.result
        except Exception as e:
            raise e


BasicApi = _BasicApi()
