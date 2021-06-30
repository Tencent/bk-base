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
import mock
import pytest
import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers


@pytest.fixture(scope='class')
def patch_es_search_data():

    Elasticsearch.search = mock.Mock()
    opr_info = {
        "kv_tpl": "{pred_name}: {before} -> {after}",
        "alias": "\\u7ed3\\u6784\\u53d8\\u5316",
        "desc_params": [{"pred_name": "field_alias", "after": "metric1111111", "before": "metric222"}],
        "desc_tpl": "\\u5b57\\u6bb5\\u522b\\u540d\\u53d8\\u66f4: {change_content}",
        "jump_to": {},
        "sub_type_alias": "\\u5b57\\u6bb5\\u522b\\u540d\\u53d8\\u66f4",
        "show_type": "add_tips",
    }
    Elasticsearch.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_index": "data_trace_log",
                    "_type": "data_trace",
                    "_id": "f90301db-8d6e-45ae-b769-551dbc9ae124",
                    "_source": {
                        "opr_type": "update_schema",
                        "dispatch_id": "b9d2508a-6ae0-44d5-b874-72b82f6411e6",
                        "opr_sub_type": "update_alias_desc",
                        "created_at": "2021-04-13 10:50:07",
                        "created_by": "admin",
                        "data_set_id": "591_test_indicator",
                        "opr_info": json.dumps(opr_info),
                        "id": "f90301db-8d6e-45ae-b769-551dbc9ae124",
                        "description": "字段别名变更: field_alias: metric222 -> metric1111111",
                    },
                }
            ]
        }
    }


@pytest.fixture(scope='class')
def patch_es_bulk_data():
    """es批量插入数据"""

    def skip(*args, **kwargs):
        pass

    helpers.bulk = skip
