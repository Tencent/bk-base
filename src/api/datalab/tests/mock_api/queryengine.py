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
import re

import httpretty as hp
from datalab.pizza_settings import QUERYENGINE_API_ROOT


def mock_query_async_success():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/query_async/"),
        body=json.dumps(
            {
                "result": True,
                "data": {
                    "select_fields_order": "ip",
                    "query_id": "bk123",
                },
            }
        ),
    )


def mock_query_async_failed():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/query_async/"),
        body=json.dumps(
            {
                "result": False,
                "message": "error_message",
                "errors": {"query_id": "bk123"},
            }
        ),
    )


def mock_query_info():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/query_async/info_list/"),
        body=json.dumps({"result": True, "data": [{"sql_text": "xxx"}]}),
    )


def mock_query_result():
    hp.register_uri(
        hp.GET,
        re.compile(QUERYENGINE_API_ROOT + "/query_async/result/"),
        body=json.dumps({"result": True, "data": {"totalRecords": 1, "select_fields_order": "ip"}}),
    )


def mock_query_stage():
    hp.register_uri(
        hp.GET,
        re.compile(QUERYENGINE_API_ROOT + "/query_async/stage/"),
        body=json.dumps(
            {
                "result": True,
                "data": [
                    {
                        "query_id": "BK11",
                        "stage_seq": 1,
                        "stage_type": "checkQuerySyntax",
                        "stage_start_time": "2021-04-29 19:15:47.608",
                        "stage_end_time": "2021-04-29 19:15:47.629",
                        "stage_cost_time": 21,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 2,
                        "stage_type": "checkPermission",
                        "stage_start_time": "2021-04-29 19:15:47.637",
                        "stage_end_time": "2021-04-29 19:15:47.688",
                        "stage_cost_time": 51,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 3,
                        "stage_type": "pickValidStorage",
                        "stage_start_time": "2021-04-29 19:15:47.695",
                        "stage_end_time": "2021-04-29 19:15:47.767",
                        "stage_cost_time": 72,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 4,
                        "stage_type": "matchQueryForbiddenConfig",
                        "stage_start_time": "2021-04-29 19:15:47.775",
                        "stage_end_time": "2021-04-29 19:15:47.783",
                        "stage_cost_time": 8,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 5,
                        "stage_type": "checkQuerySemantic",
                        "stage_start_time": "2021-04-29 19:15:47.799",
                        "stage_end_time": "2021-04-29 19:15:47.847",
                        "stage_cost_time": 48,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 6,
                        "stage_type": "matchQueryRoutingRule",
                        "stage_start_time": "2021-04-29 19:15:47.854",
                        "stage_end_time": "2021-04-29 19:15:47.862",
                        "stage_cost_time": 8,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 7,
                        "stage_type": "convertQueryStatement",
                        "stage_start_time": "2021-04-29 19:15:47.887",
                        "stage_end_time": "2021-04-29 19:15:48.019",
                        "stage_cost_time": 132,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 8,
                        "stage_type": "getQueryDriver",
                        "stage_start_time": "2021-04-29 19:15:48.035",
                        "stage_end_time": "2021-04-29 19:15:48.041",
                        "stage_cost_time": 6,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 9,
                        "stage_type": "connectDb",
                        "stage_start_time": "2021-04-29 19:15:48.056",
                        "stage_end_time": "2021-04-29 19:15:48.08",
                        "stage_cost_time": 24,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 10,
                        "stage_type": "queryDb",
                        "stage_start_time": "2021-04-29 19:15:48.087",
                        "stage_end_time": "2021-04-29 19:15:57.983",
                        "stage_cost_time": 9896,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                    {
                        "query_id": "BK11",
                        "stage_seq": 11,
                        "stage_type": "writeCache",
                        "stage_start_time": "2021-04-29 19:15:57.983",
                        "stage_end_time": "2021-04-29 19:16:03.983",
                        "stage_cost_time": 6,
                        "stage_status": "finished",
                        "error_message": "",
                    },
                ],
            }
        ),
    )


def mock_sqltype_and_result_tables():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/sqlparse/sqltype_and_result_tables/"),
        body=json.dumps(
            {
                "result": True,
                "data": {
                    "statement_type": "DML_SELECT",
                },
            }
        ),
    )


def mock_query_es():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/query_sync/"),
        body=json.dumps(
            {
                "result": True,
                "data": {
                    "list": {"hits": {"total": 10, "hits": [{"_source": {"ip": "xx"}}]}, "took": 0.1},
                    "select_fields_order": "ip",
                },
            }
        ),
    )


def mock_get_es_chart():
    hp.register_uri(
        hp.POST,
        re.compile(QUERYENGINE_API_ROOT + "/query_sync/"),
        body=json.dumps(
            {"result": True, "data": {"list": {"hits": {"total": 0, "aggregations": {"time_aggs": {"buckets": []}}}}}}
        ),
    )
