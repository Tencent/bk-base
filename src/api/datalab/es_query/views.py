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

from common.decorators import detail_route
from common.local import get_request_username
from common.log import sys_logger
from common.views import APIViewSet
from datalab.constants import (
    COUNT,
    END_TIME,
    INTERVAL,
    KEYWORD,
    PAGE,
    PAGE_SIZE,
    RESULT_TABLE_ID,
    RESULTS,
    START_TIME,
    TOTAL_PAGE,
)
from datalab.es_query.models import DatalabEsQueryHistory
from datalab.es_query.result_table import ResultTable
from datalab.es_query.utils import ES
from rest_framework.response import Response


class DatalabEsQueryViewSet(APIViewSet):

    lookup_field = RESULT_TABLE_ID

    @detail_route(methods=["post"], url_path="query")
    @ResultTable.save_es_query_history()
    def query(self, request, result_table_id):
        """
        @api {post} /v3/datalab/es_query/:rt_id/query/  ES查询
        @apiName query
        @apiGroup es_query
        @apiParam {Int} page 开启分页，指定页码，需要与 page_size 同时使用
        @apiParam {Int} page_size 开启分页，指定一页数量，需要与 page 同时使用
        @apiParam {String} keyword 搜索关键词
        @apiParam {String} start_time 开始时间
        @apiParam {String} end_time 结束时间

        @apiSuccessExample {json} 成功返回:
        {
            "message": "",
            "code": "00",
            "data": {
                "select_fields_order": [
                    "dtEventTimeStamp",
                    "dtEventTime",
                    "f2",
                    "localTime"
                ],
                "total": 293898,
                "list": [
                    {
                        "dtEventTimeStamp": 1609149461000,
                        "dtEventTime": "2020-12-28 09:57:41",
                        "f2": "value",
                        "localTime": "2020-12-28 09:00:41"
                    },
                ],
                "time_taken": 2.12
            },
            "result": true
        }
        """
        params = self.request.data
        page_size = params.get(PAGE_SIZE)
        start = (params.get(PAGE) - 1) * page_size
        dsl = ES.get_search_dsl(
            page_size, start, params.get(KEYWORD), params.get(START_TIME), params.get(END_TIME), result_table_id
        )
        sys_logger.info(
            "es_query_dsl|result_table_id: {}|keyword: {}|dsl: {}".format(result_table_id, params.get(KEYWORD), dsl)
        )
        data = ES.query(dsl)
        return Response(data)

    @detail_route(methods=["post"], url_path="get_es_chart")
    def get_es_chart(self, request, result_table_id):
        """
        @api {post} /v3/datalab/es_query/:rt_id/get_es_chart/  ES查询图
        @apiName get_es_chart
        @apiGroup es_query
        @apiParam {String} date_interval 时间区间（12h, 24h, 7d）
        @apiParam {String} keyword 搜索关键词
        @apiSuccessExample {json} 成功返回:
        {
            "cnt": [10, 20, 30, 0, 1677, 1418, 1439],
            "time": ["11-11", "11-12", "11-13", "11-14", "11-15", "11-16", "11-17"]
        }
        """
        params = self.request.data
        dsl = ES.get_chart_dsl(
            params.get(KEYWORD), params.get(INTERVAL), params.get(START_TIME), params.get(END_TIME), result_table_id
        )
        sys_logger.info(
            "es_query_dsl|result_table_id: {}|keyword: {}|dsl: {}".format(result_table_id, params.get(KEYWORD), dsl)
        )
        data = ES.get_es_chart(dsl)
        return Response(data)

    @detail_route(methods=["get"], url_path="list_query_history")
    def list_query_history(self, request, result_table_id=None):
        """
        @api {get} /v3/datalab/es_query/:rt_id/list_query_history/  结果表查询历史
        @apiName list_query_history
        @apiGroup es_query
        @apiParam {Int} [storage_type] 存储类型
        @apiParam {Int} [page] 开启分页，指定页码，需要与 page_size 同时使用
        @apiParam {Int} [page_size] 开启分页，指定一页数量，需要与 page 同时使用
        @apiSuccessExample {json} 成功返回:
          {
            "message": "",
            "code": "00",
            "data": [
                {
                    "time": "2017-10-01T16:07:28",
                    "keyword": "running"
                },
                {
                    "time": "2017-10-01T16:10:45",
                    "keyword": "test"
                }
            ],
            "result": true
        }
        """
        data = DatalabEsQueryHistory.list_query_history(get_request_username(), result_table_id)
        if [PAGE, PAGE_SIZE] <= list(request.query_params.keys()):
            page = int(request.query_params[PAGE])
            page_size = int(request.query_params[PAGE_SIZE])

            count = len(data)
            total_page = (count + page_size - 1) / page_size
            data = data[page_size * (page - 1) : page_size * page]
            data_paging = {TOTAL_PAGE: total_page, COUNT: count, RESULTS: data}
        else:
            # 无分页请求时返回全部
            data_paging = {TOTAL_PAGE: 1, COUNT: len(data), RESULTS: data}
        return Response(data_paging)
