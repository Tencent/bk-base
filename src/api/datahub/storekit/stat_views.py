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

import time

from common.decorators import list_route
from common.views import APIViewSet
from datahub.common.const import (
    BK_BIZ_ID,
    COMPONENT,
    DATE,
    PROJECT_ID,
    RESULT_TABLE_ID,
    SERIES,
    TIME,
)
from datahub.storekit import stat, util
from datahub.storekit.serializers import StatsConditionSerializer
from datahub.storekit.settings import STAT_DATABASE
from rest_framework.response import Response


class StatBizSet(APIViewSet):
    lookup_field = BK_BIZ_ID

    def retrieve(self, request, bk_biz_id):
        """
        @api {get} v3/storekit/stats/biz/:bk_biz_id/ 获取业务统计信息
        @apiGroup Stats
        @apiDescription 获取指定业务的统计信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        condition = f"{BK_BIZ_ID}='{bk_biz_id}'"
        result = stat.get_metrics(params, "daily_count_biz", condition)
        return Response(result)

    def list(self, request):
        """
        @api {get} v3/storekit/stats/biz/ 获取业务统计列表
        @apiGroup Stats
        @apiDescription 获取所有业务的统计信息
        @apiParam {String} limit 返回数据条数，可选参数，默认值100
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        result = stat.get_metrics(params, "daily_count_biz")
        return Response(result)


class StatProjectSet(APIViewSet):
    lookup_field = PROJECT_ID

    def retrieve(self, request, project_id):
        """
        @api {get} v3/storekit/stats/project/:project_id/ 获取项目统计信息
        @apiGroup Stats
        @apiDescription 获取指定项目的统计信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        condition = f"{PROJECT_ID}='{project_id}'"
        result = stat.get_metrics(params, "daily_count_project", condition)
        return Response(result)

    def list(self, request):
        """
        @api {get} v3/storekit/stats/project/ 获取项目统计列表
        @apiGroup Stats
        @apiDescription 获取所有项目的统计信息
        @apiParam {String} limit 返回数据条数，可选参数，默认值100
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        result = stat.get_metrics(params, "daily_count_project")
        return Response(result)


class StatComponentSet(APIViewSet):
    lookup_field = COMPONENT

    def retrieve(self, request, component):
        """
        @api {get} v3/storekit/stats/component/:component/ 获取模块统计信息
        @apiGroup Stats
        @apiDescription 获取指定模块的统计信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        condition = f"{COMPONENT}='{component}'"
        result = stat.get_metrics(params, "daily_count_component", condition)
        return Response(result)

    def list(self, request):
        """
        @api {get} v3/storekit/stats/component/ 获取模块统计列表
        @apiGroup Stats
        @apiDescription 获取所有模块的统计信息
        @apiParam {String} limit 返回数据条数，可选参数，默认值100
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        result = stat.get_metrics(params, "daily_count_component")
        return Response(result)


class StatRtSet(APIViewSet):
    lookup_field = RESULT_TABLE_ID

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/stats/result_tables/:result_table_id/ 获取rt统计信息
        @apiGroup Stats
        @apiDescription 获取指定rt的统计信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        condition = f"rt_id='{result_table_id}'"
        result = stat.get_metrics(params, "daily_count_rt", condition)
        return Response(result)

    def list(self, request):
        """
        @api {get} v3/storekit/stats/result_tables/ 获取rt统计列表
        @apiGroup Stats
        @apiDescription 获取所有rt的统计信息
        @apiParam {String} limit 返回数据条数，可选参数，默认值100
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=StatsConditionSerializer)
        result = stat.get_metrics(params, "daily_count_rt")
        return Response(result)

    @list_route(methods=["get"], url_path="top")
    def top(self, request):
        """
        @api {get} v3/storekit/stats/result_tables/top/ 获取topN rt统计
        @apiGroup Stats
        @apiDescription 获取最高数量的n条rt统计信息
        @apiParam {String} n 返回数据条数，可选参数，默认值10
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        bk_biz_id = request.query_params.get(BK_BIZ_ID, None)
        project_id = request.query_params.get(PROJECT_ID, None)
        component = request.query_params.get(COMPONENT, None)
        date = request.query_params.get(DATE, "")
        if not date.isdigit():
            date = util.get_date_by_diff(-1)
        # 转换为时间戳，便于sql查询
        date_ts = int(time.mktime(time.strptime(f"{date}", "%Y%m%d")))
        condition = f" WHERE {TIME}={date_ts}s"
        if bk_biz_id:
            condition += f" AND {BK_BIZ_ID}='{bk_biz_id}'"
        if project_id:
            condition += f" AND {PROJECT_ID}='{project_id}'"
        if component:
            condition += f" AND {COMPONENT}='{component}'"

        n_str = request.query_params.get("n", "")
        n = 10  # 默认10条数据限制
        if n_str.isdigit():
            n = int(n_str) if int(n_str) > 0 else 10

        sql = f"SELECT top(daily_count, {n}), * FROM daily_count_rt {condition}"
        metric = util.query_metrics(STAT_DATABASE, sql)
        return Response(metric[SERIES])
