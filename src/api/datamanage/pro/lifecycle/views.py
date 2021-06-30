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


import threading
from uuid import uuid4

from datamanage.pro import pizza_settings
from datamanage.pro.lifecycle.data_trace.data_set_create import get_dataset_create_info
from datamanage.pro.lifecycle.data_trace.data_trace_elasticsearch import (
    DataTraceElasticSearchClient,
)
from datamanage.pro.lifecycle.data_trace.data_trace_event import (
    get_data_trace_events_from_es,
    set_event_status_convg_mgr_events,
)
from datamanage.pro.lifecycle.metrics.cost import get_storage_info
from datamanage.pro.lifecycle.metrics.heat import get_heat_metric
from datamanage.pro.lifecycle.metrics.importance import (
    get_alert_info,
    get_data_count_info,
    get_importance_dict,
)
from datamanage.pro.lifecycle.metrics.range import get_range_metric
from datamanage.pro.lifecycle.metrics.range_bfs import get_range_bfs_metric
from datamanage.pro.lifecycle.metrics.trend import list_count, list_day_query_count
from datamanage.pro.lifecycle.serializers import (
    DataSetSerializer,
    DataTraceEventReportSerializer,
    LifeCycleTrendSerializer,
)
from datamanage.pro.lifecycle.tasks import data_value_inventory_task
from datamanage.pro.utils.time import get_date
from datamanage.utils.api.dataquery import DataqueryApi
from datamanage.utils.api.meta import MetaApi
from datamanage.utils.dbtools.influx_util import influx_query_by_geog_area
from django.core.cache import cache
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.exceptions import ValidationError
from common.log import logger
from common.views import APIViewSet

ES_USER = pizza_settings.ES_USER
DATA_TRACE_ES_API_HOST = pizza_settings.DATA_TRACE_ES_API_HOST
DATA_TRACE_ES_API_PORT = pizza_settings.DATA_TRACE_ES_API_PORT
DATA_TRACE_ES_TOKEN = pizza_settings.DATA_TRACE_ES_TOKEN
FORMAT_TOOLTIP = _("今日暂未统计")


class RangeView(APIViewSet):
    @list_route(methods=["get"], url_path="range_score")
    def range(self, request):
        """
        @api {get} /datamanage/lifecycle/range/range_score/ 获取单个数据集广度分数和相关指标
        @apiVersion 0.1.0
        @apiGroup Range
        @apiName range_score
        @apiParam {String} dataset_id rt_id/data_id
        @apiParam {String} dataset_type 数据集类型raw_data/result_table
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "node_count":[
                    7,
                    30,
                    12,
                    10,
                    3,
                    2,
                    1
                ],
                "proj_count":6,
                "depth":7,
                "biz_count":2,
                "range_score":55.88883508184201
            },
            "result":true
        }
        """
        # 直接用dgraph统计血缘中除去数据处理节点后的每一层的节点数，得到的分数未进行归一化
        dataset_id = request.query_params.get("dataset_id", "")
        dataset_type = self.params_valid(serializer=DataSetSerializer, params=request.query_params).get("dataset_type")
        if (not dataset_id) or (not dataset_type):
            raise ValidationError

        # 拿到单个数据集合的广度分数和相关指标，直接用dgraph统计血缘中除去数据处理节点后的每一层的节点数
        range_metric_dict = get_range_metric(dataset_id, dataset_type)
        return Response(range_metric_dict)

    @list_route(methods=["get"], url_path="range_score_bfs")
    def range_bfs(self, request):
        """
        @api {post} /datamanage/lifecycle/range/range_score_bfs/ 拿到单个数据集rt/raw_data广度指标
        @apiVersion 0.1.0
        @apiGroup Range
        @apiName range_bfs
        @apiParam {String} dataset_id rt_id/data_id
        @apiParam {String} dataset_type 数据集类型raw_data/result_table
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "app_code_count":0,
                "project_count":6,
                "biz_count":2,
                "normalized_range_score":78.57,
                "node_count":54,
                "range_score":49.57,
                "node_count_list":[
                    6,
                    34,
                    4,
                    8,
                    2
                ],
                "depth":5,
                "weighted_node_count":41.5718227247134
            },
            "result":true
        }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        dataset_type = self.params_valid(serializer=DataSetSerializer, params=request.query_params).get("dataset_type")
        if (not dataset_id) or (not dataset_type):
            raise ValidationError

        # 拿到单个数据集合的广度分数和相关指标，用广度优先搜索统计血缘中除去数据处理节点后的每一层的节点数
        range_metric_dict = get_range_bfs_metric(dataset_id, dataset_type)
        return Response(range_metric_dict)

    @list_route(methods=["get"], url_path="range_metric_by_influxdb")
    def range_metric_by_influxdb(self, request):
        dataset_id = request.query_params.getlist("dataset_id", None)
        if dataset_id is None:
            raise ValidationError

        # 从influx_db中查询广度指标和得分
        if isinstance(dataset_id, list) and len(dataset_id) > 1:
            query_condition = " or ".join(["dataset_id='%s'" % d for d in dataset_id])
        elif isinstance(dataset_id, list) and len(dataset_id) == 1:
            query_condition = "dataset_id='%s'" % dataset_id[0]
        sql = "SELECT * FROM range_metric where %s group by dataset_id order by time desc limit 1" % query_condition
        query_list = influx_query_by_geog_area(sql, db="monitor_custom_metrics", is_dict=True)

        # 参数错误导致查询结果为空
        if not query_list:
            raise ValidationError

        # 对广度得分保留两位小数
        for each_dataset in query_list:
            each_dataset["range_score"] = round(each_dataset["range_score"], 2)
        return Response(query_list)

    @list_route(methods=["get"], url_path="list_range_metric_by_influxdb")
    def list_range_metric_by_influxdb(self, request):
        """
        拿到广度评分和指标的变化趋势
        :param request:
        :return:
        """
        dataset_id = request.query_params.get("dataset_id", "")
        if dataset_id is None:
            raise ValidationError
        _params = self.params_valid(serializer=LifeCycleTrendSerializer, params=request.query_params)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]
        dataset_type = _params.get("dataset_type", "result_table")

        data = (
            list_count(
                "range",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
                frequency,
            )
            if frequency
            else list_count("range", dataset_id, start_timestamp, end_timestamp, dataset_type)
        )

        node_count_tooltip = None
        node_count_list = data.get("node_count", [])
        last_node_count = 0 if not node_count_list else node_count_list[-1]
        for each in node_count_list:
            if each and last_node_count == 0:
                node_count_tooltip = FORMAT_TOOLTIP
                break
        data["node_count_tooltip"] = node_count_tooltip

        return Response(data)


class RankingView(APIViewSet):
    # 获取数据排名
    @list_route(methods=["get"], url_path="range_heat_ranking_by_influxdb")
    def range_heat_ranking_by_influxdb(self, request):
        dataset_id = request.query_params.getlist("dataset_id", None)
        if dataset_id is None:
            raise ValidationError

        if isinstance(dataset_id, list) and len(dataset_id) > 1:
            query_condition = " or ".join(["dataset_id='%s'" % d for d in dataset_id])
        elif isinstance(dataset_id, list) and len(dataset_id) == 1:
            query_condition = "dataset_id='%s'" % dataset_id[0]
        sql = (
            "SELECT * FROM range_heat_ranking where %s group by dataset_id order by time desc limit 1" % query_condition
        )
        query_list = influx_query_by_geog_area(sql, db="monitor_custom_metrics", is_dict=True)
        # 参数错误导致查询结果为空，暂不raise异常，返回{}
        if not query_list:
            # raise ValidationError
            sql = """SELECT * FROM range_heat_ranking where heat_score=0 and range_score=13.1 order by time desc
                  limit %s""" % len(
                dataset_id
            )
            query_list = influx_query_by_geog_area(sql, db="monitor_custom_metrics", is_dict=True)
            if query_list:
                num = 0
                for each_id in dataset_id:
                    query_list[num]["dataset_id"] = each_id
                    num += 1
        return Response(query_list)


class HeatView(APIViewSet):
    # 获取数据热度以及数据被app的使用，统计app_code
    @list_route(methods=["get"], url_path="heat_score")
    def heat(self, request):
        dataset_id = request.query_params.get("dataset_id", "")
        dataset_type = request.query_params.get("dataset_type", "")
        agg_by_day = True if request.query_params.get("agg_by_day", "false") == "true" else False
        try:
            duration = request.query_params.get("duration", 7)
            duration = int(duration)
        except ValueError:
            logger.info("convert params duration:%s to int error" % duration)
            duration = 7
        if (not dataset_id) or (not dataset_type):
            raise ValidationError

        heat_metric_dict = get_heat_metric(dataset_id, dataset_type, duration, agg_by_day)
        return Response(heat_metric_dict)

    @list_route(methods=["get"], url_path="heat_metric_by_influxdb")
    def heat_metric_by_influxdb(self, request):
        """
        @api {post} /datamanage/lifecycle/heat/heat_metric_by_influxdb/ 通过influxdb获取热度得分和查询指标
        @apiVersion 0.1.0
        @apiGroup Heat
        @apiName heat_metric_by_influxdb
        @apiParam {List} dataset_id rt_id/data_id
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "count":null,
                    "dataset_type":"result_table",
                    "heat_score":1.74,
                    "timestamp":null,
                    "queue_service_count":0,
                    "app_code":null,
                    "query_count":34709,
                    "day_query_count":null,
                    "app_query_count":null,
                    "time":1574235004,
                    "app_code_1":null,
                    "dataset_id":"1068_system_net"
                }
            ],
            "result":true
        }
        """
        dataset_id = request.query_params.getlist("dataset_id", None)
        if dataset_id is None:
            raise ValidationError

        if isinstance(dataset_id, list) and len(dataset_id) > 1:
            query_condition = " or ".join(["dataset_id='%s'" % d for d in dataset_id])
        elif isinstance(dataset_id, list) and len(dataset_id) == 1:
            query_condition = "dataset_id='%s'" % dataset_id[0]
        sql = "SELECT * FROM heat_metrics where %s group by dataset_id order by time desc limit 1" % query_condition
        query_list = influx_query_by_geog_area(sql, db="monitor_custom_metrics", is_dict=True)
        # 参数错误导致查询结果为空
        if not query_list:
            raise ValidationError

        # 对热度得分保留两位小数
        for each_dataset in query_list:
            each_dataset["heat_score"] = round(each_dataset["heat_score"], 2)
        return Response(query_list)

    @list_route(methods=["get"], url_path="list_heat_metric_by_influxdb")
    def list_heat_metric_by_influxdb(self, request):
        """
        @api {post} /datamanage/lifecycle/heat/list_heat_metric_by_influxdb/ 拿到热度评分和指标的变化趋势
        @apiVersion 0.1.0
        @apiGroup Lifecycle/Heat
        @apiName asset_value_trend
        @apiParam {string} dataset_id rt_id/data_id
        @apiParam {string} dataset_type 数据集类型
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "message":"",
                "code":"00",
                "data":{
                    "day_query_count":[
                        0,
                        0,
                        0
                    ],
                    "timezone":"+0800",
                    "query_count":[
                        0,
                        0,
                        0
                    ],
                    "score":[
                        10,
                        0,
                        0
                    ],
                    "time":[
                        "06-17",
                        "06-18",
                        "06-19"
                    ]
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        if dataset_id is None:
            raise ValidationError
        _params = self.params_valid(serializer=LifeCycleTrendSerializer, params=request.query_params)
        dataset_type = _params.get("dataset_type", "result_table")

        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]

        data = (
            list_count(
                "heat",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
                frequency,
            )
            if frequency
            else list_count("heat", dataset_id, start_timestamp, end_timestamp, dataset_type)
        )
        day_query_count_dict = list_day_query_count(dataset_id, start_timestamp, end_timestamp)
        day_query_count_list = []
        num = 0
        for each_time in data["time"]:
            if each_time in list(day_query_count_dict.keys()):
                day_query_count_list.append(day_query_count_dict.get(each_time, 0))
            else:
                if data["query_count"][num] is None:
                    day_query_count_list.append(None)
                else:
                    day_query_count_list.append(0)
            num += 1
        data["day_query_count"] = day_query_count_list
        day_query_count_tooltip = None
        last_day_query_count = 0 if not day_query_count_list else day_query_count_list[-1]
        for each in day_query_count_list:
            if each and last_day_query_count == 0:
                day_query_count_tooltip = FORMAT_TOOLTIP
                break
        data["day_query_count_tooltip"] = day_query_count_tooltip
        return Response(data)


class HeatTaskView(APIViewSet):
    @list_route(methods=["get"], url_path="data_value_inventory_task")
    def data_value_inventory_task(self, request):
        t = threading.Thread(target=data_value_inventory_task, args=())
        t.start()
        return Response("data_value_inventory_task")


class AssetValueView(APIViewSet):
    @list_route(methods=["get"], url_path="trend/asset_value")
    def asset_value_trend(self, request):
        """
        @api {get} /datamanage/lifecycle/asset_value/trend/asset_value/ 价值评分变化趋势
        @apiVersion 0.1.0
        @apiGroup Lifecycle/AssetValue
        @apiName asset_value_trend
        @apiParam {string} dataset_id rt_id/data_id
        @apiParam {string} dataset_type 数据集类型
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "timezone":"+0800",
                    "score":[
                        null,
                        null,
                        null,
                        null,
                        null,
                        48.67,
                        48.67,
                        48.67
                    ],
                    "time":[
                        "05-26",
                        "05-27",
                        "05-28",
                        "05-29",
                        "05-30",
                        "05-31",
                        "06-01",
                        "06-02"
                    ]
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        if dataset_id is None:
            raise ValidationError
        _params = self.params_valid(serializer=LifeCycleTrendSerializer, params=request.query_params)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]
        dataset_type = _params.get("dataset_type", "result_table")

        data = (
            list_count(
                "asset_value",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
                frequency,
            )
            if frequency
            else list_count("asset_value", dataset_id, start_timestamp, end_timestamp, dataset_type)
        )
        return Response(data)

    @list_route(methods=["get"], url_path="trend/assetvalue_to_cost")
    def assetvalue_to_cost_trend(self, request):
        """
        @api {get} /datamanage/lifecycle/asset_value/trend/assetvalue_to_cost/ 价值成本比变化趋势
        @apiVersion 0.1.0
        @apiGroup Lifecycle/AssetValue
        @apiName assetvalue_to_cost_trend
        @apiParam {string} dataset_id rt_id/data_id
        @apiParam {string} dataset_type 数据集类型
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "timezone":"+0800",
                    "score":[
                        null,
                        null,
                        null,
                        null,
                        null,
                        -1,
                        -1,
                        -1
                    ],
                    "time":[
                        "05-27",
                        "05-28",
                        "05-29",
                        "05-30",
                        "05-31",
                        "06-01",
                        "06-02",
                        "06-03"
                    ]
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        if dataset_id is None:
            raise ValidationError
        _params = self.params_valid(serializer=LifeCycleTrendSerializer, params=request.query_params)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]
        dataset_type = _params.get("dataset_type", "result_table")

        data = (
            list_count(
                "assetvalue_to_cost",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
                frequency,
            )
            if frequency
            else list_count(
                "assetvalue_to_cost",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
            )
        )
        data["score"] = [None if each_score == -1 else each_score for each_score in data.get("score")]
        return Response(data)

    @list_route(methods=["get"], url_path="trend/importance")
    def importance_trend(self, request):
        """
        @api {get} /datamanage/lifecycle/asset_value/trend/importance/ 重要度评分变化趋势
        @apiVersion 0.1.0
        @apiGroup Lifecycle/AssetValue
        @apiName importance_trend
        @apiParam {string} dataset_id rt_id/data_id
        @apiParam {string} dataset_type 数据集类型
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "timezone":"+0800",
                    "score":[
                        null,
                        null,
                        null,
                        null,
                        41.67,
                        41.67,
                        41.67,
                        41.67
                    ],
                    "time":[
                        "05-27",
                        "05-28",
                        "05-29",
                        "05-30",
                        "05-31",
                        "06-01",
                        "06-02",
                        "06-03"
                    ]
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        if dataset_id is None:
            raise ValidationError
        _params = self.params_valid(serializer=LifeCycleTrendSerializer, params=request.query_params)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]
        dataset_type = _params.get("dataset_type", "result_table")

        data = (
            list_count(
                "importance",
                dataset_id,
                start_timestamp,
                end_timestamp,
                dataset_type,
                frequency,
            )
            if frequency
            else list_count("importance", dataset_id, start_timestamp, end_timestamp, dataset_type)
        )
        return Response(data)

    @list_route(methods=["get"], url_path="metric/asset_value")
    def asset_value_metric(self, request):
        """
        @api {get} /datamanage/lifecycle/asset_value/metric/asset_value/ 获取价值评分及其相关的热度、广度和重要度评分
        @apiVersion 0.1.0
        @apiGroup Lifecycle/AssetValue
        @apiName asset_value_metric
        @apiParam {string} dataset_id rt_id/data_id
        @apiParam {string} dataset_type 数据集类型

        @apiSuccess (输出) {String} data.heat_score_ranking 热度评分分组排名
        @apiSuccess (输出) {String} data.heat_score 热度评分
        @apiSuccess (输出) {String} data.asset_value_score_ranking 价值评分分组排名
        @apiSuccess (输出) {Boolean} data.asset_value_score 价值评分
        @apiSuccess (输出) {Boolean} data.importance_score_ranking 重要度评分分组排名
        @apiSuccess (输出) {Boolean} data.importance_score 重要度评分
        @apiSuccess (输出) {Boolean} data.assetvalue_to_cost_ranking 价值成本比分组排名
        @apiSuccess (输出) {Boolean} data.assetvalue_to_cost 价值成本比
        @apiSuccess (输出) {Boolean} data.range_score_ranking 广度评分排名
        @apiSuccess (输出) {Boolean} data.normalized_range_score 广度评分

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "heat_score_ranking": 0.9814,
                    "heat_score": 7.5,
                    "asset_value_score_ranking": 0.9967,
                    "assetvalue_to_cost": -1,
                    "importance_score_ranking": 0.9786,
                    "asset_value_score": 42.58,
                    "range_score_ranking": 0.9995,
                    "assetvalue_to_cost_ranking": 0.9997,
                    "normalized_range_score": 78.57,
                    "importance_score": 41.67
                },
                "result": true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        _params = self.params_valid(serializer=DataSetSerializer, params=request.query_params)
        dataset_type = _params.get("dataset_type", "result_table")

        if dataset_id is None:
            raise ValidationError
        asset_value_id = "{}_{}".format(dataset_id, dataset_type)
        statement = (
            """
        {
          asset_value(func:eq(AssetValue.id, "%s")) {
            AssetValue.asset_value_score
            AssetValue.asset_value_score_ranking
            AssetValue.importance_score
            AssetValue.heat_score
            AssetValue.normalized_range_score
            AssetValue.heat {
              Heat.heat_score_ranking
            }
            AssetValue.range {
              Range.range_score_ranking
            }
            AssetValue.importance {
              Importance.importance_score_ranking
            }
            ~LifeCycle.asset_value{
              LifeCycle.assetvalue_to_cost_ranking
              LifeCycle.assetvalue_to_cost
            }
          }
        }
        """
            % asset_value_id
        )
        search_dict = MetaApi.complex_search({"statement": statement, "backend_type": "dgraph"}).data
        tmp_dict = search_dict["data"]["asset_value"][0] if search_dict.get("data", {}).get("asset_value", []) else {}
        lifecycle_dict = tmp_dict["~LifeCycle.asset_value"][0] if tmp_dict.get("~LifeCycle.asset_value") else {}
        range_dict = tmp_dict["AssetValue.range"][0] if tmp_dict.get("AssetValue.range") else {}
        heat_dict = tmp_dict["AssetValue.heat"][0] if tmp_dict.get("AssetValue.heat") else {}
        importance_dict = tmp_dict["AssetValue.importance"][0] if tmp_dict.get("AssetValue.importance") else {}
        asset_value_dict = {
            "asset_value_score": tmp_dict.get("AssetValue.asset_value_score", 4.37),
            "normalized_range_score": tmp_dict.get("AssetValue.normalized_range_score", 13.1),
            "importance_score": tmp_dict.get("AssetValue.importance_score", 0),
            "heat_score": tmp_dict.get("AssetValue.heat_score", 0),
            "asset_value_score_ranking": tmp_dict.get("AssetValue.asset_value_score_ranking", 0),
            "assetvalue_to_cost": lifecycle_dict.get("LifeCycle.assetvalue_to_cost", -1),
            "assetvalue_to_cost_ranking": lifecycle_dict.get("LifeCycle.assetvalue_to_cost_ranking", 0),
            "heat_score_ranking": heat_dict.get("Heat.heat_score_ranking", 0),
            "range_score_ranking": range_dict.get("Range.range_score_ranking", 0),
            "importance_score_ranking": importance_dict.get("Importance.importance_score_ranking", 0),
        }
        return Response(asset_value_dict)

    @list_route(methods=["get"], url_path="metric/importance")
    def importance_metric(self, request):
        """
        @api {get} /datamanage/lifecycle/asset_value/metric/importance/ 获取重要度相关指标
        @apiVersion 0.1.0
        @apiGroup Lifecycle/AssetValue
        @apiName importance_metric
        @apiParam {String} dataset_id rt_id/data_id
        @apiParam {String} dataset_type  数据集类型
        @apiParam {String} bk_username  用户英文名

        @apiSuccess (输出) {String} data.oper_state_name 业务运营状态
        @apiSuccess (输出) {String} data.app_important_level_name 业务重要级别
        @apiSuccess (输出) {String} data.bip_grade_name 业务星级
        @apiSuccess (输出) {Boolean} data.active 项目运营状态：运营/删除
        @apiSuccess (输出) {Boolean} data.is_bip 是否关联BIP
        @apiSuccess (输出) {Boolean} data.has_alert 是否有告警
        @apiSuccess (输出) {Boolean} data.alert_count 最近1天告警数量
        @apiSuccess (输出) {Boolean} data.has_data 最近7天是否有数据
        @apiSuccess (输出) {Boolean} data.generate_type 数据生成类型

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "active":false,
                    "app_important_level_name":"普通",
                    "is_bip":false,
                    "oper_state_name":"公测",
                    "bip_grade_name":"",
                    "has_alert": true,
                    "has_data": true,
                    "generate_type": "user"
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        bk_username = request.query_params.get("bk_username", "")
        _params = self.params_valid(serializer=DataSetSerializer, params=request.query_params)
        dataset_type = _params.get("dataset_type", "result_table")
        if dataset_id is None:
            raise ValidationError

        # 数据重要度指标（业务指标、项目指标）
        importance_dict = get_importance_dict(dataset_id, dataset_type)

        if dataset_type == "result_table":
            importance_dict["generate_type"] = "user"

        # 判断数据是否有告警&有几条告警
        has_alert, alert_count = get_alert_info(dataset_id, bk_username)
        importance_dict["has_alert"] = has_alert
        importance_dict["alert_count"] = alert_count

        # 是否有数据
        has_data = get_data_count_info(dataset_id, dataset_type)
        importance_dict["has_data"] = has_data

        return Response(importance_dict)


class CostView(APIViewSet):
    @list_route(methods=["get"], url_path="metric/storage")
    def storage(self, request):
        """
        @api {get} /datamanage/lifecycle/cost/metric/storage/ 查看结果表存储总量和最近7天存储增量和数据增量
        @apiVersion 0.1.0
        @apiGroup Lifecycle/Cost
        @apiName storage
        @apiParam {string} result_table_id rt_id
        @apiSuccess (输出) {String} data.capacity 总占用存储,单位byte
        @apiSuccess (输出) {String} data.incre_count 最近7日日平均增量
        @apiSuccess (输出) {String} data.format_capacity 总占用存储,单位:自适应
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "tspider":{
                    "capacity":5302346964992,
                    "incre_count":3692809436,
                    "format_capacity": "7.125MB"
                }
            }
        """
        result_table_id = request.query_params.get("result_table_id", "")
        if result_table_id is None:
            raise ValidationError

        storages_dict, has_data = get_storage_info(result_table_id)
        return Response(storages_dict)


class DataApplicationView(APIViewSet):
    @list_route(methods=["get"], url_path="info")
    def data_application(self, request):
        """
        @api {get} /datamanage/lifecycle/data_application/info/ 数据集数据应用情况
        @apiVersion 0.1.0
        @apiGroup Lifecycle/DataApplication
        @apiName data_application
        @apiParam {String} dataset_id rt_id/data_id
        @apiParam {String} dataset_type  数据集类型
        @apiSuccess (输出) {String} data.count 当前app对该数据集的查询量（1日）
        @apiSuccess (输出) {String} data.app_code app
        @apiSuccess (输出) {String} data.applied_at 时间
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "query":[
                        {
                            "count":26720,
                            "app_code":"ieod_monitor"
                        }
                    ]
                },
                "result":true
            }
        """
        dataset_id = request.query_params.get("dataset_id", "")
        _params = self.params_valid(serializer=DataSetSerializer, params=request.query_params)
        dataset_type = _params.get("dataset_type", "result_table")
        if dataset_id is None:
            raise ValidationError

        if dataset_type == "raw_data":
            return Response({})

        # 获取前日日期，格式：'20200115'，用昨日日期的话，凌晨1点前若离线任务没有算完会导致热门查询没有数据
        date = get_date(1)
        applied_at = get_date(1, "-")

        sql = """SELECT count, app_code, result_table_id
                FROM 591_dataquery_application
                WHERE thedate={} and result_table_id='{}'
                GROUP BY result_table_id, app_code
                order by count desc limit 1000""".format(
            date, dataset_id
        )
        prefer_storage = "tspider"

        query_dict = DataqueryApi.query({"sql": sql, "prefer_storage": prefer_storage}).data
        if query_dict:
            query_list = query_dict.get("list", [])
            app_code_dict = cache.get("app_code_dict", {})
            for each in query_list:
                each["app_code_alias"] = app_code_dict.get(each["app_code"], None)
        else:
            return Response({"query": []})
        for each_query in query_list:
            each_query["applied_at"] = applied_at
        # 此处缺app_code的中文
        return Response({"query": query_list})


class DataTraceView(APIViewSet):
    @params_valid(serializer=DataSetSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/lifecycle/data_traces/ 数据足迹

        @apiVersion 3.5.0
        @apiGroup Lifecycle_DataTrace
        @apiName lifecycle_data_trace_list
        @apiDescription 数据足迹

        @apiParam {String} dataset_id rt_id/data_id
        @apiParam {String} dataset_type  数据集类型 result_table/raw_data/tdw_table

        @apiSuccess (返回) {Int} data.type 事件类型
        @apiSuccess (返回) {String} data.type_alias 事件别名
        @apiSuccess (返回) {String} data.sub_type 子事件类型
        @apiSuccess (返回) {String} data.sub_type_alias 子事件类型
        @apiSuccess (返回) {String} data.created_by 操作人
        @apiSuccess (返回) {String} data.created_at 时间
        @apiSuccess (返回) {String} data.status 数据迁移状态
        @apiSuccess (返回) {String} data.status_alias 数据迁移状态别名
        @apiSuccess (返回) {String} data.sub_events 数据迁移子事件列表
        @apiSuccess (返回) {String} data.sub_events.sub_type 数据迁移子事件类型
        @apiSuccess (返回) {String} data.sub_events.sub_type_alias 数据迁移子事件别名
        @apiSuccess (返回) {String} data.sub_events.created_by 数据迁移子事件操作人
        @apiSuccess (返回) {String} data.sub_events.created_at 数据迁移子事件时间
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":[
                    {
                        "description":"创建结果表",
                        "event_id":"event_id_1",
                        "created_at":"2020-12-14 00:01:30",
                        "created_by":"admin",
                        "details":{
                            "result_table_id":"test_rt_id"
                        },
                        "type_alias":"数据创建",
                        "sub_type_alias":"创建结果表",
                        "type":"create",
                        "sub_type":"create_result_table",
                        "action_id":"action_1"
                    },
                    {
                        "description":"关联任务XXX启动成功",
                        "event_id":"event_id_2",
                        "created_at":"2020-12-14 10:45:30",
                        "created_by":"admin",
                        "details":{
                            "flow_id":1,
                            "project_id":1,
                            "jump_to":""
                        },
                        "type_alias":"任务状态变化",
                        "sub_type_alias":"任务启动",
                        "type":"update_task_status",
                        "sub_type":"task_start",
                        "action_id":"action_2"
                    },
                    {
                        "status":"migrate_finish",
                        "created_at":"2020-12-14 11:22:03",
                        "type_alias":"数据迁移",
                        "sub_events":[
                            {
                                "description":"数据迁移开始",
                                "event_id":"event_id_3",
                                "created_at":"2020-12-14 11:22:03",
                                "created_by":"admin",
                                "details":{
                                    "jump_to":""
                                },
                                "type_alias":"数据迁移",
                                "sub_type_alias":"开始迁移",
                                "type":"migrate",
                                "sub_type":"migrate_start",
                                "action_id":"action_3"
                            },
                            {
                                "description":"数据迁移完成",
                                "event_id":"event_id_4",
                                "created_at":"2020-12-15 10:07:21",
                                "created_by":"admin",
                                "details":{
                                    "jump_to":""
                                },
                                "type_alias":"数据迁移",
                                "sub_type_alias":"完成迁移",
                                "type":"migrate",
                                "sub_type":"migrate_finish",
                                "action_id":"action_3"
                            }
                        ],
                        "created_by":"admin",
                        "status_alias":"已完成",
                        "type":"migrate"
                    }
                ],
                "result":true
            }
        """
        # 1) 数据创建事件
        create_trace_events = get_dataset_create_info(params["dataset_id"], params["dataset_type"])

        # 2)es中数据足迹事件
        es_data_trace_events = get_data_trace_events_from_es(params["dataset_id"])
        format_es_data_trace_events = set_event_status_convg_mgr_events(es_data_trace_events)

        # 3)对es中数据足迹事件按照时间排序（只对es中非创建的足迹事件排序，防止数据创建和其他事件created_at相同）
        format_es_data_trace_events.sort(key=lambda event: (event["datetime"]), reverse=False)
        return Response(create_trace_events + format_es_data_trace_events)

    @list_route(methods=["post"], url_path="event_report")
    @params_valid(serializer=DataTraceEventReportSerializer)
    def data_trace_event_report(self, request, params):
        """
        @api {post} /datamanage/lifecycle/data_traces/event_report/ 数据足迹事件上报

        @apiVersion 3.5.0
        @apiGroup Lifecycle_DataTrace
        @apiName lifecycle_data_trace_event_report
        @apiDescription 数据足迹事件上报

        @apiParam {List} contents 事件内容列表
        @apiParam {String} contents.opr_type 事件类型
        @apiParam {String} contents.opr_sub_type 子事件类型
        @apiParam {String} contents.dispatch_id 事件namespace+事件id(注意:在集市想对事件进行聚合后展示的话，dispatch_id要一致)
        @apiParam {String} contents.created_at 事件时间
        @apiParam {String} contents.created_by 用户名
        @apiParam {String} contents.data_set_id rt_id
        @apiParam {String} contents.description 事件描述
        @apiParam {Json} contents.opr_info 事件描述
        @apiParam {String} contents.opr_info.alias 事件中名文
        @apiParam {String} contents.opr_info.sub_type_alias 子事件中文名
        @apiParam {String='display', 'add_tips', 'group'} contents.opr_info.show_type 集市中展示方式
        @apiParam {String} contents.opr_info.desc_tpl 事件描述（带change_content参数）
        @apiParam {String} contents.opr_info.kv_tpl 参数
        @apiParam {List} contents.opr_info.desc_params 参数值
        @apiParam {Dict} contents.opr_info.jump_to 集市跳转详情

        @apiParamExample {json} 参数样例:
        {
            "contents": [
                {
                    "dispatch_id": "namespace-6fb25074-9dfb-4d9a-96b0-9581ea1671eb",
                    "opr_type": "update_task_status",
                    "opr_sub_type": "task_stop",
                    "data_set_id": "591_xxxxxxx",
                    "opr_info": "{
                        \"show_type\": \"add_tips\",
                        \"kv_tpl\": \"{flow_id}\",
                        \"alias\": \"\\u4efb\\u52a1\\u72b6\\u6001\\u53d8\\u5316\",
                         \"desc_params\": [{\"flow_id\": \"1\"}],
                         \"sub_type_alias\": \"flow\\u4efb\\u52a1\\u505c\\u6b62\",
                          \"desc_tpl\": \"\\u5173\\u8054flow\\u4efb\\u52a1 {change_content} \\u505c\\u6b62\",
                           \"jump_to\": {\"bk_biz_id\": 591, \"flow_id\": \"1\", \"jump_to\": \"flow\"}}",
                    "description": "\\u5173\\u8054flow\\u4efb\\u52a1 1294 \\u505c\\u6b62",
                    "created_at": "2021-04-10 15:43:21",
                    "created_by": "admin"
                }
            ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":true,
                "result":true
            }
        """
        # 1) get es config and es client
        es_client = DataTraceElasticSearchClient()

        # 2) report bulk data trace event to es
        event_list = []
        for event_dict in params["contents"]:
            # es对应的唯一id
            event_dict["id"] = str(uuid4())
            event_list.append(event_dict)
        es_client.bulk_data(event_list)
        return Response(True)
