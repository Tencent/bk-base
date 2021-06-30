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

from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.serializers.base import DataQualityDataSetSerializer
from datamanage.pro.dataquality.serializers.summary import DataQualitySummarySerializer
from datamanage.utils.api import MetaApi
from datamanage.utils.dbtools.influx_util import influx_query
from datamanage.utils.metric_configs import ProfilingWay
from datamanage.utils.num_tools import safe_float
from datamanage.utils.time_tools import timetostr
from rest_framework.response import Response

from common.decorators import list_route, params_valid, trace_gevent
from common.log import logger
from common.trace_patch import gevent_spawn_with_span
from common.views import APIViewSet


class DataqualitySummaryViewSet(BaseMixin, APIViewSet):
    @params_valid(serializer=DataQualityDataSetSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dataquality/summary/ 查询汇总数据质量指标

        @apiVersion 3.5.0
        @apiGroup DataQualitySummary
        @apiName dataquality_summary_metrics
        @apiDescription 查询数据质量汇总指标

        @apiParam {String} data_set_id 数据集

        @apiSuccess (200) {Number} data.alert_count 最近24小时告警数据量
        @apiSuccess (200) {Number} data.last_output_count 最近1分钟数据输出量
        @apiSuccess (200) {Number} data.avg_output_count 最近七天每天平均数据量
        @apiSuccess (200) {Number} data.last_data_time_delay 最近一分钟数据延时
        @apiSuccess (200) {Number} data.avg_data_time_delay 最近七天每分钟平均数据延时
        @apiSuccess (200) {Number} data.last_process_time_delay 最近一分钟处理延时
        @apiSuccess (200) {Number} data.avg_process_time_delay 最近七天每分钟平均处理延时
        @apiSuccess (200) {Number} data.last_loss_count 最近一分钟数据丢失条数
        @apiSuccess (200) {Number} data.last_loss_rate 最近一分钟数据丢失率
        @apiSuccess (200) {Number} data.avg_loss_count 最近七天每分钟平均丢失条数
        @apiSuccess (200) {Number} data.avg_loss_rate 最近七天每分钟平均丢失率
        @apiSuccess (200) {Number} data.last_drop_count 最近一分钟数据无效条数
        @apiSuccess (200) {Number} data.last_drop_rate 最近一分钟数据无效率
        @apiSuccess (200) {Number} data.avg_drop_count 最近七天每分钟平均无效条数
        @apiSuccess (200) {Number} data.avg_drop_rate 最近七条每分钟平均丢失率

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "alert_count": 10,
                    "last_output_count": 100,
                    "avg_output_count": 144000,
                    "last_data_time_delay": 60,
                    "avg_data_time_delay": 10,
                    "last_process_time_delay": 60,
                    "avg_process_time_delay": 10,
                    "last_loss_count": 10,
                    "last_loss_rate": 10.00,
                    "avg_loss_count": 9,
                    "avg_loss_rate": 9.00,
                    "last_drop_count": 10,
                    "last_drop_rate": 10.00,
                    "avg_drop_count": 11,
                    "avg_drop_rate": 11.00,
                }
            }
        """
        import gevent
        from gevent import monkey

        monkey.patch_all()

        data_set_id = params.get("data_set_id")
        response_content = {}

        gevent.joinall(
            [
                gevent.spawn(self.get_alert_count, data_set_id, response_content),
                gevent.spawn(self.get_last_output_count, data_set_id, response_content),
                gevent.spawn(self.get_avg_output_count, data_set_id, response_content),
                gevent.spawn(self.get_last_data_time_delay, data_set_id, response_content),
                gevent.spawn(self.get_avg_data_time_delay, data_set_id, response_content),
                gevent.spawn(self.get_last_process_time_delay, data_set_id, response_content),
                gevent.spawn(self.get_avg_process_time_delay, data_set_id, response_content),
                gevent.spawn(self.get_last_loss_count, data_set_id, response_content),
                gevent.spawn(self.get_last_loss_rate, data_set_id, response_content),
                gevent.spawn(self.get_avg_loss_count, data_set_id, response_content),
                gevent.spawn(self.get_avg_loss_rate, data_set_id, response_content),
                gevent.spawn(self.get_last_drop_count, data_set_id, response_content),
                gevent.spawn(self.get_last_drop_rate, data_set_id, response_content),
                gevent.spawn(self.get_avg_drop_count, data_set_id, response_content),
                gevent.spawn(self.get_avg_drop_rate, data_set_id, response_content),
            ]
        )

        return Response(response_content)

    @trace_gevent()
    def get_alert_count(self, data_set_id, response_content):
        results = self.get_metric_value(
            "alert_count",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-1d",
            end_time="now()",
            format="value",
        )
        if results and len(results) > 0:
            response_content["alert_count"] = results[0]["value"].get("alert_count", 0)
        else:
            response_content["alert_count"] = 0

    @trace_gevent()
    def get_last_output_count(self, data_set_id, response_content):
        results = self.get_metric_value(
            "output_count",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_output_count"] = self.get_metric_by_storage_orders(
                results, "output_count", ["kafka", "hdfs"]
            )
        else:
            response_content["last_output_count"] = 0

    @trace_gevent()
    def get_avg_output_count(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "output_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "output_count", ["kafka", "hdfs"]),
                )
            )
            response_content["avg_output_count"] = sum(metrics) // 7
        else:
            response_content["avg_output_count"] = 0

    @trace_gevent()
    def get_last_data_time_delay(self, data_set_id, response_content):
        results = self.get_metric_value(
            "data_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_data_time_delay"] = self.get_metric_by_storage_orders(
                results, "data_time_delay", ["kafka", "hdfs"]
            )
        else:
            response_content["last_data_time_delay"] = 0

    @trace_gevent()
    def get_avg_data_time_delay(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "data_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            aggregation="mean",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "data_time_delay", ["kafka", "hdfs"]),
                )
            )
            response_content["avg_data_time_delay"] = sum(metrics) // 7
        else:
            response_content["avg_data_time_delay"] = 0

    @trace_gevent()
    def get_last_process_time_delay(self, data_set_id, response_content):
        results = self.get_metric_value(
            "process_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_process_time_delay"] = self.get_metric_by_storage_orders(
                results, "process_time_delay", ["kafka", "hdfs"]
            )
        else:
            response_content["last_process_time_delay"] = 0

    @trace_gevent()
    def get_avg_process_time_delay(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "process_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            aggregation="mean",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "process_time_delay", ["kafka", "hdfs"]),
                )
            )
            response_content["avg_process_time_delay"] = sum(metrics) // 7
        else:
            response_content["avg_process_time_delay"] = 0

    @trace_gevent()
    def get_last_loss_count(self, data_set_id, response_content):
        results = self.get_metric_value(
            "loss_count",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_loss_count"] = self.get_metric_by_storage_orders(
                results, "loss_count", ["kafka", "hdfs"]
            )
            if response_content["last_loss_count"] < 0:
                response_content["last_loss_count"] = 0
        else:
            response_content["last_loss_count"] = 0

    @trace_gevent()
    def get_last_loss_rate(self, data_set_id, response_content):
        results = self.get_metric_value(
            "loss_rate",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_loss_rate"] = self.get_metric_by_storage_orders(results, "loss_rate", ["kafka"])
            if response_content["last_loss_rate"] < 0:
                response_content["last_loss_rate"] = 0
        else:
            response_content["last_loss_rate"] = 0

    @trace_gevent()
    def get_avg_loss_count(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "loss_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "loss_count", ["kafka"]),
                )
            )
            response_content["avg_loss_count"] = sum(metrics) // 7
            if response_content["avg_loss_count"] < 0:
                response_content["avg_loss_count"] = 0
        else:
            response_content["avg_loss_count"] = 0

    @trace_gevent()
    def get_avg_loss_rate(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "loss_rate",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            aggregation="mean",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "loss_rate", ["kafka"]),
                )
            )
            response_content["avg_loss_rate"] = sum(metrics) // 7
            if response_content["avg_loss_rate"] < 0:
                response_content["avg_loss_rate"] = 0
        else:
            response_content["avg_loss_rate"] = 0

    @trace_gevent()
    def get_last_drop_count(self, data_set_id, response_content):
        results = self.get_metric_value(
            "drop_count",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_drop_count"] = self.get_metric_by_storage_orders(
                results, "drop_count", ["kafka", "hdfs"]
            )
            if response_content["last_drop_count"] < 0:
                response_content["last_drop_count"] = 0
        else:
            response_content["last_drop_count"] = 0

    @trace_gevent()
    def get_last_drop_rate(self, data_set_id, response_content):
        results = self.get_metric_value(
            "drop_rate",
            data_set_id=data_set_id,
            storage=None,
            start_time="now()-6m",
            end_time="now()-5m",
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            response_content["last_drop_rate"] = self.get_metric_by_storage_orders(
                results, "drop_rate", ["kafka", "hdfs"]
            )
            if response_content["last_drop_rate"] < 0:
                response_content["last_drop_rate"] = 0
        else:
            response_content["last_drop_rate"] = 0

    @trace_gevent()
    def get_avg_drop_count(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "drop_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "drop_count", ["kafka", "hdfs"]),
                )
            )
            response_content["avg_drop_count"] = sum(metrics) // 7
            if response_content["avg_drop_count"] < 0:
                response_content["avg_drop_count"] = 0
        else:
            response_content["avg_drop_count"] = 0

    @trace_gevent()
    def get_avg_drop_rate(self, data_set_id, response_content):
        start_time = "{}s".format(int(self.get_start_of_the_day(delta=7)))
        end_time = "{}s".format(int(self.get_start_of_the_day(delta=0)))
        results = self.get_metric_value(
            "drop_rate",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="series",
            aggregation="mean",
            tags=["storage_cluster_type"],
            time_grain="1d",
        )
        if results and len(results) > 0:
            metrics = list(
                map(
                    safe_float,
                    self.get_metric_series_by_storage_orders(results, "drop_rate", ["kafka", "hdfs"]),
                )
            )
            response_content["avg_drop_rate"] = sum(metrics) // 7
            if response_content["avg_drop_rate"] < 0:
                response_content["avg_drop_rate"] = 0
        else:
            response_content["avg_drop_rate"] = 0

    @list_route(methods=["get"], url_path=r"(?P<measurement>\w+)")
    @params_valid(serializer=DataQualitySummarySerializer)
    def metrics_measurement(self, request, measurement, params):
        """
        @api {get} /datamanage/dataquality/summary/:measurement/ 查询数据质量度量汇总指标

        @apiVersion 3.5.0
        @apiGroup DataQualitySummary
        @apiName dataquality_measurement_summary_metrics
        @apiDescription 查询数据质量度量汇总指标
        目前支持input_count, output_count, data_time_delay, processing_time_delay，
        loss_count, loss_rate, drop_count, drop_rate, profiling

        @apiParam {String} measurement 数据质量指标
        @apiParam {String} data_set_id 数据集
        @apiParam {String} [field] 字段
        @apiParam {Boolean} [with_current] 是否获取当前值
        @apiParam {Boolean} [with_whole] 是否获取全量数据

        @apiSuccess (output_count) {Number} data.today_sum_output_count 今日输出数据量
        @apiSuccess (output_count) {Number} data.last_day_sum_output_count 昨日同时刻输出数据量
        @apiSuccess (output_count) {Number} data.last_week_sum_output_count 上周同时刻输出数据量
        @apiSuccess (output_count) {Number} data.max_output_count_same_period 最近7天同时段最大输出数据量
        @apiSuccess (output_count) {Number} data.min_output_count_same_period 最近7天同时段最小输出数据量
        @apiSuccess (output_count) {Number} data.avg_output_count_same_period 最近7天同时段平均输出数据量
        @apiSuccess (output_count) {Number} data.today_output_count_current 今日当前输出数据量
        @apiSuccess (output_count) {Number} data.last_day_output_count_current 昨日当前输出数据量
        @apiSuccess (output_count) {Number} data.last_week_output_count_current 上周当前输出数据量
        @apiSuccess (output_count) {Number} data.last_day_output_count_whole_day 昨日全天输出数据量
        @apiSuccess (output_count) {Number} data.last_week_output_count_whole_day 上周全天输出数据量
        @apiSuccess (output_count) {Number} data.max_output_count_whole_day 最近7天天最大输出数据量
        @apiSuccess (output_count) {Number} data.min_output_count_whole_day 最近7天天最小输出数据量
        @apiSuccess (output_count) {Number} data.avg_output_count_whole_day 最近7天天平均输出数据量

        @apiSuccess (data_time_delay) {Number} data.today_avg_data_time_delay 今日平均数据延时
        @apiSuccess (data_time_delay) {Number} data.last_day_avg_data_time_delay 昨日同时刻平均数据延时
        @apiSuccess (data_time_delay) {Number} data.last_week_avg_data_time_delay 上周同时刻平均数据延时
        @apiSuccess (data_time_delay) {Number} data.max_data_time_delay_same_period 最近7天同时段最大平均数据延时
        @apiSuccess (data_time_delay) {Number} data.min_data_time_delay_same_period 最近7天同时段最小平均数据延时
        @apiSuccess (data_time_delay) {Number} data.avg_data_time_delay_same_period 最近7天同时段平均平均数据延时
        @apiSuccess (data_time_delay) {Number} data.today_data_time_delay_current 今日当前数据延迟
        @apiSuccess (data_time_delay) {Number} data.last_day_data_time_delay_current 昨日当前数据延迟
        @apiSuccess (data_time_delay) {Number} data.last_week_data_time_delay_current 上周当前数据延迟
        @apiSuccess (data_time_delay) {Number} data.last_day_data_time_delay_whole_day 昨日全天数据延迟
        @apiSuccess (data_time_delay) {Number} data.last_week_data_time_delay_whole_day 上周全天数据延迟
        @apiSuccess (data_time_delay) {Number} data.max_data_time_delay_whole_day 最近7天天最大平均数据延时
        @apiSuccess (data_time_delay) {Number} data.min_data_time_delay_whole_day 最近7天天最小平均数据延时
        @apiSuccess (data_time_delay) {Number} data.avg_data_time_delay_whole_day 最近7天天平均平均数据延时

        @apiSuccess (process_time_delay) {Number} data.today_avg_process_time_delay 今日平均处理延时
        @apiSuccess (process_time_delay) {Number} data.last_day_avg_process_time_delay 昨日同时刻平均处理延时
        @apiSuccess (process_time_delay) {Number} data.last_week_avg_process_time_delay 上周同时刻平均处理延时
        @apiSuccess (process_time_delay) {Number} data.max_process_time_delay_same_period 最近7天同时段最大平均处理延时
        @apiSuccess (process_time_delay) {Number} data.min_process_time_delay_same_period 最近7天同时段最小平均处理延时
        @apiSuccess (process_time_delay) {Number} data.avg_process_time_delay_same_period 最近7天同时段平均平均处理延时
        @apiSuccess (process_time_delay) {Number} data.today_process_time_delay_current 今日当前处理延迟
        @apiSuccess (process_time_delay) {Number} data.last_day_process_time_delay_current 昨日当前处理延迟
        @apiSuccess (process_time_delay) {Number} data.last_week_process_time_delay_current 上周当前处理延迟
        @apiSuccess (process_time_delay) {Number} data.last_day_process_time_delay_whole_day 昨日全天处理延迟
        @apiSuccess (process_time_delay) {Number} data.last_week_process_time_delay_whole_day 上周全天处理延迟
        @apiSuccess (process_time_delay) {Number} data.max_process_time_delay_whole_day 最近7天天最大平均处理延时
        @apiSuccess (process_time_delay) {Number} data.min_process_time_delay_whole_day 最近7天天最小平均处理延时
        @apiSuccess (process_time_delay) {Number} data.avg_process_time_delay_whole_day 最近7天天平均平均处理延时

        @apiSuccess (loss_count) {Number} data.today_avg_loss_count 今日平均丢失条数
        @apiSuccess (loss_count) {Number} data.last_day_avg_loss_count 昨日同时刻平均丢失条数
        @apiSuccess (loss_count) {Number} data.last_week_avg_loss_count 上周同时刻平均丢失条数
        @apiSuccess (loss_count) {Number} data.max_loss_count_same_period 最近7天同时段最大平均丢失条数
        @apiSuccess (loss_count) {Number} data.min_loss_count_same_period 最近7天同时段最小平均丢失条数
        @apiSuccess (loss_count) {Number} data.avg_loss_count_same_period 最近7天同时段平均平均丢失条数
        @apiSuccess (loss_count) {Number} data.today_loss_count_current 今日当前丢失条数
        @apiSuccess (loss_count) {Number} data.last_day_loss_count_current 昨日当前丢失条数
        @apiSuccess (loss_count) {Number} data.last_week_loss_count_current 上周当前丢失条数
        @apiSuccess (loss_count) {Number} data.last_day_loss_count_whole_day 昨日全天丢失条数
        @apiSuccess (loss_count) {Number} data.last_week_loss_count_whole_day 上周全天丢失条数
        @apiSuccess (loss_count) {Number} data.max_loss_count_whole_day 最近7天天最大平均丢失条数
        @apiSuccess (loss_count) {Number} data.min_loss_count_whole_day 最近7天天最小平均丢失条数
        @apiSuccess (loss_count) {Number} data.avg_loss_count_whole_day 最近7天天平均平均丢失条数

        @apiSuccess (drop_count) {Number} data.today_avg_drop_count 今日平均无效数据条数
        @apiSuccess (drop_count) {Number} data.last_day_avg_drop_count 昨日同时刻平均无效数据条数
        @apiSuccess (drop_count) {Number} data.last_week_avg_drop_count 上周同时刻平均无效数据条数
        @apiSuccess (drop_count) {Number} data.max_drop_count_same_period 最近7天同时段最大平均无效数据条数
        @apiSuccess (drop_count) {Number} data.min_drop_count_same_period 最近7天同时段最小平均无效数据条数
        @apiSuccess (drop_count) {Number} data.avg_drop_count_same_period 最近7天同时段平均平均无效数据条数
        @apiSuccess (drop_count) {Number} data.today_drop_count_current 今日当前无效数据条数
        @apiSuccess (drop_count) {Number} data.last_day_drop_count_current 昨日当前无效数据条数
        @apiSuccess (drop_count) {Number} data.last_week_drop_count_current 上周当前无效数据条数
        @apiSuccess (drop_count) {Number} data.last_day_drop_count_whole_day 昨日全天无效数据条数
        @apiSuccess (drop_count) {Number} data.last_week_drop_count_whole_day 上周全天无效数据条数
        @apiSuccess (drop_count) {Number} data.max_drop_count_whole_day 最近7天天最大平均无效数据条数
        @apiSuccess (drop_count) {Number} data.min_drop_count_whole_day 最近7天天最小平均无效数据条数
        @apiSuccess (drop_count) {Number} data.avg_drop_count_whole_day 最近7天天平均平均无效数据条数

        @apiSuccess (profiling) {Number} data.total_count 剖析数据条数
        @apiSuccess (profiling) {String} data.profiling_way 剖析方式，可选complete, custom, sampling
        @apiSuccess (profiling) {String} data.start_time 剖析数据开始时间
        @apiSuccess (profiling) {String} data.end_time 剖析数据结束时间
        @apiSuccess (profiling) {Object} data.fields 字段信息
        @apiSuccess (profiling) {String} data.fields.field_type 字段类型
        @apiSuccess (profiling) {Number} data.fields.mean 平均值
        @apiSuccess (profiling) {Number} data.fields.max 最大值
        @apiSuccess (profiling) {Number} data.fields.min 最小值
        @apiSuccess (profiling) {Number} data.fields.range 极差
        @apiSuccess (profiling) {Number} data.fields.quantile25 四分之一位数
        @apiSuccess (profiling) {Number} data.fields.median 中位数
        @apiSuccess (profiling) {Number} data.fields.quantile75 四分之三位数
        @apiSuccess (profiling) {Number} data.fields.modes 众数
        @apiSuccess (profiling) {Number} data.fields.std 标准差
        @apiSuccess (profiling) {Number} data.fields.var 方差
        @apiSuccess (profiling) {Number} data.fields.skew 偏度
        @apiSuccess (profiling) {Number} data.fields.kurt 峰度
        @apiSuccess (profiling) {Number} data.fields.unique_count 唯一值个数
        @apiSuccess (profiling) {Number} data.fields.unique_rate 唯一值率
        @apiSuccess (profiling) {Number} data.fields.null_count 空值个数
        @apiSuccess (profiling) {Number} data.fields.null_rate 空值率
        @apiSuccess (profiling) {JSON} data.fields.distribution 基数分布统计值
        @apiSuccess (profiling) {JSON} data.fields.distribution_hist 数据分布统计值

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "today_sum_output_count": 100000,
                    "last_day_sum_output_count": 150000,
                    "last_week_sum_output_count": 140000,
                    "max_output_count_whole_day": 110000,
                    "min_output_count_whole_day": 90000,
                    "avg_output_count_whole_day": 98000
                }
            }
        """
        data_set_id = params.get("data_set_id")
        response_content = {}
        with_whole = params.get("with_whole")
        with_current = params.get("with_current")

        if measurement == "output_count":
            self.get_summary_metric_same_period(
                "output_count",
                self.fetch_output_count,
                data_set_id,
                response_content,
            )
            if with_whole:
                self.get_summary_metric_whole_day(
                    "output_count",
                    self.fetch_output_count,
                    data_set_id,
                    response_content,
                )
            if with_current:
                self.get_summary_metric_current(
                    "output_count",
                    self.fetch_output_count,
                    data_set_id,
                    response_content,
                )
        elif measurement == "data_time_delay":
            self.get_summary_metric_same_period(
                "data_time_delay",
                self.fetch_data_time_delay,
                data_set_id,
                response_content,
            )
            if with_whole:
                self.get_summary_metric_whole_day(
                    "data_time_delay",
                    self.fetch_data_time_delay,
                    data_set_id,
                    response_content,
                )
            if with_current:
                self.get_summary_metric_current(
                    "data_time_delay",
                    self.fetch_data_time_delay,
                    data_set_id,
                    response_content,
                )
        elif measurement == "process_time_delay":
            self.get_summary_metric_same_period(
                "process_time_delay",
                self.fetch_process_time_delay,
                data_set_id,
                response_content,
            )
            if with_whole:
                self.get_summary_metric_whole_day(
                    "process_time_delay",
                    self.fetch_process_time_delay,
                    data_set_id,
                    response_content,
                )
            if with_current:
                self.get_summary_metric_current(
                    "process_time_delay",
                    self.fetch_process_time_delay,
                    data_set_id,
                    response_content,
                )
        elif measurement == "loss_count":
            self.get_summary_metric_same_period(
                "loss_count",
                self.fetch_loss_count,
                data_set_id,
                response_content,
            )
            if with_whole:
                self.get_summary_metric_whole_day(
                    "loss_count",
                    self.fetch_loss_count,
                    data_set_id,
                    response_content,
                )
            if with_current:
                self.get_summary_metric_current(
                    "loss_count",
                    self.fetch_loss_count,
                    data_set_id,
                    response_content,
                )
        elif measurement == "drop_count":
            self.get_summary_metric_same_period(
                "drop_count",
                self.fetch_drop_count,
                data_set_id,
                response_content,
            )
            if with_whole:
                self.get_summary_metric_whole_day(
                    "drop_count",
                    self.fetch_drop_count,
                    data_set_id,
                    response_content,
                )
            if with_current:
                self.get_summary_metric_current(
                    "drop_count",
                    self.fetch_drop_count,
                    data_set_id,
                    response_content,
                )
        elif measurement == "profiling":
            field = params.get("field")
            self.fetch_last_profiling_result(data_set_id, field, response_content)
        return Response(response_content)

    def get_summary_metric_same_period(self, metric_name, fetch_func, data_set_id, response_content):
        import gevent
        from gevent import monkey

        monkey.patch_all()

        tmp_response = {}
        tasks = []
        for day_shift in range(8):
            tasks.append(gevent_spawn_with_span(fetch_func, day_shift, data_set_id, tmp_response))
        gevent.joinall(tasks)

        response_content["today_sum_{}".format(metric_name)] = tmp_response.pop(0)
        response_content["last_day_sum_{}".format(metric_name)] = tmp_response[1]
        response_content["last_week_sum_{}".format(metric_name)] = tmp_response[7]
        response_content["max_{}_same_period".format(metric_name)] = max(tmp_response.values())
        response_content["min_{}_same_period".format(metric_name)] = min(tmp_response.values())
        response_content["avg_{}_same_period".format(metric_name)] = sum(tmp_response.values()) // 7

    def get_summary_metric_whole_day(self, metric_name, fetch_func, data_set_id, response_content):
        import gevent
        from gevent import monkey

        monkey.patch_all()

        tmp_response = {}
        tasks = []
        for day_shift in range(1, 8):
            tasks.append(gevent_spawn_with_span(fetch_func, day_shift, data_set_id, tmp_response, "whole"))
        gevent.joinall(tasks)

        response_content["last_day_{}_whole_day".format(metric_name)] = tmp_response[1]
        response_content["last_week_{}_whole_day".format(metric_name)] = tmp_response[7]
        response_content["max_{}_whole_day".format(metric_name)] = max(tmp_response.values())
        response_content["min_{}_whole_day".format(metric_name)] = min(tmp_response.values())
        response_content["avg_{}_whole_day".format(metric_name)] = sum(tmp_response.values()) // 7

    def get_summary_metric_current(self, metric_name, fetch_func, data_set_id, response_content):
        import gevent
        from gevent import monkey

        monkey.patch_all()

        tmp_response = {}
        tasks = []
        for day_shift in [0, 1, 7]:
            tasks.append(gevent_spawn_with_span(fetch_func, day_shift, data_set_id, tmp_response, "current"))
        gevent.joinall(tasks)

        response_content["today_{}_current".format(metric_name)] = tmp_response.pop(0)
        response_content["last_day_{}_current".format(metric_name)] = tmp_response[1]
        response_content["last_week_{}_current".format(metric_name)] = tmp_response[7]

    def get_start_and_end_time(self, day_shift, range):
        if range == "current":
            start_time = "now()-{}d-6m".format(day_shift) if day_shift > 0 else "now()-6m"
            end_time = "now()-{}d-5m".format(day_shift) if day_shift > 0 else "now()-5m"
            return start_time, end_time

        start_time = "{}s".format(int(self.get_start_of_the_day(delta=day_shift)))
        if range == "whole":
            end_time = "{}s".format(int(self.get_start_of_the_day(delta=day_shift - 1)))
        else:
            end_time = "now()-{}d".format(day_shift) if day_shift > 0 else "now()"
        return start_time, end_time

    @trace_gevent()
    def fetch_output_count(self, day_shift, data_set_id, tmp_response, range=None):
        start_time, end_time = self.get_start_and_end_time(day_shift, range)
        results = self.get_metric_value(
            "output_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            tmp_response[day_shift] = self.get_metric_by_storage_orders(results, "output_count", ["kafka", "hdfs"])
        else:
            tmp_response[day_shift] = 0

    @trace_gevent()
    def fetch_data_time_delay(self, day_shift, data_set_id, tmp_response, same_period=False):
        start_time, end_time = self.get_start_and_end_time(day_shift, same_period)
        results = self.get_metric_value(
            "data_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="value",
            aggregation="mean",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            tmp_response[day_shift] = self.get_metric_by_storage_orders(results, "data_time_delay", ["kafka", "hdfs"])
        else:
            tmp_response[day_shift] = 0

    @trace_gevent()
    def fetch_process_time_delay(self, day_shift, data_set_id, tmp_response, same_period=False):
        start_time, end_time = self.get_start_and_end_time(day_shift, same_period)
        results = self.get_metric_value(
            "process_time_delay",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="value",
            aggregation="mean",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            tmp_response[day_shift] = self.get_metric_by_storage_orders(
                results, "process_time_delay", ["kafka", "hdfs"]
            )
        else:
            tmp_response[day_shift] = 0

    @trace_gevent()
    def fetch_loss_count(self, day_shift, data_set_id, tmp_response, same_period=False):
        start_time, end_time = self.get_start_and_end_time(day_shift, same_period)
        results = self.get_metric_value(
            "loss_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            tmp_response[day_shift] = self.get_metric_by_storage_orders(results, "loss_count", ["kafka"])
            if tmp_response[day_shift] < 0:
                tmp_response[day_shift] = 0
        else:
            tmp_response[day_shift] = 0

    @trace_gevent()
    def fetch_drop_count(self, day_shift, data_set_id, tmp_response, same_period=False):
        start_time, end_time = self.get_start_and_end_time(day_shift, same_period)
        results = self.get_metric_value(
            "drop_count",
            data_set_id=data_set_id,
            storage=None,
            start_time=start_time,
            end_time=end_time,
            format="value",
            tags=["storage_cluster_type"],
        )
        if results and len(results) > 0:
            tmp_response[day_shift] = self.get_metric_by_storage_orders(results, "drop_count", ["kafka"])
            if tmp_response[day_shift] < 0:
                tmp_response[day_shift] = 0
        else:
            tmp_response[day_shift] = 0

    def fetch_last_profiling_result(self, data_set_id, field, response_content):
        response_content.update(
            {
                "total_count": 0,
                "profiling_way": None,
                "start_time": None,
                "end_time": None,
                "fields": {},
            }
        )
        fields = {}
        try:
            resp = MetaApi.result_tables.fields({"result_table_id": data_set_id}).data
            for item in resp:
                fields[item.get("field_name")] = item
        except Exception as e:
            logger.error("Failed to get fields about result_table({}), error: {}".format(data_set_id, e))

        sql = """
            SELECT LAST(*) from data_set_field_profiling
            WHERE time > now() - 1d AND data_set_id = '{}' GROUP BY "field", profiling_way
        """.format(
            data_set_id
        )
        results = influx_query(sql, is_dict=True)
        for item in results:
            if not item.get("last_timestamp"):
                continue
            if not response_content["total_count"]:
                response_content["total_count"] = item.get("last_total_count")
            if not response_content["start_time"]:
                timestamp = int(item.get("last_timestamp") or item.get("time"))
                response_content["start_time"] = timetostr(timestamp)
                response_content["end_time"] = timetostr(timestamp + 60)

            profiling_way = item.get("profiling_way")
            field = item.get("field")
            if profiling_way not in response_content["fields"]:
                response_content["fields"][profiling_way] = {}

            max_val = safe_float(item.get("last_max"), None)
            min_val = safe_float(item.get("last_min"), None)
            response_content["fields"][profiling_way][field] = {
                "field_type": fields.get(field, {}).get("field_type"),
                "mean": safe_float(item.get("last_mean"), None),
                "max": max_val,
                "min": min_val,
                "range": max_val - min_val if max_val is not None and min_val is not None else None,
                "quantile25": safe_float(item.get("last_quantile25"), None),
                "median": safe_float(item.get("last_median"), None),
                "quantile75": safe_float(item.get("last_quantile75"), None),
                "modes": item.get("last_modes"),
                "std": safe_float(item.get("last_std"), None),
                "var": safe_float(item.get("last_var"), None),
                "skew": safe_float(item.get("last_skew"), None),
                "kurt": safe_float(item.get("last_kurt"), None),
                "unique_count": item.get("last_unique_count"),
                "unique_rate": item.get("last_unique_rate"),
                "null_count": item.get("last_null_count"),
                "null_rate": item.get("last_null_rate"),
                "distribution": json.loads(item.get("last_distribution") or "{}"),
                "distribution_hist": json.loads(item.get("last_distribution_hist") or "{}"),
            }

        for profiling_way in ProfilingWay.ALL_TYPES:
            if profiling_way in response_content["fields"] and response_content["fields"][profiling_way]:
                response_content["fields"] = response_content["fields"][profiling_way]
                response_content["profiling_way"] = profiling_way
                break
