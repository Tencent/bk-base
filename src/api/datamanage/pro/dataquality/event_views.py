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
import time

from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.models.audit import (
    DataQualityAuditRuleEvent,
    DataQualityEventTemplateVariable,
    EventTypeConfig,
)
from datamanage.pro.dataquality.serializers.base import DataQualityDataSetSerializer
from datamanage.pro.dataquality.serializers.event import (
    DataQualityEventListSerializer,
    DataQualityEventTemplateVariableSerializer,
    EventTypeConfigSerializer,
)
from datamanage.utils.dbtools.influx_util import influx_query
from datamanage.utils.drf import DataPageNumberPagination
from datamanage.utils.time_tools import timetostr
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIModelViewSet, APIViewSet


class DataQualityEventViewSet(BaseMixin, APIViewSet):
    @params_valid(serializer=DataQualityEventListSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dataquality/events/ 查询数据集事件列表

        @apiVersion 3.5.0
        @apiGroup DataQualityEvent
        @apiName dataquality_event_list
        @apiDescription 查询数据集事件列表

        @apiParam {String} data_set_id 数据集
        @apiParam {Number} start_time 开始时间（10位时间戳）
        @apiParam {Number} end_time 结束时间（10位时间戳）

        @apiSuccess (200) {Number} data.event_id 事件ID
        @apiSuccess (200) {String} data.event_alias 事件别名
        @apiSuccess (200) {String} data.description 描述
        @apiSuccess (200) {Number} data.event_currency 事件时效性
        @apiSuccess (200) {String} data.event_type 事件类型
        @apiSuccess (200) {String} data.event_sub_type 事件子类型
        @apiSuccess (200) {String} data.event_polarity 事件极性
        @apiSuccess (200) {String} data.sensitivity 事件敏感度
        @apiSuccess (200) {Object} data.rule 事件关联规则
        @apiSuccess (200) {String} data.rule.rule_name 规则名称
        @apiSuccess (200) {String} data.rule.rule_config_alias 规则配置别名
        @apiSuccess (200) {List} data.event_instances 事件实例列表
        @apiSuccess (200) {String} data.event_instances.event_time 事件发生时间
        @apiSuccess (200) {String} data.event_instances.event_status_alias 审核指标状态
        @apiSuccess (200) {String} data.event_instances.event_detail 事件详情信息
        @apiSuccess (200) {Boolean} data.in_effect 事件是否生效中（是否告警）
        @apiSuccess (200) {List} data.notify_ways 事件通知方式
        @apiSuccess (200) {String} data.receivers 事件通知接收人
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "event_id": "custom_event1",
                        "event_alias": "数据空值是否超过阈值",
                        "description": "空值率较高",
                        "event_currency": 30,
                        "event_type": "data_quality",
                        "event_sub_type": "profiling_metric_out_of_range",
                        "event_polarity": "negative",
                        "sensitivity": "private",
                        "rule": {
                            "rule_id": 1,
                            "rule_name": "范围约束",
                            "rule_config_alias": "(空值率 > 30%) & (空值个数 > 1000)"
                        },
                        "event_instances": [
                            {
                                "event_time": "2020-07-27 00:00:00",
                                "event_status_alias": "(F1空值率 = 30%) & (F1空值个数 = 1000)",
                                "event_detail": "当前F1空值率(30%)已大于阈值(20%)，超出50%，缺失较为严重"
                            },
                            {
                                "event_time": "2020-07-26 00:00:00",
                                "event_status_alias": "(F1空值率 = 30%) & (F1空值个数 = 1000)",
                                "event_detail": "当前F1空值率(30%)已大于阈值(20%)，超出50%，缺失较为严重"
                            }
                        ],
                        "in_effect": true,
                        "notify_ways": ["voice"],
                        "receivers": "admin,admin1",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        data_set_id = params["data_set_id"]
        start_time = params.get("start_time", int(time.time()) - 86400)
        end_time = params.get("end_time", int(time.time()))

        rule_events = (
            DataQualityAuditRuleEvent.objects.select_related("event")
            .select_related("rule")
            .filter(rule__data_set_id=data_set_id)
        )

        event_instances = {}
        sql = """
            SELECT * FROM dataquality_event
            WHERE data_set_id = '{data_set_id}' AND time >= {start_time}s AND time < {end_time}s
        """.format(
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
        )
        influx_results = influx_query(sql, is_dict=True)
        for event_instance in influx_results:
            event_id = event_instance.get("event_id")
            if event_id not in event_instances:
                event_instances[event_id] = []
            event_instances[event_id].append(
                {
                    "event_time": timetostr(event_instance.get("time")),
                    "event_status_alias": (event_instance.get("event_status_alias") or "").replace("\\n", "\n"),
                    "event_detail": event_instance.get("event_detail"),
                }
            )

        results = []
        for rule_event in rule_events:
            results.append(
                {
                    "event_id": rule_event.event.event_id,
                    "event_alias": rule_event.event.event_alias,
                    "description": rule_event.event.description,
                    "event_currency": rule_event.event.event_currency,
                    "event_type": rule_event.event.event_type,
                    "event_sub_type": rule_event.event.event_sub_type,
                    "event_polarity": rule_event.event.event_polarity,
                    "sensitivity": rule_event.event.sensitivity,
                    "rule": {
                        "rule_id": rule_event.rule.id,
                        "rule_name": rule_event.rule.rule_name,
                        "rule_config_alias": rule_event.rule.rule_config_alias,
                    },
                    "event_instances": event_instances.get(rule_event.event.event_id, []),
                    "in_effect": True,
                    "notify_ways": rule_event.notify_ways.split(","),
                    "receivers": rule_event.receivers.split(","),
                    "created_by": rule_event.event.created_by,
                    "created_at": rule_event.event.created_at,
                    "updated_by": rule_event.event.updated_by,
                    "updated_at": rule_event.event.updated_at,
                }
            )
        return Response(results)

    @list_route(methods=["get"], url_path="summary")
    @params_valid(serializer=DataQualityDataSetSerializer)
    def summary(self, request, params):
        """
        @api {get} /datamanage/dataquality/events/summary/ 数据审核事件信息汇总

        @apiVersion 1.0.0
        @apiGroup DataQualityEvent
        @apiName dataquality_events_summary
        @apiDescription 数据审核事件信息汇总

        @apiParam {String} data_set_id 数据集ID

        @apiSuccess (200) {Number} data.event_count 今日事件总数
        @apiSuccess (200) {Number} data.event_notified_count 今日事件通知总数

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "event_count": 10,
                    "event_notified_count": 10
                }
            }
        """
        sql = """
            SELECT count(event_detail) as total_count FROM dataquality_event
            WHERE data_set_id = '{data_set_id}' AND time > now() - 1d
        """.format(
            data_set_id=params["data_set_id"]
        )
        tsdb_results = influx_query(sql, is_dict=True)

        result = {
            "event_count": tsdb_results[0].get("total_count", 0) if len(tsdb_results) > 0 else 0,
            "event_notified_count": tsdb_results[0].get("total_count", 0) if len(tsdb_results) > 0 else 0,
        }
        return Response(result)

    @list_route(methods=["get"], url_path="notify_configs")
    def notify_configs(self, request):
        """
        @api {get} /datamanage/dataquality/events/notify_configs/ 查询事件通知配置

        @apiVersion 3.5.0
        @apiGroup DataQualityEvent
        @apiName dataquality_event_notify_configs
        @apiDescription 查询事件通知配置

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "event_id": "custom_event1",
                        "rule_id": "custom_rule",
                        "event_type": "data_quality",
                        "event_sub_type": "profiling_metric_out_of_range",
                        "notify_ways": ["voice"],
                        "receivers": ["user1", "user2"],
                        "convergence_config": {
                            "duration": 60,
                            "alert_threshold": 1,
                            "mask_time": 60
                        },
                        "active": true,
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        notify_configs = []

        rule_events = DataQualityAuditRuleEvent.objects.select_related("event").select_related("rule").all()

        for rule_event in rule_events:
            notify_configs.append(
                {
                    "notify_config_id": rule_event.id,
                    "event_id": rule_event.event.event_id,
                    "rule_id": rule_event.rule.id,
                    "event_type": rule_event.event.event_type,
                    "event_sub_type": rule_event.event.event_sub_type,
                    "notify_ways": rule_event.notify_ways.split(","),
                    "receivers": rule_event.receivers.split(","),
                    "convergence_config": json.loads(rule_event.convergence_config or "{}"),
                    "active": rule_event.active,
                    "created_by": rule_event.event.created_by,
                    "created_at": rule_event.event.created_at,
                    "updated_by": rule_event.event.updated_by,
                    "updated_at": rule_event.event.updated_at,
                }
            )
        return Response(notify_configs)

    @list_route(methods=["get"], url_path="stats")
    @params_valid(serializer=DataQualityEventListSerializer)
    def stats(self, request, params):
        """
        @api {get} /datamanage/dataquality/events/stats/ 查询事件统计信息

        @apiVersion 3.5.0
        @apiGroup DataQualityEvent
        @apiName dataquality_event_stats
        @apiDescription 查询事件统计信息

        @apiParam {String} data_set_id 数据集
        @apiParam {Number} start_time 开始时间（10位时间戳）
        @apiParam {Number} end_time 结束时间（10位时间戳）

        @apiSuccess (200) {Object} data.total_events 总事件数
        @apiSuccess (200) {Object} data.summary 汇总统计（用于饼图）
        @apiSuccess (200) {Object} data.trend 趋势统计（用于堆叠柱形图）

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "total_events": 10,
                    "summary": {
                        "data_interrupt": {
                            "alias": "数据中断",
                            "count": 120
                        },
                        "invalid_data": {
                            "alias": "无效数据",
                            "count": 210
                        },
                        "data_loss": {
                            "alias": "数据丢失",
                            "count": 210
                        },
                        "custom_event1": {
                            "alias": "自定义事件1",
                            "count": 120
                        }
                    },
                    "trend": {
                        "data_interrupt": {
                            "alias": "数据中断",
                            "times": [
                                "2020-07-27 00:00:00",
                                "2020-07-27 01:00:00",
                                "2020-07-27 02:00:00",
                                "2020-07-27 03:00:00",
                                "2020-07-27 04:00:00",
                                "2020-07-27 05:00:00"
                            ],
                            "count": [10, 20, 30, 30, 20, 10]
                        },
                        "invalid_data": {
                            "alias": "无效数据",
                            "times": [
                                "2020-07-27 00:00:00",
                                "2020-07-27 01:00:00",
                                "2020-07-27 02:00:00",
                                "2020-07-27 03:00:00",
                                "2020-07-27 04:00:00",
                                "2020-07-27 05:00:00"
                            ],
                            "count": [10, 20, 30, 40, 50, 60]
                        },
                        "data_loss": {
                            "alias": "数据丢失",
                            "times": [
                                "2020-07-27 00:00:00",
                                "2020-07-27 01:00:00",
                                "2020-07-27 02:00:00",
                                "2020-07-27 03:00:00",
                                "2020-07-27 04:00:00",
                                "2020-07-27 05:00:00"
                            ],
                            "count": [60, 50, 40, 30, 20, 10]
                        },
                        "custom_event1": {
                            "alias": "自定义事件1",
                            "times": [
                                "2020-07-27 00:00:00",
                                "2020-07-27 01:00:00",
                                "2020-07-27 02:00:00",
                                "2020-07-27 03:00:00",
                                "2020-07-27 04:00:00",
                                "2020-07-27 05:00:00"
                            ],
                            "count": [30, 20, 10, 10, 20, 30]
                        }
                    }
                }
            }
        """
        data_set_id = params["data_set_id"]
        start_time = params.get("start_time", int(time.time()) - 86400)
        end_time = params.get("end_time", int(time.time()))
        result = {
            "total_events": 0,
            "summary": {},
            "trend": {},
        }

        # 准备事件子类型alias信息
        event_sub_types = EventTypeConfig.objects.filter(parent_type_name="data_quality")
        alias_mappings = {item.event_type_name: item.event_type_alias for item in event_sub_types}

        self.fetch_event_types_summary_result(data_set_id, start_time, end_time, result, alias_mappings)
        self.fetch_event_types_trend_result(data_set_id, start_time, end_time, result, alias_mappings)

        return Response(result)

    def fetch_event_types_summary_result(self, data_set_id, start_time, end_time, result, alias_mappings):
        sql = """
            SELECT count(event_detail) as total_count FROM dataquality_event
            WHERE data_set_id = '{data_set_id}' AND time >= {start_time}s AND time < {end_time}s
            GROUP BY event_sub_type
        """.format(
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
        )
        tsdb_results = influx_query(sql, is_dict=True)

        for item in tsdb_results:
            event_sub_type = item.get("event_sub_type")
            if event_sub_type not in result["summary"]:
                result["summary"][event_sub_type] = {
                    "alias": alias_mappings.get(event_sub_type, event_sub_type),
                    "count": 0,
                }
            result["summary"][event_sub_type]["count"] += item.get("total_count", 0)
            result["total_events"] += item.get("total_count", 0)

    def fetch_event_types_trend_result(self, data_set_id, start_time, end_time, result, alias_mappings):
        sql = """
            SELECT count(event_detail) as total_count FROM dataquality_event
            WHERE data_set_id = '{data_set_id}' AND time >= {start_time}s AND time < {end_time}s
            GROUP BY event_sub_type, time(1h) fill(0)
        """.format(
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
        )
        tsdb_results = influx_query(sql, is_dict=True)

        for item in tsdb_results:
            event_sub_type = item.get("event_sub_type")
            if event_sub_type not in result["trend"]:
                result["trend"][event_sub_type] = {
                    "alias": alias_mappings.get(event_sub_type, event_sub_type),
                    "times": [],
                    "count": [],
                }
            result["trend"][event_sub_type]["times"].append(timetostr(item.get("time")))
            result["trend"][event_sub_type]["count"].append(item.get("total_count", 0))


class DataQualityEventTypeViewSet(BaseMixin, APIModelViewSet):
    model = EventTypeConfig
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    search_fields = ("parent_type_name",)
    pagination_class = DataPageNumberPagination
    serializer_class = EventTypeConfigSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True).order_by("seq_index")

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/event_types/ 数据审核事件类型列表

        @apiVersion 3.5.0
        @apiGroup DataQualityEvent
        @apiName dataquality_event_type_list
        @apiDescription 数据审核事件类型列表

        @apiSuccess (200) {String} data.event_type_name 事件类型名称
        @apiSuccess (200) {String} data.event_type_alias 事件类型别名
        @apiSuccess (200) {String} data.description 模板描述
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "event_type_name": "data_profiling",
                        "event_type_alias": "数据剖析事件",
                        "description": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityEventTypeViewSet, self).list(request)


class DataQualityEventTemplateVariableViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityEventTemplateVariable
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    search_fields = ("var_name",)
    pagination_class = DataPageNumberPagination
    serializer_class = DataQualityEventTemplateVariableSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True).order_by("id")

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/event_template_variables/ 数据审核事件模板变量列表

        @apiVersion 3.5.0
        @apiGroup DataQualityEvent
        @apiName dataquality_event_template_variable_list
        @apiDescription 数据审核事件模板变量列表

        @apiSuccess (200) {String} data.var_name 事件模板变量名称
        @apiSuccess (200) {String} data.var_alias 事件模板变量别名
        @apiSuccess (200) {String} data.var_example 事件模板变量示例
        @apiSuccess (200) {String} data.description 模板描述
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "var_name": "event_name",
                        "var_alias": "事件名称",
                        "var_example": "",
                        "description": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityEventTemplateVariableViewSet, self).list(request)
