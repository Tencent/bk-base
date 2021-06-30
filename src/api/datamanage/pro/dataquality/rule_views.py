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

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.models.audit import (
    DataQualityAuditFunction,
    DataQualityAuditRule,
    DataQualityAuditRuleEvent,
    DataQualityAuditRuleTemplate,
    DataQualityAuditTask,
    DataQualityEvent,
    DataQualityMetric,
)
from datamanage.pro.dataquality.serializers.audit import (
    DataQualityAuditFunctionSerializer,
    DataQualityAuditTaskSerializer,
    DataQualityRuleMetricsSerializer,
    DataQualityRuleSerializer,
    DataQualityRuleTemplateSerializer,
)
from datamanage.pro.dataquality.serializers.base import DataQualityDataSetSerializer
from datamanage.pro.pizza_settings import RULE_AUDIT_TASK_QUEUE
from datamanage.utils.dbtools.redis_util import redis_lpush
from datamanage.utils.drf import DataPageNumberPagination
from django.db import transaction
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.response import Response

from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIModelViewSet, APIViewSet


class DataQualityRuleViewSet(BaseMixin, APIViewSet):
    lookup_field = "rule_id"

    @params_valid(serializer=DataQualityRuleSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/dataquality/rules/ 创建数据质量审核规则

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rules_create
        @apiDescription 创建数据质量审核规则

        @apiParam {String} data_set_id 数据集ID
        @apiParam {Number} bk_biz_id 业务ID
        @apiParam {String} rule_name 规则名称
        @apiParam {String} rule_description 规则描述
        @apiParam {Number} rule_template_id 规则模板ID
        @apiParam {Object} rule_config 规则配置
        @apiParam {String} rule_config_alias 规则配置别名
        @apiParam {String} event_name 事件名称
        @apiParam {String} event_alias 事件别名
        @apiParam {String} event_description 事件描述
        @apiParam {String} event_type 事件类型
        @apiParam {String} event_polarity 事件极性
        @apiParam {Number} event_currency 事件时效性
        @apiParam {String} sensitivity 敏感程度
        @apiParam {String} event_detail_template 事件详情模板
        @apiParam {List} notify_ways 通知方式
        @apiParam {List} receivers 接收人
        @apiParam {Object} convergence_config 收敛策略
        @apiParam {Number} convergence_config.duration 触发时间周期范围
        @apiParam {Number} convergence_config.alert_threshold 周期内触发次数
        @apiParam {Number} convergence_config.mask_time 屏蔽时间

        @apiParamExample {json} 参数样例:
        {
            "data_set_id": "591_test",
            "rule_name": "规则一",
            "rule_description": "规则一的描述",
            "rule_template_id": 1,
            "rule_config": [
                {
                    "metric": {
                        "metric_type": "data_flow",
                        "metric_name": "output_count"
                    },
                    "function": "greater",
                    "constant": {
                        "constant_type": "float",
                        "constant_value": 100
                    },
                    "operation": "and"
                },
                {
                    "metric": {
                        "metric_type": "data_profiling",
                        "metric_name": "null_rate",
                        "metric_field": "field1"
                    },
                    "function": "less",
                    "constant": {
                        "constant_type": "float",
                        "constant_value": 100
                    },
                    "operation": null
                }
            ],
            "rule_config_alias": "(输出量 > 100) & (field1 空值率 < 100)",
            "event_name": "rule1_event",
            "event_alias": "规则一的事件",
            "event_description": "规则一事件的描述",
            "event_type": "data_quality",
            "event_sub_type": "test",
            "event_polarity": "positive",
            "event_currency": 30,
            "sensitivity": "private",
            "event_detail_template": "test",
            "notify_ways": ["wechart"],
            "receivers": ["admin"],
            "convergence_config": {
                "duration": 1,
                "alert_threshold": 1,
                "mask_time": 60
            }
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }

        """
        bk_username = get_request_username()
        with transaction.atomic(using="bkdata_basic"):
            rule = DataQualityAuditRule.objects.create(
                data_set_id=params["data_set_id"],
                bk_biz_id=params["bk_biz_id"],
                rule_name=params["rule_name"],
                rule_template_id=params["rule_template_id"],
                rule_config=json.dumps(params["rule_config"]),
                rule_config_alias=params["rule_config_alias"],
                created_by=bk_username,
                generate_type="user",
                description=params["rule_description"],
            )

            event = DataQualityEvent.objects.create(
                event_id="{}_{}".format(params["bk_biz_id"], params["event_name"]),
                event_name=params["event_name"],
                event_alias=params["event_alias"],
                event_currency=params["event_currency"],
                event_type=params["event_type"],
                event_sub_type=params.get("event_sub_type"),
                event_polarity=params["event_polarity"],
                event_detail_template=params["event_detail_template"],
                sensitivity=params["sensitivity"],
                generate_type="user",
                created_by=bk_username,
                description=params["event_description"],
            )

            DataQualityAuditRuleEvent.objects.create(
                rule=rule,
                event=event,
                notify_ways=",".join(params["notify_ways"]),
                receivers=",".join(params["receivers"]),
                convergence_config=json.dumps(params["convergence_config"]),
                created_by=bk_username,
            )
        return Response(True)

    @params_valid(serializer=DataQualityRuleSerializer)
    def update(self, request, rule_id, params):
        """
        @api {put} /datamanage/dataquality/rules/{rule_id}/ 更新数据质量审核规则

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rules_update
        @apiDescription 更新数据质量审核规则

        @apiParam {String} rule_name 规则名称
        @apiParam {String} rule_description 规则描述
        @apiParam {Number} rule_template_id 规则模板ID
        @apiParam {Object} rule_config 规则配置
        @apiParam {String} rule_config_alias 规则配置别名
        @apiParam {String} event_name 事件名称
        @apiParam {String} event_alias 事件别名（新增）
        @apiParam {String} event_description 事件描述
        @apiParam {String} event_type 事件类型
        @apiParam {String} event_sub_type 事件子类型（新增）
        @apiParam {String} event_polarity 事件极性
        @apiParam {Number} event_currency 事件时效性
        @apiParam {String} sensitivity 敏感程度
        @apiParam {String} event_detail_template 事件详情模板
        @apiParam {List} notify_ways 通知方式
        @apiParam {List} receivers 接收人
        @apiParam {Object} convergence_config 收敛策略
        @apiParam {Number} convergence_config.duration 触发时间周期范围
        @apiParam {Number} convergence_config.alert_threshold 周期内触发次数
        @apiParam {Number} convergence_config.mask_time 屏蔽时间

        @apiParamExample {json} 参数样例:
        {
            "rule_name": "规则一",
            "rule_description": "规则一的描述",
            "rule_template_id": 1
            "rule_config": [
                {
                    "metric": {
                        "metric_type": "data_flow",
                        "metric_name": "output_count"
                    },
                    "function": "greater",
                    "constant": {
                        "constant_type": "float",
                        "constant_value": 100
                    },
                    "operation": "and"
                },
                {
                    "metric": {
                        "metric_type": "data_profiling",
                        "metric_name": "null_rate",
                        "metric_field": "field1"
                    },
                    "function": "less",
                    "constant": {
                        "constant_type": "float",
                        "constant_value": 100
                    },
                    "operation": null
                }
            ],
            "rule_config_alias": "(输出量 > 100) & (field1 空值率 < 100)",
            "event_name": "rule1_event",
            "event_alias": "规则一的事件",
            "event_description": "规则一事件的描述",
            "event_type": "data_quality",
            "event_sub_type": "",
            "event_polarity": "positive",
            "event_currency": 30,
            "sensitivity": "private",
            "event_detail_template": "",
            "notify_ways": ["wechart"],
            "receivers": ["admin"],
            "convergence_config": {
                "duration": 1,
                "alert_threshold": 1,
                "mask_time": 60
            }
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        bk_username = get_request_username()
        try:
            rule = DataQualityAuditRule.objects.get(id=rule_id)
        except DataQualityAuditRule.DoesNotExist:
            raise dm_pro_errors.AuditRuleNotExistError()

        task_status = "waiting"
        try:
            tasks = DataQualityAuditTask.objects.filter(rule=rule)
            if tasks.count() > 0:
                task_status = tasks[0].status
        except Exception as e:
            logger.error(e)
        if task_status == "running":
            raise dm_pro_errors.RunningAuditTaskCannotEditError()

        try:
            rule_event = DataQualityAuditRuleEvent.objects.filter(rule=rule)[0]
            event = DataQualityEvent.objects.filter(event_id=rule_event.event_id)[0]
        except Exception:
            raise dm_pro_errors.DataQualityEventNotExistError()

        with transaction.atomic(using="bkdata_basic"):
            rule.rule_name = params["rule_name"]
            rule.rule_template_id = params["rule_template_id"]
            rule.rule_config = json.dumps(params["rule_config"])
            rule.rule_config_alias = params["rule_config_alias"]
            rule.updated_by = bk_username
            rule.description = params["rule_description"]
            rule.save()

            event.event_alias = params["event_alias"]
            event.event_currency = params["event_currency"]
            event.event_type = params["event_type"]
            event.event_sub_type = params.get("event_sub_type")
            event.event_polarity = params["event_polarity"]
            event.event_detail_template = params["event_detail_template"]
            event.sensitivity = params["sensitivity"]
            event.updated_by = bk_username
            event.save()

            rule_event.notify_ways = ",".join(params["notify_ways"])
            rule_event.receivers = ",".join(params["receivers"])
            rule_event.convergence_config = json.dumps(params["convergence_config"])
            rule_event.updated_by = bk_username
            rule_event.save()
        return Response(True)

    def delete(self, request, rule_id):
        """
        @api {delete} /datamanage/dataquality/rules/{rule_id}/ 删除数据质量审核规则

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rules_delete
        @apiDescription 删除数据质量审核规则

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        try:
            rule = DataQualityAuditRule.objects.get(id=rule_id)
        except DataQualityAuditRule.DoesNotExist:
            raise dm_pro_errors.AuditRuleNotExistError()

        task_status = "waiting"
        try:
            tasks = DataQualityAuditTask.objects.filter(rule=rule)
            if tasks.count() > 0:
                task_status = tasks[0].status
        except Exception as e:
            logger.error(e)
        if task_status == "running":
            raise dm_pro_errors.RunningAuditTaskCannotEditError()

        try:
            rule_event = DataQualityAuditRuleEvent.objects.filter(rule=rule)[0]
            event = DataQualityEvent.objects.filter(event_id=rule_event.event_id)[0]
        except Exception:
            raise dm_pro_errors.DataQualityEventNotExistError()

        with transaction.atomic(using="bkdata_basic"):
            rule_event.delete()
            event.delete()
            rule.delete()
        return Response(True)

    @params_valid(serializer=DataQualityDataSetSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dataquality/rules/ 数据质量审核规则列表

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rules_list
        @apiDescription 数据质量审核规则列表

        @apiParam {String} data_set_id 数据集ID

        @apiSuccess (200) {Number} data.rule_id 规则ID
        @apiSuccess (200) {String} data.rule_name 规则名称
        @apiSuccess (200) {String} data.rule_description 规则描述
        @apiSuccess (200) {String} data.rule_template_id 规则模板ID
        @apiSuccess (200) {String} data.rule_template_alias 规则模板别名
        @apiSuccess (200) {String} data.rule_config 规则配置
        @apiSuccess (200) {String} data.rule_config_alias 规则配置别名
        @apiSuccess (200) {String} data.audit_task_status 审核任务状态
        @apiSuccess (200) {String} data.event_id 事件ID
        @apiSuccess (200) {String} data.event_name 事件名称
        @apiSuccess (200) {String} data.event_alias 事件别名
        @apiSuccess (200) {Number} data.event_description 事件描述
        @apiSuccess (200) {String} data.event_type 事件类型
        @apiSuccess (200) {String} data.event_sub_type 事件子类型
        @apiSuccess (200) {String} data.event_polarity 事件极性
        @apiSuccess (200) {Number} data.event_currency 事件时效性
        @apiSuccess (200) {String} data.sensitivity 事件敏感度
        @apiSuccess (200) {String} data.event_detail_template 事件详情模板
        @apiSuccess (200) {List} data.notify_ways 事件通知方式
        @apiSuccess (200) {List} data.receivers 事件通知接收者
        @apiSuccess (200) {Object} data.convergence_config 事件通知收敛配置
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
                        "rule_id": 1,
                        "rule_name": "规则一",
                        "rule_description": "规则一的描述",
                        "rule_template_id": 1,
                        "rule_template_alias": "空值约束",
                        "audit_task_status": "running",
                        "rule_config": [
                            {
                                "metric": {
                                    "metric_type": "data_flow",
                                    "metric_name": "output_count"
                                },
                                "function": "greater",
                                "constant": {
                                    "constant_type": "float",
                                    "constant_value": 100
                                },
                                "operation": "and"
                            },
                            {
                                "metric": {
                                    "metric_type": "data_profiling",
                                    "metric_name": "null_rate",
                                    "metric_field": "field1"
                                },
                                "function": "less",
                                "constant": {
                                    "constant_type": "float",
                                    "constant_value": 100
                                },
                                "operation": null
                            }
                        ],
                        "rule_config_alias": "(输出量 > 100) & (field1 空值率 < 100)",
                        "event_id": "591_rule1_event",
                        "event_name": "rule1_event",
                        "event_alias": "规则一的事件",
                        "event_description": "规则一事件的描述",
                        "event_type": "data_quality",
                        "event_sub_type": "",
                        "event_polarity": "positive",
                        "event_currency": 30,
                        "sensitivity": "private",
                        "event_detail_template": "",
                        "notify_ways": ["wechat"],
                        "receivers": ["admin"],
                        "convergence_config": {
                            "duration": 1,
                            "alert_threshold": 1,
                            "mask_time": 60
                        },
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        results = []
        rules = (
            DataQualityAuditRule.objects.select_related("rule_template")
            .filter(data_set_id=params["data_set_id"])
            .order_by("id")
        )
        for rule in rules:
            try:
                rule_event = DataQualityAuditRuleEvent.objects.select_related("event").filter(rule=rule)[0]
            except Exception as e:
                logger.error(e)
                raise dm_pro_errors.AuditRuleEventNotFoundError()

            task_status = "waiting"
            try:
                tasks = DataQualityAuditTask.objects.filter(rule=rule)
                if tasks.count() > 0:
                    task_status = tasks[0].status
            except Exception as e:
                logger.error(e)

            results.append(
                {
                    "rule_id": rule.id,
                    "rule_name": rule.rule_name,
                    "rule_description": rule.description,
                    "rule_template_id": rule.rule_template.id,
                    "rule_template_alias": rule.rule_template.template_alias,
                    "audit_task_status": task_status,
                    "rule_config": json.loads(rule.rule_config),
                    "rule_config_alias": rule.rule_config_alias,
                    "event_id": rule_event.event.event_id,
                    "event_name": rule_event.event.event_name,
                    "event_alias": rule_event.event.event_alias,
                    "event_description": rule_event.event.description,
                    "event_type": rule_event.event.event_type,
                    "event_sub_type": rule_event.event.event_sub_type,
                    "event_polarity": rule_event.event.event_polarity,
                    "event_currency": rule_event.event.event_currency,
                    "sensitivity": rule_event.event.sensitivity,
                    "event_detail_template": rule_event.event.event_detail_template,
                    "notify_ways": rule_event.notify_ways.split(","),
                    "receivers": rule_event.receivers.split(","),
                    "convergence_config": json.loads(rule_event.convergence_config),
                    "created_by": rule.created_by,
                    "created_at": rule.created_at,
                    "updated_by": rule.updated_by,
                    "updated_at": rule.updated_at,
                }
            )

        return Response(results)

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, rule_id):
        """
        @api {post} /datamanage/dataquality/rules/{rule_id}/start/ 启动数据质量审核任务

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rule_start
        @apiDescription 启动数据质量审核任务

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        bk_username = get_request_username()
        try:
            rule = DataQualityAuditRule.objects.get(id=rule_id)
        except DataQualityAuditRule.DoesNotExist:
            raise dm_pro_errors.AuditRuleNotExistError()

        try:
            rule_event = DataQualityAuditRuleEvent.objects.select_related("event").filter(rule=rule)[0]
        except Exception as e:
            logger.error(e)
            raise dm_pro_errors.AuditRuleEventNotFoundError()

        try:
            task = DataQualityAuditTask.objects.filter(rule=rule)[0]
            if task.status == "running":
                raise dm_pro_errors.AuditTaskAlreadyStartedError()
            task.rule_config = json.dumps(DataQualityAuditTask.convert_task_rule_config(rule, rule_event.event))
            task.rule_config_alias = rule.rule_config_alias
            task.status = "running"
            task.updated_by = bk_username
            task.save()
            self.start_task(task)
        except IndexError:
            task = DataQualityAuditTask(
                data_set_id=rule.data_set_id,
                rule=rule,
                rule_config=json.dumps(DataQualityAuditTask.convert_task_rule_config(rule, rule_event.event)),
                rule_config_alias=rule.rule_config_alias,
                status="running",
                created_by=bk_username,
            )
            task.save()
            self.start_task(task)

        return Response(True)

    def start_task(self, task):
        try:
            result = redis_lpush(
                RULE_AUDIT_TASK_QUEUE,
                json.dumps(
                    {
                        "data_set_id": task.data_set_id,
                        "rule_id": task.rule.id,
                        "rule_config": task.rule_config,
                    }
                ),
            )
            if not result:
                raise dm_pro_errors.StartAuditTaskError()
        except Exception:
            raise dm_pro_errors.StartAuditTaskError()

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, rule_id):
        """
        @api {post} /datamanage/dataquality/rules/{rule_id}/stop/ 停止数据质量审核任务

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rule_stop
        @apiDescription 停止数据质量审核任务

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        bk_username = get_request_username()
        try:
            task = DataQualityAuditTask.objects.filter(rule=rule_id)[0]
            if task.status == "waiting":
                raise dm_pro_errors.AuditTaskAlreadyStoppedError()
            task.status = "waiting"
            task.updated_by = bk_username
            task.save()
        except IndexError:
            raise dm_pro_errors.AuditTaskAlreadyStoppedError()
        return Response(True)

    @detail_route(methods=["get"], url_path="metrics")
    @params_valid(serializer=DataQualityRuleMetricsSerializer)
    def metrics(self, request, rule_id, params):
        """
        @api {get} /datamanage/dataquality/rules/{rule_id}/metrics/ 数据质量审核规则相关指标

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rule_metrics
        @apiDescription 数据质量审核规则相关指标

        @apiParam {Number} start_time 开始时间
        @apiParam {Number} end_time 结束时间

        @apiSuccess (200) {object} field1_x 指标信息
        @apiSuccess (200) {object} field1_x.metric_type 指标类型
        @apiSuccess (200) {String} field1_x.metric_name 指标名称
        @apiSuccess (200) {String} field1_x.metric_alias 指标别名
        @apiSuccess (200) {String} field1_x.metric_field 字段名称
        @apiSuccess (200) {List} field1_x.today 指标1今日曲线数据
        @apiSuccess (200) {List} field1_x.last_day 指标1昨日曲线数据
        @apiSuccess (200) {List} field1_x.last_week 指标1上周曲线数据

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "F1_null_rate": {
                        "metric_type": "data_profiling",
                        "metric_name": "null_rate",
                        "metric_alias": "空值率",
                        "metric_field": "F1",
                        "today": [
                            {
                                "time": 1596470400,
                                "null_rate": 10
                            }
                        ],
                        "last_day": [
                            {
                                "time": 1596470400,
                                "null_rate": 10
                            }
                        ],
                        "last_week": [
                            {
                                "time": 1596470400,
                                "null_rate": 10
                            }
                        ]
                    }
                }
            }
        """
        start_time = params.get("start_time", time.time() - 86400)
        end_time = params.get("end_time", time.time())
        try:
            rule = DataQualityAuditRule.objects.get(id=rule_id)
        except DataQualityAuditRule.DoesNotExist:
            raise dm_pro_errors.AuditRuleNotExistError()

        metrics = DataQualityMetric.objects.filter(active=True)
        metrics_configs = {metric.metric_name: metric for metric in metrics}

        response = {}
        rule_configs = json.loads(rule.rule_config) or []
        for rule_config in rule_configs:
            metric_info = rule_config.get("metric")
            metric_type = metric_info.get("metric_type")
            metric_name = metric_info.get("metric_name")

            try:
                metric_config = metrics_configs[metric_name]
            except Exception as e:
                logger.error(e)
                continue

            if metric_type == "data_profiling":
                metric_field = metric_info.get("metric_field")
                metric_key = "{}_{}".format(metric_field, metric_name)
                response[metric_key] = {
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "metric_alias": metric_config.metric_alias,
                    "today": self.get_today_data_profiling_series(
                        metric_config,
                        rule.data_set_id,
                        metric_field,
                        start_time,
                        end_time,
                    ),
                    "last_day": self.get_last_day_data_profiling_series(
                        metric_config,
                        rule.data_set_id,
                        metric_field,
                        start_time,
                        end_time,
                    ),
                    "last_week": self.get_last_week_data_profiling_series(
                        metric_config,
                        rule.data_set_id,
                        metric_field,
                        start_time,
                        end_time,
                    ),
                }
            else:
                response[metric_name] = {
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "metric_alias": metric_config.metric_alias,
                    "today": self.get_today_data_flow_series(metric_name, rule.data_set_id, start_time, end_time),
                    "last_day": self.get_last_day_data_flow_series(metric_name, rule.data_set_id, start_time, end_time),
                    "last_week": self.get_last_week_data_flow_series(
                        metric_name, rule.data_set_id, start_time, end_time
                    ),
                }
        return Response(response)

    def get_today_data_flow_series(self, metric_name, data_set_id, start_time, end_time):
        results = self.get_metric_value(
            metric_name,
            data_set_id=data_set_id,
            storage=None,
            start_time="{}s".format(start_time),
            end_time="{}s".format(end_time),
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1m",
        )

        for storage in ["kafka", "hdfs"]:
            for item in results:
                if item["storage_cluster_type"] == storage:
                    return item["series"]
        return []

    def get_last_day_data_flow_series(self, metric_name, data_set_id, start_time, end_time):
        results = self.get_metric_value(
            metric_name,
            data_set_id=data_set_id,
            storage=None,
            start_time="{}s".format(start_time - 86400),
            end_time="{}s".format(end_time - 86400),
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1m",
        )

        for storage in ["kafka", "hdfs"]:
            for item in results:
                if item["storage_cluster_type"] == storage:
                    return item["series"]
        return []

    def get_last_week_data_flow_series(self, metric_name, data_set_id, start_time, end_time):
        results = self.get_metric_value(
            metric_name,
            data_set_id=data_set_id,
            storage=None,
            start_time="{}s".format(start_time - 86400 * 7),
            end_time="{}s".format(end_time - 86400 * 7),
            format="series",
            tags=["storage_cluster_type"],
            time_grain="1m",
        )

        for storage in ["kafka", "hdfs"]:
            for item in results:
                if item["storage_cluster_type"] == storage:
                    return item["series"]
        return []

    def get_today_data_profiling_series(self, metric_info, data_set_id, field, start_time, end_time):
        results = self.get_profiling_metric_value(
            json.loads(metric_info.metric_config),
            data_set_id=data_set_id,
            field=field,
            start_time="{}s".format(start_time),
            end_time="{}s".format(end_time),
            format="series",
            time_grain="1m",
        )
        return results[0]["series"]

    def get_last_day_data_profiling_series(self, metric_info, data_set_id, field, start_time, end_time):
        results = self.get_profiling_metric_value(
            json.loads(metric_info.metric_config),
            data_set_id=data_set_id,
            field=field,
            start_time="{}s".format(start_time - 86400),
            end_time="{}s".format(end_time - 86400),
            format="series",
            time_grain="1m",
        )
        return results[0]["series"]

    def get_last_week_data_profiling_series(self, metric_info, data_set_id, field, start_time, end_time):
        results = self.get_profiling_metric_value(
            json.loads(metric_info.metric_config),
            data_set_id=data_set_id,
            field=field,
            start_time="{}s".format(start_time - 86400 * 7),
            end_time="{}s".format(end_time - 86400 * 7),
            format="series",
            time_grain="1m",
        )
        return results[0]["series"]

    @list_route(methods=["get"], url_path="summary")
    @params_valid(serializer=DataQualityDataSetSerializer)
    def summary(self, request, params):
        """
        @api {get} /datamanage/dataquality/rules/summary/ 数据审核规则信息汇总

        @apiVersion 1.0.0
        @apiGroup DataQualityRule
        @apiName dataquality_rules_summary
        @apiDescription 数据审核规则信息汇总

        @apiParam {String} data_set_id 数据集ID

        @apiSuccess (200) {Number} data.rule_count 审核任务总数
        @apiSuccess (200) {Number} data.running_rule_count 运行中审核任务数量
        @apiSuccess (200) {Number} data.waiting_rule_count 待启动审核任务数量
        @apiSuccess (200) {Number} data.failed_rule_count 失败审核任务数量

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "rule_count": 10,
                    "running_rule_count": 10,
                    "waiting_rule_count": 10,
                    "failed_rule_count": 10
                }
            }
        """
        data_set_id = params["data_set_id"]

        rules = DataQualityAuditRule.objects.filter(data_set_id=params["data_set_id"])
        tasks = DataQualityAuditTask.objects.filter(data_set_id=data_set_id)

        response = {
            "rule_count": rules.count(),
            "running_rule_count": 0,
            "waiting_rule_count": 0,
            "failed_rule_count": 0,
        }

        for task in tasks:
            if task.status == "running":
                response["running_rule_count"] += 1
            elif task.status == "failed":
                response["failed_rule_count"] += 1
            elif task.status == "waiting":
                response["waiting_rule_count"] += 1

        response["waiting_rule_count"] += rules.count() - tasks.count()
        return Response(response)


class DataQualityRuleTemplateViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityAuditRuleTemplate
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = DataQualityRuleTemplateSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True)

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/rule_templates/ 数据质量审核规则模板列表

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_rule_templates_list
        @apiDescription 数据质量审核规则模板列表

        @apiSuccess (200) {Number} data.template_name 模板名称
        @apiSuccess (200) {String} data.template_alias 模板别名
        @apiSuccess (200) {String} data.template_config 模板配置
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
                        "template_id": 1,
                        "template_name": "null_constraint",
                        "template_alias": "空值约束",
                        "template_config": {},
                        "description": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityRuleTemplateViewSet, self).list(request)


class DataQualityAuditTaskViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityAuditTask
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = DataQualityAuditTaskSerializer
    search_fields = ("status",)
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.all()


class DataQualityAuditFunctionViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityAuditFunction
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = DataQualityAuditFunctionSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True)

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/functions/ 数据质量审核函数列表

        @apiVersion 3.5.0
        @apiGroup DataQualityRule
        @apiName dataquality_audit_functions_list
        @apiDescription 数据质量审核函数列表

        @apiSuccess (200) {Number} data.function_name 函数名称
        @apiSuccess (200) {String} data.function_alias 函数别名
        @apiSuccess (200) {String} data.function_type 函数类型
        @apiSuccess (200) {String} data.description 函数描述
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
                        "function_name": "addition",
                        "function_alias": "+",
                        "function_type": "builtin",
                        "description": "加法",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    },
                    {
                        "function_name": "subtraction",
                        "function_alias": "-",
                        "function_type": "builtin",
                        "description": "减法",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    },
                    {
                        "function_name": "multiplication",
                        "function_alias": "*",
                        "function_type": "builtin",
                        "description": "乘法",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    },
                    {
                        "function_name": "division",
                        "function_alias": "/",
                        "function_type": "builtin",
                        "description": "除法",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityAuditFunctionViewSet, self).list(request)
