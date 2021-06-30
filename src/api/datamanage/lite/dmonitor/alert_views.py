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
import datetime
import json

import gevent
from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor.constants import (
    ALERT_ENV_MAPPINGS,
    ALERT_TITLE_MAPPSINGS,
    ALERT_VERSION_MAPPINGS,
    GRAFANA_ALERT_STATUS,
    MSG_TYPE_MAPPSINGS,
)
from datamanage.lite.dmonitor.filters import AlertsFilter
from datamanage.lite.dmonitor.mixins.base_mixins import BaseMixin
from datamanage.lite.dmonitor.models import (
    AlertDetail,
    AlertLog,
    AlertShield,
    DatamonitorAlertConfigRelation,
)
from datamanage.lite.dmonitor.serializers import (
    AlertDetailListSerializer,
    AlertDetailMineSerializer,
    AlertDetailSerializer,
    AlertLogSerializer,
    AlertReportSerializer,
    AlertSendSerializer,
    AlertShieldSerializer,
    AlertTargetMineSerializer,
    GrafanaAlertSerilizer,
)
from datamanage.pizza_settings import NOTIFY_WITH_ENV, RUN_MODE, RUN_VERSION
from datamanage.utils.api import DataflowApi, MetaApi
from datamanage.utils.drf import DataPageNumberPagination
from datamanage.utils.time_tools import timetostr, tznow
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.response import Response

from common.api import CmsiApi
from common.bklanguage import BkLanguage
from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIModelViewSet, APIViewSet


class AlertViewSet(APIModelViewSet):
    """
    @api {get} /datamanage/dmonitor/alerts/:id/ 查询汇总告警详情
    @apiName dmonitor_retrivev_alert
    @apiGroup DmonitorAlert
    @apiVersion 1.0.0
    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": [
                    {
                        "message": "xxx",
                        "message_en": "xxx",
                        "alert_time": "2019-03-01 00:00:00",
                        "alert_level": "warning",
                        "receiver": "user",
                        "notify_way": "wechat",
                        "description": "xxxx",
                        "alert_details": [
                            {
                                "message": "yyyy",
                                "message_en": "yyyyy",
                                "full_message": "zzzzz",
                                "full_message_en": "zzzzz",
                                "alert_level": "warning",
                                "alert_code": "data_delay",
                                "bk_biz_id": 1,    // 可选维度
                                "project_id": 4,   // 可选维度
                                "flow_id": 1,      // 可选维度
                                "raw_data_id": 1,  // 可选维度
                                "result_table_id": "1_example",  // 可选维度
                                "bk_app_code": "dataweb"    // 可选维度
                            }
                        ]
                    }
                ],
                "message": "",
                "code": "00",
            }
    """

    """
    @api {get} /datamanage/dmonitor/alerts/ 查询汇总告警列表
    @apiName dmonitor_list_alerts
    @apiGroup DmonitorAlert
    @apiVersion 1.0.0

    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": {
                    "message": "xxx",
                    "message_en": "xxx",
                    "alert_time": "2019-03-01 00:00:00",
                    "alert_level": "warning",
                    "receiver": "user",
                    "notify_way": "wechat",
                    "description": "xxxx",
                    "alert_details": [
                        {
                            "message": "yyyy",
                            "message_en": "yyyyy",
                            "full_message": "zzzzz",
                            "full_message_en": "zzzzz",
                            "alert_level": "warning",
                            "alert_code": "data_delay",
                            "bk_biz_id": 1,    // 可选维度
                            "project_id": 4,   // 可选维度
                            "flow_id": 1,      // 可选维度
                            "raw_data_id": 1,  // 可选维度
                            "result_table_id": "1_example",  // 可选维度
                            "bk_app_code": "dataweb"    // 可选维度
                        }
                    ]
                },
                "message": "",
                "code": "00",
            }
    """
    model = AlertLog
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    filter_class = AlertsFilter
    search_fields = ("receiver",)
    pagination_class = DataPageNumberPagination
    serializer_class = AlertLogSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.all()

    @list_route(methods=["post"], url_path="send")
    @params_valid(serializer=AlertSendSerializer)
    def do_alert_send(self, request, params):
        """
        @api {post} /datamanage/dmonitor/alerts/send/ 发送告警
        @apiName dmonitor_send_alerts
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {String} receiver 告警接收人
        @apiParam {String="weixin","work-weixin","mail","sms"} notify_way 告警方式
        @apiParam {String} [title] 告警标题
        @apiParam {String} message 告警信息
        @apiParam {String} [message_en] 英文告警信息

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "00",
                }
        """
        receiver = params["receiver"]
        title = params.get("title")
        title_en = params.get("title_en")
        message = params["message"]
        notify_way = params["notify_way"]
        notify_way = MSG_TYPE_MAPPSINGS.get(notify_way, notify_way)

        if RUN_VERSION == "tencent":
            title = self.get_alert_title(title, title_en, BkLanguage.CN)
            if notify_way == "weixin":
                message = "\n{}".format(message)
            return Response(self.send_alert_message(receiver, notify_way, title, message))
        else:
            receivers = receiver.split(",")
            if notify_way == "voice":
                result = self.send_alert_message_with_voice(receivers, message, params)
            else:
                result = self.send_alert_message_one_by_one(receivers, notify_way, message, params)
            return Response(result)

    def get_alert_title(self, title=None, title_en=None, language=None):
        if NOTIFY_WITH_ENV:
            version = RUN_VERSION.upper()
            env = RUN_MODE.upper()
            if language == BkLanguage.EN:
                title = title_en
            return "{title}{version}{env}".format(
                title=title if (title is not None) else ALERT_TITLE_MAPPSINGS.get(language, title),
                version="[{}]".format(ALERT_VERSION_MAPPINGS.get(version, {}).get(language, version)),
                env="[{}]".format(ALERT_ENV_MAPPINGS.get(env, {}).get(language, env)) if env != "PRODUCT" else "",
            )
        else:
            return title if (title is not None and language is None) else ALERT_TITLE_MAPPSINGS.get(language, title)

    def send_alert_message(self, receivers, msg_type, title, content):
        # 内部版不支持voice语音，需要通过UWORK接口来发送
        if RUN_VERSION == "tencent" and msg_type == "voice":
            message = "{}\n{}".format(title, content)
            result = CmsiApi.send_voice_msg({"receivers": receivers, "message": message})
        else:
            result = CmsiApi.send_msg(
                {
                    "msg_type": msg_type,
                    "receivers": receivers,
                    "content": content,
                    "title": title,
                }
            )

        if not result.is_success():
            raise dm_errors.SendAlertError(result.message)
        return result.data

    def send_alert_message_with_voice(self, receivers, default_message, params):
        if len(receivers) > 0:
            first_user = receivers[0]
            language = BkLanguage.get_user_language(first_user)
            message = default_message
            if language != BkLanguage.CN:
                message = params.get("message_{}".format(language), "") or message
            title = self.get_alert_title(language=language)

            result = self.send_alert_message(",".join(receivers), "voice", title, message)
            return result
        return {}

    def send_alert_message_one_by_one(self, receivers, notify_way, default_message, params):
        result, errors = {}, {}
        for user in receivers:
            language = BkLanguage.get_user_language(user)
            message = default_message
            if language != BkLanguage.CN:
                message = params.get("message_{}".format(language), "") or message
            title = self.get_alert_title(language=language)

            try:
                result[user] = self.send_alert_message(user, notify_way, title, message)
            except Exception as e:
                errors[user] = str(e)

        if len(errors) > 0:
            errors.update(result)
            raise dm_errors.SendAlertError(errors=errors)

        return result

    @list_route(methods=["post"], url_path="report")
    @params_valid(serializer=AlertReportSerializer)
    def do_alert_report(self, request, params):
        """
        @api {post} /datamanage/dmonitor/alerts/report/ 上报告警
        @apiVersion 1.0.0
        @apiGroup DmonitorAlert
        @apiName dmonitor_report_alert

        @apiParam {object} flow_info 数据流信息
        @apiParam {string} alert_code 告警码, 任务监控目前亩田为task
        @apiParam {string} level 告警级别, 可选值有info/warning/danger
        @apiParam {string} message 告警信息
        @apiParam {string} message_en 告警英文信息
        @apiParam {string} [full_message] 告警详情
        @apiParam {string} [full_message_en] 告警英文详情
        @apiParam {object} custom_tags 自定义告警维度

        @apiParamExample {json} 参数样例:
            {
                // flow_info支持多种结构，目前总线的任务告警需要提供flow_id(实际为raw_data_id)
                "flow_info": {
                    "flow_id": 100,
                    "data_set_id": "1_clean_task"
                },
                "alert_code": "task",
                "level": "warning",
                "message": "清洗任务异常",
                "message_en": "There are some errors about cleaning task",
                "full_message": "清洗任务(xxx)异常",
                "full_message_en": "There are some errors about the xxx cleaning task",
                "custom_tags": {
                    "project_id": 1,
                    "bk_biz_id": 1,
                    "raw_data_id": 100,
                    "detail_code": "databus_custom_alert_code",
                }
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {}
            }
        """
        return Response({})

    @list_route(methods=["post"], url_path="grafana")
    @params_valid(serializer=GrafanaAlertSerilizer)
    def do_grafana_alert(self, request, params):
        """
        @api {post} /datamanage/dmonitor/alerts/grafana/ 处理Grafana告警
        @apiName dmonitor_send_grafana_alerts
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {String} title 告警标题
        @apiParam {String} ruleName 告警规则名称
        @apiParam {String} ruleUrl 告警规则路径
        @apiParam {String} state 告警状态
        @apiParam {String} [message] 告警信息
        @apiParam {Dict} [evalMatches] 告警参数

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "00",
                }
        """
        receivers = request.GET.getlist("receivers", [])
        notify_ways = request.GET.getlist("notify_ways", [])

        message = params.get("message", "")
        rule_name = params.get("ruleName", "")
        title = params.get("title", "")

        title = "{title}(Grafana)".format(title=rule_name or title)
        if len(receivers) > 0:
            first_user = receivers[0]
            language = BkLanguage.get_user_language(first_user)
            title = self.get_alert_title(title, language=language)

        for notify_way in notify_ways:
            title = "{title}(Grafana)".format(title=rule_name or title)
            message = ("[{status}]{message}\n" "各指标当前值为:\n" "\t{metric_value}\n" "详情请查看仪表板: {grafana_url}").format(
                status=GRAFANA_ALERT_STATUS.get(params.get("state"), "未知状态"),
                message=message,
                metric_value="\t\n".join(self.get_grafana_metric_values(params.get("evalMatches", []))),
                grafana_url=params.get("ruleUrl"),
            )
            msg_type = MSG_TYPE_MAPPSINGS.get(notify_way, notify_way)
            self.send_alert_message(receivers=receivers, msg_type=msg_type, title=title, content=message)

        return Response("ok")

    def get_grafana_metric_values(self, evalMatches):
        for item in evalMatches:
            yield "{metric_key}{tags_str}: {metric_value}".format(
                metric_key=item["metric"],
                metric_value=item["value"],
                tags_str="({})".format(", ".join("{}: {}".format(k, v) for k, v in list(item.get("tags", {}).items())))
                if item.get("tags")
                else "",
            )

    @list_route(methods=["post"], url_path="grafana/shield")
    @params_valid(serializer=GrafanaAlertSerilizer)
    def do_grafana_shield(self, request, params):
        """
        @api {post} /datamanage/dmonitor/alerts/grafana/shield/ 处理Grafana告警
        @apiName dmonitor_do_grafana_shield
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {String} title 告警标题
        @apiParam {String} ruleName 告警规则名称
        @apiParam {String} ruleUrl 告警规则路径
        @apiParam {String} state 告警状态
        @apiParam {String} [message] 告警信息
        @apiParam {Dict} [evalMatches] 告警参数
        @apiParam {Int} shield_time 屏蔽时间
        @apiParam {String} shield_params 屏蔽参数

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "00",
                }
        """
        shield_time = int(request.GET.get("shield_time", 60))
        shield_params = json.loads(request.GET.get("shield_params") or "{}")

        title = params.get("title", "")
        start_time = tznow()
        end_time = start_time + datetime.timedelta(seconds=shield_time)

        AlertShield.objects.create(
            start_time=start_time,
            end_time=end_time,
            reason=title,
            alert_code=shield_params.get("alert_code"),
            alert_level=shield_params.get("alert_level"),
            alert_config_id=shield_params.get("alert_config_id"),
            receivers=shield_params.get("receivers"),
            notify_ways=shield_params.get("notify_ways"),
            dimensions=shield_params.get("dimensions"),
            description=json.dumps(params),
        )

        return Response("ok")


class AlertShieldViewSet(APIModelViewSet):
    """
    @api {post} /datamanage/dmonitor/alert_shields/ 创建告警屏蔽规则
    @apiVersion 1.0.0
    @apiGroup DmonitorShield
    @apiName dmonitor_shield_alert

    @apiParam {datetime} [start_time=now] 屏蔽开始时间
    @apiParam {datetime} end_time 屏蔽结束时间
    @apiParam {String} reason 屏蔽原因
    @apiParam {String} [alert_code] 屏蔽策略
    @apiParam {String} [alert_level] 屏蔽级别
    @apiParam {Int} [alert_config_id] 屏蔽告警配置ID
    @apiParam {List} [receivers] 屏蔽接收人
    @apiParam {List} [notify_ways] 屏蔽通知方式
    @apiParam {Dict} [dimensions] 屏蔽维度
    @apiParam {String} [description] 描述

    @apiParamExample {json} 参数样例:
        {
            "start_time": "2019-07-18 12:00:00",
            "end_time": "2019-07-18 18:00:00",
            "reason": "数据平台变更"
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {}
        }
    """

    model = AlertShield
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = AlertShieldSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.all()

    @list_route(methods=["get"], url_path="in_effect")
    def in_effect(self, request):
        queryset = self.filter_queryset(self.get_queryset())
        nowtime = tznow()
        queryset = queryset.filter(active=True, start_time__lte=nowtime, end_time__gte=nowtime)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def perform_create(self, serializer):
        serializer.save(created_by=get_request_username())

    def perform_update(self, serializer):
        serializer.save(created_by=get_request_username())


class AlertDetailViewSet(BaseMixin, APIModelViewSet):
    """
    @api {get} /datamanage/dmonitor/alert_details/:id/ 查询告警详情
    @apiName dmonitor_retrivev_alert_detail
    @apiGroup DmonitorAlert
    @apiVersion 1.0.0

    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": "ok",
                "message": "",
                "code": "00",
            }
    """

    model = AlertDetail
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    filter_fields = ("alert_code", "alert_status", "alert_send_status")
    pagination_class = DataPageNumberPagination
    serializer_class = AlertDetailSerializer
    ordering_fields = ("id", "alert_time")
    ordering = ("-alert_time",)

    def get_queryset(self):
        return self.model.objects.all()

    def retrieve(self, request, *args, **kwargs):
        """
        @api {get} /datamanage/dmonitor/alert_details/:id/ 查询我的告警列表
        @apiName dmonitor_retrieve_alert_detail
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "alert_target_type": "rawdata",
                        "alert_target_id": "391",
                        "alert_target_alias": "体验登录日志",
                        "alert_type": "task_monitor",
                        "alert_code": "no_data",
                        "alert_level": "warning",
                        "project_id": 400,
                        "project_alias: "TEST_PROJECT",
                        "bk_biz_id": 591,
                        "bk_biz_name": "测试业务",
                        "message": "xxxxx",
                        "message_en": "xxxsxxx",
                        "full_message": "xxxxxx",
                        "full_message_en": "xxxxxxx",
                        "alert_status": "alerting",  // "alerting", "converged", "recovered"
                        "alert_send_status": "success",  // "init", "success", "error"
                        "alert_send_error": null,
                        "alert_time": "2019-02-25 00:00:00",
                        "alert_send_time": "2019-02-25 00:01:00",
                        "alert_recover_time": null,
                        "description": "xxxx"
                    },
                    "message": "",
                    "code": "00",
                }
        """
        instance = self.get_object()
        serializer = self.get_serializer(instance)

        alert_detail = self.attach_dimension_information([serializer.data])[0]
        if alert_detail.get("alert_target_type") == "dataflow":
            self.check_permission(
                "flow.query_metrics",
                alert_detail.get("alert_target_id"),
                get_request_username(),
            )
        elif alert_detail.get("alert_target_type") == "rawdata":
            self.check_permission(
                "raw_data.query_data",
                alert_detail.get("alert_target_id"),
                get_request_username(),
            )
        return Response(alert_detail)

    @list_route(methods=["GET"], url_path="mine")
    @params_valid(serializer=AlertDetailMineSerializer)
    def mine(self, request, params, *args, **kwargs):
        """
        @api {get} /datamanage/dmonitor/alert_details/mine/ 查询我的告警列表
        @apiName dmonitor_mine_alert_details
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {Int} [start_time] 开始时间(默认为当天0点)
        @apiParam {Int} [end_time] 结束时间(默认为当前时间)
        @apiParam {String} [alert_type] 告警类型, data_monitor, task_monitor
        @apiParam {String} [alert_target] 告警对象
        @apiParam {String} [alert_level] 告警级别, warning, danger
        @apiParam {List} [alert_status] 告警策略ID列表

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [
                        {
                            "alert_target_type": "rawdata",
                            "alert_target_id": "391",
                            "alert_target_alias": "体验登录日志",
                            "alert_type": "task_monitor",
                            "alert_code": "no_data",
                            "alert_level": "warning",
                            "project_id": 400,
                            "project_alias: "TEST_PROJECT",
                            "bk_biz_id": 591,
                            "bk_biz_name": "测试业务",
                            "message": "xxxxx",
                            "message_en": "xxxsxxx",
                            "full_message": "xxxxxx",
                            "full_message_en": "xxxxxxx",
                            "alert_status": "alerting",  // "alerting", "converged", "recovered"
                            "alert_send_status": "success",  // "init", "success", "error"
                            "alert_send_error": null,
                            "alert_time": "2019-02-25 00:00:00",
                            "alert_send_time": "2019-02-25 00:01:00",
                            "alert_recover_time": null,
                            "description": "xxxx"
                        }
                    ],
                    "message": "",
                    "code": "00",
                }
        """
        now = tznow()
        default_start_time = datetime.datetime(year=now.year, month=now.month, day=now.day)
        default_end_time = now
        start_time = timetostr(params.get("start_time", default_start_time))
        end_time = timetostr(params.get("end_time", default_end_time))

        bk_username = get_request_username()
        queryset = AlertDetail.objects.all()
        queryset = queryset.filter(
            alert_time__gte=start_time,
            alert_time__lt=end_time,
            receivers__contains=bk_username,
        ).order_by("-alert_time")
        if params.get("alert_type"):
            queryset = queryset.filter(alert_type=params.get("alert_type"))
        if params.get("alert_level"):
            queryset = queryset.filter(alert_level=params.get("alert_level"))
        if params.get("alert_status"):
            queryset = queryset.filter(alert_status__in=params.get("alert_status", []))
        if params.get("alert_target"):
            queryset = queryset.filter(flow_id=params.get("alert_target"))

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(self.attach_dimension_information(serializer.data))

        serializer = self.get_serializer(queryset, many=True)
        return Response(self.attach_dimension_information(serializer.data))

    def attach_dimension_information(self, alert_data):
        from gevent import monkey

        monkey.patch_all()

        # 获取dataflow和rawdata的信息
        rawdata_targets, dataflow_targets = set(), set()
        for alert in alert_data:
            flow_id = alert.get("flow_id")
            if alert.get("flow_id").startswith("rawdata"):
                rawdata_targets.add(flow_id.strip("rawdata"))
                alert["alert_target_id"] = flow_id.strip("rawdata")
                alert["alert_target_type"] = "rawdata"
            else:
                dataflow_targets.add(flow_id)
                alert["alert_target_id"] = flow_id
                alert["alert_target_type"] = "dataflow"

        raw_data_infos, dataflow_infos, biz_infos, project_infos = {}, {}, {}, {}
        bk_username = get_request_username()

        task_list = [gevent.spawn(self.fetch_biz_infos, biz_infos)]
        if len(rawdata_targets) > 0:
            task_list.append(gevent.spawn(self.fetch_rawdata_infos, raw_data_infos, rawdata_targets))
        if len(dataflow_targets) > 0:
            task_list.append(
                gevent.spawn(
                    self.fetch_dataflow_infos,
                    dataflow_infos,
                    dataflow_targets,
                    bk_username,
                )
            )
        gevent.joinall(task_list)

        # 获取项目信息
        project_ids = set()
        for dataflow in list(dataflow_infos.values()):
            project_ids.add(dataflow.get("project_id"))
        self.fetch_project_infos(project_infos, project_infos)

        # 补全项目，业务，dataflow和rawdata信息到告警中
        for alert in alert_data:
            if alert["alert_target_type"] == "rawdata":
                raw_data_info = raw_data_infos.get(alert.get("alert_target_id"), {})
                bk_biz_id = raw_data_info.get("bk_biz_id")
                alert["alert_target_alias"] = raw_data_info.get("raw_data_alias")
                alert["bk_biz_id"] = bk_biz_id
                alert["bk_biz_name"] = biz_infos.get(bk_biz_id, {}).get("bk_biz_name")
            elif alert["alert_target_type"] == "dataflow":
                dataflow_info = dataflow_infos.get(alert.get("alert_target_id"), {})
                project_id = dataflow_info.get("project_id")
                alert["alert_target_alias"] = dataflow_info.get("flow_name")
                alert["project_id"] = project_id
                alert["project_alias"] = project_infos.get(project_id, {}).get("project_name")

        return alert_data

    def prepare_alert_list_filter(self, params):
        # 准备过滤维度
        flow_id = params.get("flow_id")
        node_id = params.get("node_id")
        bk_biz_id = params.get("bk_biz_id")
        project_id = params.get("project_id")
        generate_type = params.get("generate_type")
        alert_config_ids = params.get("alert_config_ids", [])
        dimensions = params.get("dimensions") or {}
        if dimensions and not isinstance(dimensions, dict):
            dimensions = json.loads(dimensions)
        if "bk_biz_id" in dimensions:
            bk_biz_id = dimensions.get("bk_biz_id")
            del dimensions["bk_biz_id"]
        if "project_id" in dimensions:
            project_id = dimensions.get("project_id")
            del dimensions["project_id"]
        if "generate_type" in dimensions:
            generate_type = dimensions.get("generate_type")
            del dimensions["generate_type"]
        return (
            flow_id,
            node_id,
            bk_biz_id,
            project_id,
            generate_type,
            alert_config_ids,
            dimensions,
        )

    def prepare_alert_list_time(self, params):
        # 准备过滤时间
        now = tznow()
        default_start_time = datetime.datetime(year=now.year, month=now.month, day=now.day)
        default_end_time = now
        start_time = timetostr(params.get("start_time", default_start_time))
        end_time = timetostr(params.get("end_time", default_end_time))
        return start_time, end_time

    def prepare_alert_list_queryset(
        self,
        start_time,
        end_time,
        flow_id,
        node_id,
        bk_biz_id,
        project_id,
        generate_type,
        alert_config_ids,
    ):
        queryset = self.filter_queryset(self.get_queryset())
        queryset = (
            queryset.filter(alert_time__gte=start_time, alert_time__lt=end_time)
            .exclude(alert_status="converged")
            .exclude(alert_status="shielded")
            .order_by("-alert_time")
        )
        if flow_id:
            queryset = queryset.filter(flow_id=flow_id)
        if node_id:
            queryset = queryset.filter(node_id=node_id)
        if bk_biz_id:
            queryset = queryset.filter(bk_biz_id=bk_biz_id)
        if project_id:
            queryset = queryset.filter(project_id=project_id)
        if generate_type:
            queryset = queryset.filter(generate_type=generate_type)
        if alert_config_ids:
            queryset = queryset.filter(alert_config_id__in=alert_config_ids)
        return queryset

    @params_valid(serializer=AlertDetailListSerializer)
    def list(self, request, params, *args, **kwargs):
        """
        @api {get} /datamanage/dmonitor/alert_details/ 查询告警列表
        @apiName dmonitor_list_alert_details
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {Int} [start_time] 开始时间(默认为当天0点)
        @apiParam {Int} [end_time] 结束时间(默认为当前时间)
        @apiParam {String} [flow_id] FlowID(数据源的flow用rawdata{}的格式作为flow_id)
        @apiParam {String} [node_id] Flow节点的ID
        @apiParam {List} [alert_config_ids] 告警策略ID列表
        @apiParam {Json} [dimensions] 需要过滤的维度条件

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [
                        {
                            "message": "xxxxx",
                            "message_en": "xxxsxxx",
                            "full_message": "xxxxxx",
                            "full_message_en": "xxxxxxx",
                            "receivers": ["zhangshan", "lisi"],
                            "notify_ways": ["wechat", "phone"],
                            "alert_code": "no_data",
                            "monitor_config": {
                                "no_data_interval": 600,
                                "monitor_status": "on",
                            },
                            "dimensions": {
                                "bk_biz_id": 591,
                            },
                            "alert_status": "alerting",  // "alerting", "converged", "recovered"
                            "alert_send_status": "success",  // "init", "success", "error"
                            "alert_send_error": null,
                            "alert_time": "2019-02-25 00:00:00",
                            "alert_send_time": "2019-02-25 00:01:00",
                            "alert_recover_time": null,
                            "description": "xxxx"
                        }
                    ],
                    "message": "",
                    "code": "00",
                }
        """
        (
            flow_id,
            node_id,
            bk_biz_id,
            project_id,
            generate_type,
            alert_config_ids,
            dimensions,
        ) = self.prepare_alert_list_filter(
            params
        )  # noqa
        start_time, end_time = self.prepare_alert_list_time(params)

        queryset = self.prepare_alert_list_queryset(
            start_time,
            end_time,
            flow_id,
            node_id,
            bk_biz_id,
            project_id,
            generate_type,
            alert_config_ids,
        )

        page = self.paginate_queryset(queryset)
        # 带分页的情况下不允许按维度过滤
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer_data = self.get_serializer(queryset, many=True).data

        if dimensions:
            response_data = []
            for item in serializer_data:
                if self.check_dimension_match(dimensions, item["dimensions"]):
                    response_data.append(item)
            return Response(response_data)

        return Response(serializer_data)

    @list_route(methods=["GET"], url_path="summary")
    @params_valid(serializer=AlertDetailListSerializer)
    def summary(self, request, params):
        """
        @api {get} /datamanage/dmonitor/alert_details/summary/ 查询告警汇总信息
        @apiName dmonitor_list_alert_detail_summary
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {Int} [start_time] 开始时间(默认为24小时前)
        @apiParam {Int} [end_time] 结束时间(默认为当前时间)
        @apiParam {String} [flow_id] FlowID(数据源的flow用raw_data_id作为flow_id)
        @apiParam {List} [alert_config_ids] 告警策略ID列表
        @apiParam {Json} [dimensions] 需要过滤的维度条件
        @apiParam {String} [group] 分组维度

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "alert_count": 100,
                        "alert_levels": {
                            "warning": 60,
                            "danger": 40
                        },
                        "alert_codes": {
                            "task": 40,
                            "no_data": 10,
                            "data_trend": 10,
                            "data_time_delay": 10,
                            "process_time_delay": 10,
                            "data_drop": 10,
                            "data_interrupt": 10
                        },
                        "alert_types": {
                            "task_monitor": 40,
                            "data_monitor": 60
                        },
                        "groups": {
                            "591_rt1": {
                                "alert_count": 100,
                                "alert_levels": {
                                    "warning": 60,
                                    "danger": 40
                                },
                                "alert_codes": {
                                    "task": 40,
                                    "no_data": 10,
                                    "data_trend": 10,
                                    "data_time_delay": 10,
                                    "process_time_delay": 10,
                                    "data_drop": 10,
                                    "data_interrupt": 10
                                },
                                "alert_types": {
                                    "task_monitor": 40,
                                    "data_monitor": 60
                                }
                            }
                        }
                    },
                    "message": "",
                    "code": "00",
                }
        """
        (
            flow_id,
            node_id,
            bk_biz_id,
            project_id,
            generate_type,
            alert_config_ids,
            dimensions,
        ) = self.prepare_alert_list_filter(
            params
        )  # noqa
        start_time, end_time = self.prepare_alert_list_time(params)

        queryset = self.prepare_alert_list_queryset(
            start_time,
            end_time,
            flow_id,
            node_id,
            bk_biz_id,
            project_id,
            generate_type,
            alert_config_ids,
        )

        serializer_data = self.get_serializer(queryset, many=True).data

        if dimensions:
            response_data = []
            for item in serializer_data:
                if self.check_dimension_match(dimensions, item["dimensions"]):
                    response_data.append(item)
            serializer_data = response_data

        response = self.summary_alerts(serializer_data, params.get("group"))

        return Response(response)


class AlertNotifyWayViewSet(APIViewSet):
    """
    @api {get} /datamanage/dmonitor/notify_ways/ 查询告警通知方式列表
    @apiName dmonitor_list_notify_ways
    @apiGroup DmonitorAlert
    @apiVersion 1.0.0

    @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "data": [
                    {
                        "notify_way": "wechat",
                        "notify_way_name": "wechat",
                        "notify_way_alias": "微信",
                        "description": "",
                        "icon": "(base64 image)"
                    },
                    {
                        "notify_way": "sms",
                        "notify_way_name": "sms",
                        "notify_way_alias": "短信",
                        "description": "",
                        "icon": "(base64 image)"
                    }
                ],
                "message": "",
                "code": "00",
            }
    """

    def list(self, request):
        # 短期内通过映射进行兼容，后续部署脚本统一升级
        notify_ways = []
        try:
            res = CmsiApi.get_msg_type({"bk_username": "admin"}, raise_exception=True)
            for msg_type_info in res.data:
                if msg_type_info.get("is_active"):
                    msg_type = msg_type_info.get("type")
                    notify_ways.append(
                        {
                            "notify_way": msg_type,
                            "notify_way_name": msg_type,
                            "notify_way_alias": msg_type_info.get("label"),
                            "description": msg_type_info.get("description", ""),
                            "icon": msg_type_info.get("icon"),
                            "active": msg_type_info.get("is_active"),
                        }
                    )
        except Exception as e:
            logger.error("Can not get message type supportted by blueking, error: {error}".format(error=e))

        return Response(notify_ways)


class AlertTargetViewSet(BaseMixin, APIViewSet):
    @list_route(methods=["GET"], url_path="mine")
    @params_valid(serializer=AlertTargetMineSerializer)
    def mine(self, request, params, *args, **kwargs):
        """
        @api {get} /datamanage/dmonitor/alert_targets/mine/ 查询我的告警对象列表
        @apiName dmonitor_mine_alert_targets
        @apiGroup DmonitorAlert
        @apiVersion 1.0.0

        @apiParam {Int} [start_time] 开始时间(默认为当天0点)
        @apiParam {Int} [end_time] 结束时间(默认为当前时间)
        @apiParam {String} [alert_type] 告警类型, data_monitor, task_monitor
        @apiParam {String} [alert_level] 告警级别, warning, danger
        @apiParam {List} [alert_status] 告警策略ID列表

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [
                        {
                            "alert_target_type": "rawdata",
                            "alert_target_id": "391",
                            "alert_target_alias": "体验登录日志"
                        }
                    ],
                    "message": "",
                    "code": "00",
                }
        """
        base = params.get("base")
        flow_ids = []

        if base == "alert":
            flow_ids = self.get_flow_ids_by_alert(params)
        elif base == "alert_config":
            flow_ids = self.get_flow_ids_by_alert_config(params)
        return Response(self.get_alert_targets(flow_ids, params))

    def get_flow_ids_by_alert(self, params):
        now = tznow()
        default_start_time = datetime.datetime(year=now.year, month=now.month, day=now.day)
        default_end_time = now
        start_time = timetostr(params.get("start_time", default_start_time))
        end_time = timetostr(params.get("end_time", default_end_time))

        bk_username = get_request_username()
        queryset = AlertDetail.objects.all()
        queryset = queryset.filter(
            alert_time__gte=start_time,
            alert_time__lt=end_time,
            receivers__contains=bk_username,
        ).order_by("-alert_time")
        if params.get("alert_type"):
            queryset = queryset.filter(alert_type=params.get("alert_type"))
        if params.get("alert_level"):
            queryset = queryset.filter(alert_level=params.get("alert_level"))
        if params.get("alert_status"):
            queryset = queryset.filter(alert_status__in=params.get("alert_status", []))

        return [x.get("flow_id") for x in queryset.values("flow_id").distinct()]

    def get_flow_ids_by_alert_config(self, params):
        bk_username = get_request_username()
        from gevent import monkey

        monkey.patch_all()

        flow_ids = set()
        flows_by_biz, flows_by_project = set(), set()
        tasks = [
            gevent.spawn(
                self.get_flow_ids_by_biz,
                params.get("bk_biz_id"),
                bk_username,
                flows_by_biz,
            ),
            gevent.spawn(
                self.get_flow_ids_by_project,
                params.get("project_id"),
                bk_username,
                flows_by_project,
            ),
        ]
        gevent.joinall(tasks)

        if not params.get("project_id") and not params.get("bk_biz_id"):
            flow_ids = flows_by_biz | flows_by_project
        elif params.get("project_id"):
            flow_ids = flows_by_project
        elif params.get("bk_biz_id"):
            flow_ids = flows_by_biz

        return flow_ids

    def combine_flow_ids(self, flow_ids, temp_ids, filtered=False):
        if not filtered:
            return flow_ids | temp_ids
        else:
            return flow_ids & temp_ids

    def get_flow_ids_by_biz(self, bk_biz_id, bk_username, flow_ids):
        if bk_biz_id:
            bk_biz_ids = [bk_biz_id]
        else:
            bizs = self.get_bizs_by_username(bk_username)
            bk_biz_ids = [x.get("bk_biz_id") for x in bizs]

        try:
            sql = """
                SELECT id FROM access_raw_data WHERE bk_biz_id in ({bk_biz_ids})
            """.format(
                bk_biz_ids=",".join(["'{}'".format(x) for x in bk_biz_ids])
            )
            res = MetaApi.complex_search(
                {
                    "statement": sql,
                    "backend": "mysql",
                },
                raise_exception=True,
            )
            for item in res.data:
                flow_ids.add("rawdata%s" % str(item.get("id")))
        except Exception as e:
            logger.error("根据业务ID获取数据源ID列表失败, ERROR: %s" % e)

        return flow_ids

    def get_flow_ids_by_project(self, project_id, bk_username, flow_ids):
        if project_id:
            project_ids = [project_id]
        else:
            projects = self.get_projects_by_username(bk_username)
            project_ids = [x.get("project_id") for x in projects]

        try:
            res = DataflowApi.flows.list({"bk_username": bk_username, "project_id": project_ids})
            for item in res.data:
                flow_ids.add(str(item.get("flow_id")))
        except Exception as e:
            logger.error("根据项目ID获取dataflow列表失败, ERROR: %s" % e)

        return flow_ids

    def get_flow_ids_by_target_type(self, alert_target_type):
        try:
            flow_ids = DatamonitorAlertConfigRelation.objects.filter(alert_target_type=alert_target_type).values_list(
                "flow_id", flat=True
            )
        except Exception as e:
            flow_ids = []
            logger.error("根据告警对象类型获取flow列表, ERROR: %s" % e)

        return set(flow_ids)

    def get_flow_ids_by_received(self, bk_username):
        try:
            flow_ids = DatamonitorAlertConfigRelation.objects.filter(
                alert_config__receivers__icontains=bk_username
            ).values_list("flow_id", flat=True)
        except Exception as e:
            flow_ids = []
            logger.error("根据我接收的告警获取flow列表, ERROR: %s" % e)

        return set(flow_ids)

    def get_alert_targets(self, flow_ids, params=None):
        params = params or {}
        from gevent import monkey

        monkey.patch_all()

        targets = {}
        alert_target_type = params.get("alert_target_type")

        # 获取dataflow和rawdata的信息
        rawdata_targets, dataflow_targets = set(), set()
        for flow_id in flow_ids:
            if flow_id.startswith("rawdata") and alert_target_type != "dataflow":
                rawdata_targets.add(flow_id.strip("rawdata"))
                targets[flow_id] = {
                    "alert_target_id": flow_id.strip("rawdata"),
                    "alert_target_type": "rawdata",
                }
            elif (not flow_id.startswith("rawdata")) and alert_target_type != "rawdata":
                dataflow_targets.add(flow_id)
                targets[flow_id] = {
                    "alert_target_id": flow_id,
                    "alert_target_type": "dataflow",
                }

        raw_data_infos, dataflow_infos = {}, {}
        bk_username = get_request_username()

        gevent.joinall(
            [
                gevent.spawn(
                    self.fetch_dataflow_multiprocess,
                    dataflow_infos,
                    dataflow_targets,
                    bk_username,
                ),
                gevent.spawn(self.fetch_rawdata_infos, raw_data_infos, rawdata_targets),
            ]
        )

        # 补全项目，业务，dataflow和rawdata信息到告警中
        target_ids = list(targets.keys())
        bk_biz_id = params.get("bk_biz_id")
        project_id = params.get("project_id")

        for target_id in target_ids:
            alert_target = targets[target_id]
            if alert_target["alert_target_type"] == "rawdata":
                raw_data_info = raw_data_infos.get(alert_target.get("alert_target_id"), {})
                if bk_biz_id and raw_data_info.get("bk_biz_id") != bk_biz_id:
                    del targets[target_id]
                    continue
                alert_target["alert_target_alias"] = raw_data_info.get("raw_data_alias")
            elif alert_target["alert_target_type"] == "dataflow":
                dataflow_info = dataflow_infos.get(alert_target.get("alert_target_id"), {})
                if project_id and dataflow_info.get("project_id") != project_id:
                    del targets[target_id]
                    continue
                alert_target["alert_target_alias"] = dataflow_info.get("flow_name")

        return list(targets.values())
