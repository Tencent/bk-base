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


import copy
import datetime
import json

import gevent
from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor.constants import (
    DEFAULT_CONVERGENCE_CONFIG,
    DEFAULT_MONITOR_CONFIG,
    DEFAULT_TRIGGER_CONFIG,
)
from datamanage.lite.dmonitor.filters import AlertConfigsFilter
from datamanage.lite.dmonitor.mixins.base_mixins import BaseMixin
from datamanage.lite.dmonitor.models import (
    DatamonitorAlertConfig,
    DatamonitorAlertConfigRelation,
)
from datamanage.lite.dmonitor.serializers import (
    AlertConfigMineSerializer,
    AlertConfigSerializer,
    FlowAlertConfigCreateSerializer,
    FlowAlertConfigSerializer,
)
from datamanage.utils.api import DataflowApi, MetaApi
from datamanage.utils.drf import DataPageNumberPagination
from datamanage.utils.time_tools import tznow
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.response import Response

from common.decorators import list_route, params_valid, trace_gevent
from common.local import get_request, get_request_username
from common.log import logger
from common.meta.common import create_tag_to_target
from common.trace_patch import gevent_spawn_with_span
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet


class AlertConfigViewSet(BaseMixin, APIModelViewSet):
    """
    @api {get} /datamanage/dmonitor/alert_configs/ 获取告警策略配置列表
    @apiVersion 1.0.0
    @apiGroup DmonitorAlertConfig
    @apiName get_alert_config_list

    @apiParam {string} [generate_type] 告警策略生成类型
    @apiParam {int} [active] 告警总开关

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {
                    "id": 1,
                    "monitor_target": [
                        {
                            "target_type": "dataflow",
                            "flow_id": 123,
                            "node_id": 1  // 可选，无改维度时配置作用于整个dataflow
                        },
                        {
                            "target_type": "rawdata",
                            "raw_data_id": 123,
                            "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                        }
                    ],
                    "monitor_config": {
                        "no_data": {
                            "no_data_interval": 600, // 单位: 秒
                            "monitor_status": "on",
                        },
                        "data_trend": {
                            "diff_period": 1,  // 波动比较周期, 单位: 小时
                            "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                            "diff_unit": "percent",  // 波动单位, 可选percent和number
                            "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                            "monitor_status": "on",
                        },
                        "task": {
                            "batch_exception_status": ["failed"],   # 需监控的异常状态
                            "monitor_status": "on"
                        },
                        "data_loss": {
                            "monitor_status": "off",
                        },
                        "data_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "process_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "delay_trend": {
                            "continued_increase_time": 600,  # 持续增长时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "data_drop": {
                            "drop_rate": 30,   # 无效数据比例(%s)
                            "monitor_status": "on"
                        },
                        "data_interrupt": {
                            "monitor_status": "on"
                        }
                    },
                    "notify_config": ["weixin", "work-weixin"],
                    "trigger_config": {
                        "duration": 1,  # 触发时间范围, 单位: 分钟
                        "alert_threshold": 1  # 触发次数
                    },
                    "convergence_config": {
                        "mask_time": 60  // 单位: 分钟
                    },
                    "extra": {},
                    "receivers": [
                        {
                            "receiver_type": "user",
                            "username": "zhangshan"
                        },
                        {
                            "receiver_type": "role",
                            "role_id": 11,
                            "scope_id": 111
                        }
                    ],
                    "active": true,
                    "generate_type": "user"  // 可选值user/admin/system
                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /datamanage/dmonitor/alert_configs/:id/ 获取告警策略配置详情
    @apiVersion 1.0.0
    @apiGroup DmonitorAlertConfig
    @apiName get_alert_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "id": 1,
                "monitor_target": [
                    {
                        "target_type": "dataflow",
                        "flow_id": 123,
                        "node_id": 1  // 可选，无改维度时配置作用于整个dataflow
                    },
                    {
                        "target_type": "rawdata",
                        "raw_data_id": 123,
                        "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                    }
                ],
                "monitor_config": {
                    "no_data": {
                        "no_data_interval": 600, // 单位: 秒
                        "monitor_status": "on",
                    },
                    "data_trend": {
                        "diff_period": 1,  // 波动比较周期, 单位: 小时
                        "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                        "diff_unit": "percent",  // 波动单位, 可选percent和number
                        "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                        "monitor_status": "on",
                    },
                    "task": {
                        "batch_exception_status": ["failed"],   # 需监控的异常状态
                        "monitor_status": "on"
                    },
                    "data_loss": {
                        "monitor_status": "off",
                    },
                    "data_time_delay": {
                        "delay_time": 300,  # 延迟时间, 单位: 秒
                        "lasted_time": 600,  # 持续时间, 单位: 秒
                        "monitor_status": "on"
                    },
                    "process_time_delay": {
                        "delay_time": 300,  # 延迟时间, 单位: 秒
                        "lasted_time": 600,  # 持续时间, 单位: 秒
                        "monitor_status": "on"
                    },
                    "delay_trend": {
                        "continued_increase_time": 600,  # 持续增长时间, 单位: 秒
                        "monitor_status": "on"
                    },
                    "data_drop": {
                        "drop_rate": 30,   # 无效数据比例(%s)
                        "monitor_status": "on"
                    },
                    "data_interrupt": {
                        "monitor_status": "on"
                    }
                },
                "notify_config": ["weixin", "work-weixin"],
                "trigger_config": {
                    "duration": 1,  # 触发时间范围, 单位: 分钟
                    "alert_threshold": 1  # 触发次数
                },
                "convergence_config": {
                    "mask_time": 60  // 单位: 分钟
                },
                "extra": {},
                "receivers": [
                    {
                        "receiver_type": "user",
                        "username": "zhangshan"
                    },
                    {
                        "receiver_type": "role",
                        "role_id": 11,
                        "scope_id": 111
                    }
                ],
                "active": true,
                "generate_type": "user"  // 可选值user/admin/system
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /datamanage/dmonitor/alert_configs/ 创建告警策略配置配置
    @apiVersion 1.0.0
    @apiGroup DmonitorAlertConfig
    @apiName create_alert_config

    @apiParam {json} monitor_target 监控对象
    @apiParam {json} monitor_config 监控策略配置
    @apiParam {json} notify_config 通知方式配置
    @apiParam {json} trigger_config 触发方式配置
    @apiParam {json} convergence_config 收敛方式配置
    @apiParam {json} receivers 告警接收人列表
    @apiParam {bool} active 告警总开关
    @apiParam {string="user","admin","system"} generate_type 监控配置生成类型
    @apiParam {json} [extra] 其他配置

    @apiParamExample {json} 参数样例:
        {
            "monitor_target": [
                {
                    "target_type": "dataflow",
                    "flow_id": 123,
                    "node_id": 1  // 可选，无改维度时配置作用于整个dataflow
                },
                {
                    "target_type": "rawdata",
                    "raw_data_id": 123,
                    "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                }
            ],
            "monitor_config": {
                "no_data": {
                    "no_data_interval": 600, // 单位: 秒
                    "monitor_status": "on",
                },
                "data_trend": {
                    "diff_period": 1,  // 波动比较周期, 单位: 小时
                    "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                    "diff_unit": "percent",  // 波动单位, 可选percent和number
                    "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                    "monitor_status": "on",
                },
                "task": {
                    "monitor_status": "on",
                    "batch_exception_status": ["failed"]   # 需监控的异常状态
                },
                "data_loss": {
                    "monitor_status": "off",
                },
                "data_time_delay": {
                    "delay_time": 300,  # 延迟时间, 单位: 秒
                    "lasted_time": 600,  # 持续时间, 单位: 秒
                    "monitor_status": "on"
                },
                "process_time_delay": {
                    "delay_time": 300,  # 延迟时间, 单位: 秒
                    "lasted_time": 600,  # 持续时间, 单位: 秒
                    "monitor_status": "on"
                },
                "delay_trend": {
                    "continued_increase_time": 600,  # 持续增长时间, 单位: 秒
                    "monitor_status": "on"
                },
                "data_drop": {
                    "drop_rate": 30,   # 无效数据比例(%s)
                    "monitor_status": "on"
                },
                "data_interrupt": {
                    "monitor_status": "on"
                }
            },
            "notify_config": ["weixin", "work-weixin"],
            "trigger_config": {
                "duration": 1,  # 触发时间范围, 单位: 分钟
                "alert_threshold": 1  # 触发次数
            },
            "convergence_config": {
                "mask_time": 60  // 单位: 分钟
            },
            "extra": {},
            "receivers": [
                {
                    "receiver_type": "user",
                    "username": "zhangshan"
                },
                {
                    "receiver_type": "role",
                    "role_id": 11,
                    "scope_id": 111
                }
            ],
            "active": true,
            "generate_type": "user"  // 可选值user/admin/system
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /datamanage/dmonitor/alert_configs/:id/ 修改部分告警策略配置配置
    @apiVersion 1.0.0
    @apiGroup DmonitorAlertConfig
    @apiName partial_update_alert_config

    @apiParam {json} [monitor_target] 监控对象
    @apiParam {json} [monitor_config] 监控策略配置
    @apiParam {json} [notify_config] 通知方式配置
    @apiParam {json} [trigger_config] 触发方式配置
    @apiParam {json} [convergence_config] 收敛方式配置
    @apiParam {json} [receivers] 告警接收人列表
    @apiParam {bool} [active] 告警总开关
    @apiParam {json} [extra] 其他配置

    @apiParamExample {json} 参数样例:
        {
            "monitor_target": [
                {
                    "target_type": "dataflow",
                    "flow_id": 123,
                    "node_id": 1  // 可选，无改维度时配置作用于整个dataflow
                },
                {
                    "target_type": "rawdata",
                    "raw_data_id": 123,
                    "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                }
            ],
            "monitor_config": {
                "no_data": {
                    "no_data_interval": 600, // 单位: 秒
                    "monitor_status": "on",
                },
                "data_trend": {
                    "diff_period": 1,  // 波动比较周期, 单位: 小时
                    "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                    "diff_unit": "percent",  // 波动单位, 可选percent和number
                    "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                    "monitor_status": "on",
                },
                "task": {
                    "monitor_status": "on",
                    "batch_exception_status": ["failed"]   # 需监控的异常状态
                },
                "data_loss": {
                    "monitor_status": "off",
                },
                "data_time_delay": {
                    "delay_time": 300,  # 延迟时间, 单位: 秒
                    "lasted_time": 600,  # 持续时间, 单位: 秒
                    "monitor_status": "on"
                },
                "process_time_delay": {
                    "delay_time": 300,  # 延迟时间, 单位: 秒
                    "lasted_time": 600,  # 持续时间, 单位: 秒
                    "monitor_status": "on"
                },
                "delay_trend": {
                    "continued_increase_time": 600,  # 持续增长时间, 单位: 秒
                    "monitor_status": "on"
                },
                "data_drop": {
                    "drop_rate": 30,   # 无效数据比例(%s)
                    "monitor_status": "on"
                },
                "data_interrupt": {
                    "monitor_status": "on"
                }
            },
            "notify_config": ["weixin", "work-weixin"],
            "trigger_config": {
                "duration": 1,  # 触发时间范围, 单位: 分钟
                "alert_threshold": 1  # 触发次数
            },
            "convergence_config": {
                "mask_time": 60  // 单位: 分钟
            },
            "extra": {},
            "receivers": [
                {
                    "receiver_type": "user",
                    "username": "zhangshan"
                },
                {
                    "receiver_type": "role",
                    "role_id": 11,
                    "scope_id": 111
                }
            ],
            "active": true
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /datamanage/dmonitor/alert_configs/:id/ 删除告警策略配置配置
    @apiVersion 1.0.0
    @apiGroup DmonitorAlertConfig
    @apiName delete_alert_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = DatamonitorAlertConfig
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    filter_class = AlertConfigsFilter
    pagination_class = DataPageNumberPagination
    serializer_class = AlertConfigSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.prefetch_related("datamonitoralertconfigrelation_set").all()

    def perform_create(self, serializer):
        request = get_request()
        request_data = json.loads(request.body)
        with auto_meta_sync(using="bkdata_basic"):
            instance = serializer.save(created_by=get_request_username())
        tags = request_data.get("tags", [])
        if not isinstance(tags, list):
            tags = [tags]

        with auto_meta_sync(using="bkdata_basic"):
            create_tag_to_target([("alert_config", instance.id)], tags)

    def perform_update(self, serializer):
        request = get_request()
        request_data = json.loads(request.body)
        with auto_meta_sync(using="bkdata_basic"):
            instance = serializer.save(updated_by=get_request_username())

            alert_config = self.attach_alert_config_information([serializer.data], True)[0]
            if alert_config.get("generate_type") == "user":
                if alert_config.get("alert_target_type") == "dataflow":
                    self.check_permission(
                        "flow.update",
                        alert_config.get("alert_target_id"),
                        get_request_username(),
                    )
                elif alert_config.get("alert_target_type") == "rawdata":
                    self.check_permission(
                        "raw_data.update",
                        alert_config.get("alert_target_id"),
                        get_request_username(),
                    )
            elif alert_config.get("generate_type") == "admin":
                self.check_permission("bkdata.admin", "*", get_request_username())

        tags = request_data.get("tags", [])
        if not isinstance(tags, list):
            tags = [tags]

        with auto_meta_sync(using="bkdata_basic"):
            create_tag_to_target([("alert_config", instance.id)], tags)

    def perform_destroy(self, instance):
        with auto_meta_sync(using="bkdata_basic"):
            DatamonitorAlertConfigRelation.objects.filter(alert_config=instance).delete()
            instance.delete()

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        alert_config = self.attach_alert_config_information([serializer.data], True)[0]
        if alert_config.get("generate_type") == "user":
            if alert_config.get("alert_target_type") == "dataflow":
                self.check_permission(
                    "flow.retrieve",
                    alert_config.get("alert_target_id"),
                    get_request_username(),
                )
            elif alert_config.get("alert_target_type") == "rawdata":
                self.check_permission(
                    "raw_data.update",
                    alert_config.get("alert_target_id"),
                    get_request_username(),
                )
        elif alert_config.get("generate_type") == "admin":
            self.check_permission("bkdata.admin", "*", get_request_username())
        return Response(alert_config)

    def list(self, request, *args, **kwargs):
        tags = request.GET.getlist("tags")

        queryset = self.filter_queryset(self.get_queryset())

        serializer = self.get_serializer(queryset, many=True)
        alert_configs = {str(item["id"]): item for item in serializer.data}

        result_alert_configs = []
        if tags:
            length = self.get_queryset().count()
            params = {
                "target_type": "alert_config",
                "tag_filter": json.dumps([{"k": "code", "func": "eq", "v": x} for x in tags]),
                "limit": length,
            }
            params = self.generate_meta_search_params(request, params)

            res = MetaApi.search_target_tag(params)
            if res.is_success():
                for alert_config_info in res.data.get("content", []):
                    target_id = str(alert_config_info.get("id"))
                    geog_area_tags = alert_config_info.get("tags", {}).get("manage", {}).get("geog_area", [])
                    if target_id in alert_configs:
                        alert_configs[target_id]["tags"] = {
                            "manage": {"geog_area": [{"code": x["code"], "alias": x["alias"]} for x in geog_area_tags]}
                        }
                        result_alert_configs.append(alert_configs[target_id])
        else:
            result_alert_configs = list(alert_configs.values())

        return Response(result_alert_configs)

    def generate_meta_search_params(self, request, params):
        active = request.GET.get("active")
        recent_updated = request.GET.get("recent_updated")

        if active is not None:
            if isinstance(active, str):
                if active == "True":
                    active = True
                else:
                    active = False
            elif bool(active) is True:
                active = True
            else:
                active = False

        if not recent_updated:
            params["target_filter"] = json.dumps([{"k": "active", "func": "eq", "v": active}])
        else:
            now = tznow()
            min_updated_time = now - datetime.timedelta(seconds=int(recent_updated))
            min_updated_time = min_updated_time.replace(microsecond=0)
            params["target_filter"] = json.dumps(
                [
                    {
                        "criterion": [
                            {"k": "active", "func": "eq", "v": active},
                            {
                                "k": "updated_at",
                                "func": "gt",
                                "v": min_updated_time.isoformat(),
                            },
                        ],
                        "condition": "and",
                    }
                ]
            )
        return params

    @list_route(methods=["GET"], url_path="mine")
    @params_valid(serializer=AlertConfigMineSerializer)
    def mine(self, request, params, *args, **kwargs):
        """
        @api {get} /datamanage/dmonitor/alert_configs/mine/ 查询我的告警配置列表
        @apiName dmonitor_mine_alert_configs
        @apiGroup DmonitorAlertConfig
        @apiVersion 1.0.0

        @apiParam {Int} [project_id] 项目ID
        @apiParam {Int} [bk_biz_id] 业务ID
        @apiParam {String} [alert_target] 告警对象
        @apiParam {String} [alert_target_type] 告警对象类型
        @apiParam {Boolean} [active] 是否启用
        @apiParam {String} [notify_way] 告警通知方式
        @apiParam {String="received","managed","configured"} [scope] 告警配置列表范围

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [
                        {
                            "alert_target_type": "rawdata",
                            "alert_target_id": "391",
                            "alert_target_alias": "体验登录日志",
                            "project_id": 400,
                            "project_alias: "TEST_PROJECT",
                            "bk_biz_id": 591,
                            "bk_biz_name": "测试业务",
                            "active": true,
                            "is_shielded": false,
                            "notify_ways": ["weixin"],
                            "active_alert_codes": [],
                            "monitor_config": {},
                            "alert_count": 14,
                            "updated_by": "zhangsan",
                            "updated_at": "2019-01-01 00:00:00"
                        }
                    ],
                    "message": "",
                    "code": "00",
                }
        """
        bk_username = get_request_username()
        from gevent import monkey

        monkey.patch_all()

        alert_config_ids = set()
        configs_by_biz, configs_by_project, configs_by_target, configs_by_type = (
            set(),
            set(),
            set(),
            set(),
        )
        tasks = [
            gevent.spawn(
                self.get_alert_config_ids_by_biz,
                params.get("bk_biz_id"),
                bk_username,
                configs_by_biz,
            ),
            gevent.spawn(
                self.get_alert_config_ids_by_project,
                params.get("project_id"),
                bk_username,
                configs_by_project,
            ),
        ]

        if params.get("alert_target"):
            tasks.append(
                gevent.spawn(
                    self.get_alert_config_ids_by_target,
                    params.get("alert_target"),
                    configs_by_target,
                )
            )

        if params.get("alert_target_type"):
            tasks.append(
                gevent.spawn(
                    self.get_alert_config_ids_by_target_type,
                    params.get("alert_target_type"),
                    configs_by_type,
                )
            )
        gevent.joinall(tasks)
        if not params.get("project_id") and not params.get("bk_biz_id"):
            alert_config_ids = configs_by_biz | configs_by_project
        elif params.get("project_id"):
            alert_config_ids = configs_by_project
        elif params.get("bk_biz_id"):
            alert_config_ids = configs_by_biz
        if params.get("alert_target"):
            alert_config_ids = alert_config_ids & configs_by_target
        if params.get("alert_target_type"):
            alert_config_ids = alert_config_ids & configs_by_type

        queryset = DatamonitorAlertConfig.objects.prefetch_related("datamonitoralertconfigrelation_set").filter(
            id__in=list(alert_config_ids), generate_type="user"
        )

        if "active" in params:
            queryset = queryset.filter(active=params.get("active", True))
        if params.get("notify_way"):
            queryset = queryset.filter(notify_config__icontains=params.get("notify_way"))
        if params.get("scope") == "received":
            receiver = get_request_username()
            queryset = queryset.filter(receivers__icontains=receiver)
        elif params.get("scope") == "configured":
            queryset = queryset.exclude(updated_by__isnull=True)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(self.attach_alert_config_information(serializer.data, True, True))

        serializer = self.get_serializer(queryset, many=True)
        return Response(self.attach_alert_config_information(serializer.data, True, True))

    def get_alert_config_ids_by_target(self, alert_target, alert_config_ids):
        if alert_target.startswith("rawdata"):
            alert_config_relations = DatamonitorAlertConfigRelation.objects.filter(
                flow_id=alert_target.strip("rawdata"), target_type="rawdata"
            )
        else:
            alert_config_relations = DatamonitorAlertConfigRelation.objects.filter(
                flow_id=alert_target, target_type="dataflow"
            )
        for alert_config_relation in alert_config_relations:
            alert_config_ids.add(alert_config_relation.alert_config_id)
        return alert_config_ids

    def get_alert_config_ids_by_target_type(self, alert_target_type, alert_config_ids):
        alert_config_relations = DatamonitorAlertConfigRelation.objects.filter(target_type=alert_target_type)
        for alert_config_relation in alert_config_relations:
            alert_config_ids.add(alert_config_relation.alert_config_id)
        return alert_config_ids

    def get_alert_config_ids_by_biz(self, bk_biz_id, bk_username, alert_config_ids):
        if bk_biz_id:
            bk_biz_ids = [bk_biz_id]
        else:
            bizs = self.get_bizs_by_username(bk_username)
            bk_biz_ids = [x.get("bk_biz_id") for x in bizs]

        try:
            sql = """
                SELECT id FROM access_raw_data WHERE bk_biz_id in ({bk_biz_ids})
            """.format(
                bk_biz_ids=",".join(["{}".format(x) for x in bk_biz_ids])
            )
            res = MetaApi.complex_search(
                {
                    "statement": sql,
                    "backend": "mysql",
                },
                raise_exception=True,
            )
            raw_data_ids = [x.get("id") for x in res.data]
        except Exception as e:
            raw_data_ids = []
            logger.error("根据业务ID获取数据源ID列表失败, ERROR: %s" % e)

        queryset = DatamonitorAlertConfigRelation.objects.filter(target_type="rawdata", flow_id__in=raw_data_ids)
        for item in queryset.values("alert_config_id"):
            alert_config_ids.add(item.get("alert_config_id"))
        return alert_config_ids

    def get_alert_config_ids_by_project(self, project_id, bk_username, alert_config_ids):
        if project_id:
            project_ids = [project_id]
        else:
            projects = self.get_projects_by_username(bk_username)
            project_ids = [x.get("project_id") for x in projects]

        try:
            res = DataflowApi.flows.list({"bk_username": bk_username, "project_id": project_ids})
            flow_ids = [str(x.get("flow_id")) for x in res.data]
        except Exception as e:
            flow_ids = []
            logger.error("根据项目ID获取dataflow列表失败, ERROR: %s" % e)

        queryset = DatamonitorAlertConfigRelation.objects.filter(target_type="dataflow", flow_id__in=flow_ids)
        for item in queryset.values("alert_config_id"):
            alert_config_ids.add(item.get("alert_config_id"))
        return alert_config_ids

    def attach_alert_config_information(self, alert_configs, with_project_and_biz=False, with_alert_shields=False):
        from gevent import monkey

        monkey.patch_all()

        # 获取dataflow和rawdata的信息
        rawdata_targets, dataflow_targets = set(), set()
        for alert_config in alert_configs:
            if alert_config.get("alert_target_type") == "rawdata":
                rawdata_targets.add(alert_config.get("alert_target_id"))
            elif alert_config.get("alert_target_type") == "dataflow":
                dataflow_targets.add(alert_config.get("alert_target_id"))

        raw_data_infos, dataflow_infos, biz_infos, project_infos = {}, {}, {}, {}
        alert_shields = []
        bk_username = get_request_username()

        # 并发获取告警配置相关信息
        task_list = []
        if len(rawdata_targets) > 0:
            task_list.append(gevent_spawn_with_span(self.fetch_rawdata_infos, raw_data_infos, rawdata_targets))
        if len(dataflow_targets) > 0:
            task_list.append(
                gevent_spawn_with_span(
                    self.fetch_dataflow_infos,
                    dataflow_infos,
                    dataflow_targets,
                    bk_username,
                )
            )
        if with_project_and_biz:
            task_list.extend(
                [
                    gevent_spawn_with_span(self.fetch_biz_infos, biz_infos),
                    gevent_spawn_with_span(self.get_project_info_by_dataflow, dataflow_infos, project_infos),
                ]
            )
        if with_alert_shields:
            task_list.append(gevent_spawn_with_span(self.fetch_alert_shields, alert_shields))
        gevent.joinall(task_list)

        # 补全项目，业务，dataflow和rawdata信息到告警配置中
        for alert_config in alert_configs:
            if alert_config.get("alert_target_type") == "rawdata":
                raw_data_info = raw_data_infos.get(alert_config.get("alert_target_id"), {})
                bk_biz_id = raw_data_info.get("bk_biz_id")
                alert_config["alert_target_alias"] = raw_data_info.get("raw_data_alias")
                alert_config["bk_biz_id"] = bk_biz_id
                if with_project_and_biz:
                    alert_config["bk_biz_name"] = biz_infos.get(bk_biz_id, {}).get("bk_biz_name")
            elif alert_config.get("alert_target_type") == "dataflow":
                dataflow_info = dataflow_infos.get(alert_config.get("alert_target_id"), {})
                project_id = dataflow_info.get("project_id")
                alert_config["alert_target_alias"] = dataflow_info.get("flow_name")
                alert_config["project_id"] = project_id
                if with_project_and_biz:
                    alert_config["project_alias"] = project_infos.get(project_id, {}).get("project_name")

            if with_alert_shields:
                alert_config["full_alert_shields"] = []
                alert_config["part_alert_shields"] = []
                for alert_shield in alert_shields:
                    if self.check_alert_config_full_shield(alert_config, alert_shield):
                        alert_config["full_alert_shields"].append(alert_shield)
                    if self.check_alert_config_part_shield(alert_config, alert_shield):
                        alert_config["part_alert_shields"].append(alert_shield)

        return alert_configs

    @trace_gevent()
    def get_project_info_by_dataflow(self, dataflow_infos, project_infos):
        project_ids = set()

        if len(dataflow_infos) == 0:
            gevent.sleep(0)

        # 获取项目信息
        for dataflow in list(dataflow_infos.values()):
            project_ids.add(dataflow.get("project_id"))
        self.fetch_project_infos(project_infos, project_infos)

    def check_alert_config_full_shield(self, alert_config, alert_shield):
        if alert_shield["alert_config_id"] and alert_shield["alert_config_id"] == alert_config["id"]:
            return True

    def check_alert_config_part_shield(self, alert_config, alert_shield):
        if "flow_id" in alert_shield["dimensions"] and alert_config["flow_id"] == alert_shield["dimensions"]["flow_id"]:
            return True

    @list_route(
        methods=["get", "delete"],
        url_path=r"(?P<target_type>dataflow|rawdata)/(?P<flow_id>\d+)",
    )
    @params_valid(serializer=FlowAlertConfigSerializer)
    def get_flow_alert_config(self, request, target_type, flow_id, params):
        """
        @api {get} /datamanage/dmonitor/alert_configs/(rawdata|dataflow)/:flow_id/ 【Flow】获取告警策略配置详情
        @apiVersion 1.0.0
        @apiGroup DmonitorAlertConfig
        @apiName get_flow_alert_config_detail

        @apiParam {string} bk_username 请求用户名
        @apiParam {string} [node_id] 数据流节点ID,df数据流用原node_id,rawdata数据流用data_set_id(raw_data_id/result_table_id)

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {
                    "id": 1,
                    "monitor_target": [
                        {
                            "target_type": "rawdata",
                            "raw_data_id": 123,
                            "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                        }
                    ],
                    "monitor_config": {
                        "no_data": {
                            "no_data_interval": 600, // 单位: 秒
                            "monitor_status": "on",
                        },
                        "data_trend": {
                            "diff_period": 1,  // 波动比较周期, 单位: 小时
                            "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                            "diff_unit": "percent",  // 波动单位, 可选percent和number
                            "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                            "monitor_status": "on",
                        },
                        "task": {
                            "batch_exception_status": ["failed"],   # 需监控的异常状态
                            "monitor_status": "on"
                        },
                        "data_loss": {
                            "monitor_status": "off",
                        },
                        "data_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "process_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "data_drop": {
                            "drop_rate": 30,   # 无效数据比例(%s)
                            "monitor_status": "on"
                        },
                        "data_interrupt": {
                            "monitor_status": "on"
                        }
                    },
                    "notify_config": ["weixin", "work-weixin"],
                    "receivers": [
                        {
                            "receiver_type": "user",
                            "username": "zhangshan"
                        }
                    ],
                    "active": true,
                    "generate_type": "user",  // 可选值user/admin/system
                    "extra": {}
                },
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1573061 监控配置不存在
        """
        """
        @api {delete} /datamanage/dmonitor/alert_configs/(rawdata|dataflow)/:flow_id/ 【Flow】删除告警策略配置详情
        @apiVersion 1.0.0
        @apiGroup DmonitorAlertConfig
        @apiName delete_flow_alert_config_detail

        @apiParam {string} bk_username 请求用户名
        @apiParam {string} [node_id] 数据流节点ID,df数据流用原node_id,rawdata数据流用data_set_id(raw_data_id/result_table_id)

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": [1, 2],
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        node_id = params.get("node_id", None)
        monitor_target = self.get_monitor_target(target_type, flow_id, node_id)

        if request.method == "GET":
            queryset = DatamonitorAlertConfigRelation.objects.select_related("alert_config").filter(
                target_type=target_type,
                alert_config_type="all_monitor",
                flow_id=flow_id,
                node_id=node_id,
            )
            alert_config = queryset[0].alert_config if queryset.count() > 0 else {}
            if not alert_config:
                geog_areas = self.get_geog_area_by_target_flow(target_type, flow_id)
                alert_config, relation = self.create_custom_alert_config(
                    monitor_target,
                    target_type,
                    "all_monitor",
                    list(DEFAULT_MONITOR_CONFIG.keys()),
                    flow_id,
                    node_id,
                    geog_areas,
                )

            alert_config_data = AlertConfigSerializer(alert_config).data
            response_data = self.attach_alert_config_information([alert_config_data], True)[0]
            if response_data.get("generate_type") == "user":
                if response_data.get("alert_target_type") == "dataflow":
                    self.check_permission(
                        "flow.retrieve",
                        response_data.get("alert_target_id"),
                        get_request_username(),
                    )
                elif response_data.get("alert_target_type") == "rawdata":
                    self.check_permission(
                        "raw_data.update",
                        response_data.get("alert_target_id"),
                        get_request_username(),
                    )
            elif response_data.get("generate_type") == "admin":
                self.check_permission("bkdata.admin", "*", get_request_username())

            return Response(response_data)
        else:
            alert_config_ids = []
            with auto_meta_sync(using="bkdata_basic"):
                queryset = DatamonitorAlertConfigRelation.objects.select_related("alert_config").filter(
                    target_type=target_type, flow_id=flow_id, node_id=node_id
                )
                for item in queryset:
                    alert_config_ids.append(item.alert_config.id)
                    item.alert_config.delete()
                queryset.delete()

            return Response(alert_config_ids)

    @list_route(methods=["post"], url_path=r"(?P<target_type>(dataflow|rawdata))")
    @params_valid(serializer=FlowAlertConfigCreateSerializer)
    def create_flow_alert_config(self, request, target_type, params):
        """
        @api {post} /datamanage/dmonitor/alert_configs/(rawdata|dataflow)/ 【Flow】创建告警策略配置详情
        @apiVersion 1.0.0
        @apiGroup DmonitorAlertConfig
        @apiName create_flow_alert_config_detail

        @apiParam {string} bk_username 请求用户名
        @apiParam {string} flow_id 数据流的ID, dataflow的数据流用原flow_id, rawdata的数据流用raw_data_id
        @apiParam {string} [node_id] 数据流节点ID,df数据流用原node_id,rawdata数据流用data_set_id(raw_data_id/result_table_id)

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "user1",
                "flow_id": "123"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {
                    "id": 1,
                    "monitor_target": [
                        {
                            "target_type": "rawdata",
                            "raw_data_id": 123,
                            "data_set_id": "591_clean"  // 可选，无该维度时配置作用于整个rawdata
                        }
                    ],
                    "monitor_config": {
                        "no_data": {
                            "no_data_interval": 600, // 单位: 秒
                            "monitor_status": "on",
                        },
                        "data_trend": {
                            "diff_period": 1,  // 波动比较周期, 单位: 小时
                            "diff_count": 1,  // 波动数值, 与diff_unit共同作用
                            "diff_unit": "percent",  // 波动单位, 可选percent和number
                            "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
                            "monitor_status": "on",
                        },
                        "task": {
                            "batch_exception_status": ["failed"],   # 需监控的异常状态
                            "monitor_status": "on"
                        },
                        "data_loss": {
                            "monitor_status": "off",
                        },
                        "data_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "process_time_delay": {
                            "delay_time": 300,  # 延迟时间, 单位: 秒
                            "lasted_time": 600,  # 持续时间, 单位: 秒
                            "monitor_status": "on"
                        },
                        "data_drop": {
                            "drop_rate": 30,   # 无效数据比例(%s)
                            "monitor_status": "on"
                        },
                        "data_interrupt": {
                            "monitor_status": "on"
                        }
                    },
                    "notify_config": ["weixin", "work-weixin"],
                    "receivers": [
                        {
                            "receiver_type": "user",
                            "username": "zhangshan"
                        }
                    ],
                    "active": true,
                    "generate_type": "user",  // 可选值user/admin/system
                    "extra": {}
                },
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1573060 监控配置已存在
        """
        flow_id = params["flow_id"]
        node_id = params.get("node_id", None)
        monitor_target = self.get_monitor_target(target_type, flow_id, node_id)
        geog_areas = self.get_geog_area_by_target_flow(target_type, flow_id)

        with auto_meta_sync(using="bkdata_basic"):
            if (
                DatamonitorAlertConfigRelation.objects.filter(
                    target_type=target_type,
                    flow_id=flow_id,
                    node_id=node_id,
                    alert_config_type="all_monitor",
                ).count()
                == 0
            ):

                alert_config, relation = self.create_custom_alert_config(
                    monitor_target,
                    target_type,
                    "all_monitor",
                    list(DEFAULT_MONITOR_CONFIG.keys()),
                    flow_id,
                    node_id,
                    geog_areas,
                )
            else:
                raise dm_errors.OnlyOneAlertConfigForFlowError()
        return Response(AlertConfigSerializer(alert_config).data)

    def get_monitor_target(self, target_type, flow_id, node_id):
        monitor_target = {"target_type": target_type}
        if target_type == "dataflow":
            monitor_target["flow_id"] = flow_id
            monitor_target["node_id"] = node_id
        elif target_type == "rawdata":
            monitor_target["raw_data_id"] = int(flow_id or 0)
            monitor_target["data_set_id"] = node_id
        return monitor_target

    def get_geog_area_by_target_flow(self, alert_target_type, flow_id):
        try:
            contents = []
            target_id, target_key, target_type = None, None, None
            if alert_target_type == "dataflow":
                flow_info = DataflowApi.flows.retrieve({"flow_id": flow_id}).data
                target_id = flow_info.get("project_id")
                target_key = "project_id"
                target_type = "project"
            elif alert_target_type == "rawdata":
                target_id = flow_id
                target_key = "id"
                target_type = "raw_data"

            contents = MetaApi.search_target_tag(
                {
                    "target_type": target_type,
                    "target_filter": json.dumps(
                        [
                            {
                                "criterion": [{"k": target_key, "func": "eq", "v": target_id}],
                                "condition": "OR",
                            }
                        ]
                    ),
                }
            ).data.get("content", [])

            for content_info in contents:
                geog_area_tags = content_info.get("tags", {}).get("manage", {}).get("geog_area", [])
                if str(target_id) == str(content_info.get(target_key)):
                    geog_area_tags = content_info.get("tags", {}).get("manage", {}).get("geog_area", [])
                    return [x["code"] for x in geog_area_tags]
        except Exception as e:
            logger.error(
                "Cant not get geog_area code of alert_config target({target}), error: {error}".format(
                    target=flow_id,
                    error=e,
                )
            )
            raise dm_errors.AlertTargetGeogAreaNotFoundError()

        return []

    def create_custom_alert_config(
        self,
        monitor_target,
        target_type,
        alert_config_type,
        alert_codes,
        flow_id,
        node_id=None,
        geog_areas=None,
    ):
        geog_areas = geog_areas or []
        monitor_configs = {}
        for alert_code in alert_codes:
            monitor_configs[alert_code] = copy.copy(DEFAULT_MONITOR_CONFIG.get(alert_code, {}))

        with auto_meta_sync(using="bkdata_basic"):
            alert_config = DatamonitorAlertConfig(
                monitor_target=json.dumps([monitor_target]),
                monitor_config=json.dumps(monitor_configs),
                notify_config="[]",
                trigger_config=json.dumps(DEFAULT_TRIGGER_CONFIG),
                convergence_config=json.dumps(DEFAULT_CONVERGENCE_CONFIG),
                receivers="[]",
                generate_type="user",
                extra="{}",
                active=False,
                created_by=get_request_username(),
            )
            alert_config.save()

            relation = DatamonitorAlertConfigRelation(
                target_type=target_type,
                flow_id=flow_id,
                node_id=node_id,
                alert_config=alert_config,
                alert_config_type=alert_config_type,
            )
            relation.save()

        with auto_meta_sync(using="bkdata_basic"):
            create_tag_to_target([("alert_config", alert_config.id)], geog_areas)

        return alert_config, relation
