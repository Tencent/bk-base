# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.contrib.auth.models import Group
from django.http import JsonResponse
from django.utils import timezone
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from app_control.models import SERVICE_MODUELS, FunctionController
from apps import exceptions
from apps.api import AuthApi, BKPAASApi
from apps.common.views import list_route
from apps.exceptions import DataError
from apps.generic import APIViewSet
from apps.utils.time_handler import get_active_timezone_offset
from common.log import logger
from config import domains as DOMAINS
from config import funcs as FUNCS


class ToolsViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="check_guide")
    def check_guide(self, request):
        """
        @api {get} /tools/check_guide/  是否完成指定教程
        @apiName check_guide
        @apiGroup Tools
        @apiParam {String} guide 教程名称，比如 new_dataflow
        @apiSuccess {Boolean} data 表示用户是否完成过指定教程，True 表示完成，False 表示未完成
        """
        db_guide = "guide.%s" % request.query_params["guide"]

        return Response(db_guide in request.user.groups.values_list("name", flat=True))

    @list_route(methods=["post"], url_path="end_guide")
    def end_guide(self, request):
        """
        @api {post} /tools/end_guide/  确认完成指定教程
        @apiName end_guide
        @apiGroup Tools
        @apiParam {String} guide 教程名称，可选值有 new_dataflow，platform_intro，dataflow_intro
        """
        support_guides = ["new_dataflow", "platform_intro", "dataflow_intro"]
        guide = request.data["guide"]

        if guide not in support_guides:
            raise exceptions.FormError(_("不合法教程，目前仅支持（%s）") % ", ".join(support_guides))

        db_guide = "guide.%s" % guide
        guide_group, created = Group.objects.get_or_create(name=db_guide)

        guide_group.user_set.add(request.user)
        return Response(True)

    @list_route(methods=["get"], url_path="get_system_time")
    def get_system_time(self, request):
        """
        @api {post} /tools/get_system_time/  获取系统时间
        @apiName get_system_time
        @apiGroup Tools
        @apiSuccessExample
        {
            "utc_time": "2018-02-26 08:39:07 (+0000) UTC",
            "user_time": "2018-02-26 17:39:07 (+0900) Asia/Tokyo"
        }
        """
        user_time = str(timezone.now().astimezone(timezone.get_current_timezone()))[:19]
        utc_time = str(timezone.now())[:19]
        return Response(
            {
                "user_time": "{} ({}) {}".format(
                    user_time, get_active_timezone_offset(), timezone.get_current_timezone()
                ),
                "utc_time": "{} (+0000) UTC".format(utc_time),
            }
        )

    @list_route(methods=["get"], url_path="list_bk_app")
    def list_bk_app(self, request):
        """
        @api {get} /tools/list_bk_app/ 获取蓝鲸APP列表
        @apiName list_bk_app
        @apiGroup Tools
        @apiSuccessExample
            [
                {
                    "app_code": "data",
                    "app_name": "蓝鲸基础计算平台"
                },
                {
                    "app_code": "log_search",
                    "app_name": "日志检索"
                },
                {
                    "app_code": "bk_monitor",
                    "app_name": "蓝鲸监控平台"
                }
            ]
        """
        apps = BKPAASApi.get_app_info()
        return Response(apps)

    @list_route(methods=["get"], url_path="get_user_info")
    def get_user_info(self, request):
        """
        @api {get} /tools/get_user_info/ 获取用户信息
        @apiName get_user_info
        @apiGroup Tools
        @apiSuccessExample
            {
                username: "admin"
            }
        """
        username = request.user.username
        try:
            is_admin = AuthApi.check_user_perm({"user_id": username, "action_id": "bkdata.admin", "object_id": "*"})
        except DataError:
            is_admin = False

        return Response({"username": username, "is_admin": is_admin})

    @list_route(methods=["get"], url_path="get_env_settings")
    def get_env_settings(self, request):
        """
        @api {get} /tools/get_env_settings/ APP端配置信息
        @apiName get_env_settings
        @apiGroup Tools
        @apiSuccessExample
            {
                // 后台环境变量
                'env': {
                    'BK_PAAS_HOST': 'xxxx'
                },
                // 后台功能开关，是否显示相关功能
                'functions': {
                    'MODELFLOW_FUNC': False,
                }
            }
        """
        return Response(
            {
                "env": {
                    "MAPLE_APIGATEWAY_ROOT": DOMAINS.DATAQUERY_APIGATEWAY_ROOT,
                },
                "functions": {"MODELFLOW_FUNC": FUNCS.MODELFLOW_FUNC},
            }
        )

    @list_route(methods=["get"], url_path="get_functions")
    def get_functions(self, request):
        """
        @api {get} /tools/get_functions/ APP端功能开关
        @apiName get_functions
        @apiGroup Tools
        @apiSuccessExample
            [
                {
                    "status": "disabled",
                    "operation_id": "batch_week_task",
                    "operation_name": "batch_week_task"
                },
            ]
        """
        # 后台已部署的服务列表
        try:
            data = AuthApi.list_operation_configs()
            exist_modules = [
                d["operation_id"] for d in data if d["operation_id"] in SERVICE_MODUELS and d["status"] == "active"
            ]
        except Exception as err:
            logger.exception("[GetFunctions] Fail to get backend service list, {}".format(err))
            data = ["datahub"]  # noqa

        functions = []
        for fc in FunctionController.objects.all():

            # 检查是否开放
            if fc.enabled and not fc.has_developer():
                status = "active"
            elif fc.enabled and fc.has_developer() and request.user.username in fc.get_developers():
                status = "active"
            else:
                status = "disabled"

            # 检查依赖服务是否部署
            if fc.service_module and fc.service_module not in exist_modules:
                status = "disabled"

            functions.append({"status": status, "operation_id": fc.func_code, "operation_name": fc.func_name})

        return JsonResponse({"result": True, "data": functions, "code": "0000", "message": "OK"})  # 前端需要特殊处理，所以定制化该状态码
