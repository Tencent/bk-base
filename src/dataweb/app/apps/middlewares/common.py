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
import json
import os
import traceback

from blueapps.core.exceptions.base import BlueException

from apps.common.log import logger

try:
    from greenlet import getcurrent as get_ident
except ImportError:
    from _thread import get_ident  # noqa

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.utils.deprecation import MiddlewareMixin
from django.utils.translation import ugettext as _

from apps.exceptions import DataError


class CommonMid(MiddlewareMixin):
    """
    公共中间件，统一处理逻辑
    """

    def process_view(self, request, view_func, view_args, view_kwargs):
        # SAAS 部署包必须部署正式环境，此处仅企业版生效
        if settings.RUN_MODE == "TEST":
            test_switch = os.environ.get("BKAPP_ALLOW_TEST", False)
            if not test_switch:
                return HttpResponse(_("S-mart应用仅支持正式环境部署！"))

        # 如果有覆盖方法，则以覆盖方法为准
        if "HTTP_X_HTTP_METHOD_OVERRIDE" in request.META:
            override_method = request.META["HTTP_X_HTTP_METHOD_OVERRIDE"].upper()
            request.method = override_method

        return None

    def process_exception(self, request, exception):
        """
        app后台错误统一处理
        """

        # 处理 Data APP 自定义异常
        if isinstance(exception, DataError):
            _msg = _("【APP 自定义异常】{message}, code={code}, args={args}").format(
                message=exception.message, code=exception.code, args=exception.args, data=exception.data
            )
            logger.warning(_msg)
            return JsonResponse(
                {
                    "code": exception.code,
                    "message": exception.message,
                    "data": exception.data,
                    "result": False,
                    "errors": exception.errors,
                }
            )

        # 用户自我感知的异常抛出
        if isinstance(exception, BlueException):
            logger.warning(
                "捕获主动抛出异常, 具体异常堆栈->[{}] status_code->[{}] & client_message->[{}] & args->[{}] ".format(
                    traceback.format_exc(), exception.error_code, exception.message, exception.args
                )
            )

            response = JsonResponse(
                {"code": exception.error_code, "message": exception.message, "data": "", "result": False}
            )

            response.status_code = exception.error_code / 100
            return response

        # 用户未主动捕获的异常
        logger.error(
            "捕获未处理异常,异常具体堆栈->[{}], 请求URL->[{}], 请求方法->[{}] 请求参数->[{}]".format(
                traceback.format_exc(), request.path, request.method, json.dumps(getattr(request, request.method, None))
            )
        )

        # 判断是否在debug模式中,
        # 在这里判断是防止阻止了用户原本主动抛出的异常
        if settings.DEBUG:
            return None

        response = JsonResponse({"code": 50000, "message": "系统异常,请联系管理员处理", "data": "", "result": False})
        response.status_code = 500

        return response
