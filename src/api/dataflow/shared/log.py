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

import logging
import sys

from common.auth.exceptions import BaseAuthError
from common.base_utils import generate_request_id, make_resp_dict
from common.django_utils import JsonResponse
from common.exceptions import BaseAPIError
from common.local import get_local_param
from common.log import ErrorCodeLogger, logger, sys_logger
from django.views import debug
from rest_framework import exceptions

from .errorcodes import ErrorCode

LOCAL_LOGGER_KEY = "dataflow_logger"

stream_logger = ErrorCodeLogger(logging.getLogger("stream"))
batch_logger = ErrorCodeLogger(logging.getLogger("batch"))
flow_logger = ErrorCodeLogger(logging.getLogger("flow"))
component_logger = ErrorCodeLogger(logging.getLogger("component"))
udf_logger = ErrorCodeLogger(logging.getLogger("udf"))
modeling_logger = ErrorCodeLogger(logging.getLogger("modeling"))
uc_logger = ErrorCodeLogger(logging.getLogger("uc"))

for func in ["error", "info", "warning", "debug", "critical", "exception"]:
    # 对 pizza 通用 logger 对象的指定方法调用，动态决定用哪个 logger 的同名方法
    if hasattr(logger, func):
        # 在不同的线程调用中，根据当前线程指定的 dataflow_logger
        def logger_func_patch(func):
            def func_to_patch(*args, **kwargs):
                # 获取当前线程指定的 logger 并调用同名的方法
                return getattr(get_local_param(LOCAL_LOGGER_KEY, sys_logger), func)(*args, **kwargs)

            return func_to_patch

        setattr(logger, func, logger_func_patch(func))


def get_logger(module):
    split_module = module.split(".")
    if len(split_module) < 2:
        sys_logger.error("not standard module(%s)" % module)
        return sys_logger
    sub_module = split_module[1]
    if sub_module == "stream":
        return stream_logger
    elif sub_module == "batch":
        return batch_logger
    elif sub_module == "flow":
        return flow_logger
    elif sub_module == "component":
        return component_logger
    elif sub_module == "udf":
        return udf_logger
    elif sub_module == "modeling":
        return modeling_logger
    elif sub_module == "uc":
        return uc_logger
    else:
        sys_logger.warning("use defualt logger,submodule - %s" % sub_module)
        return sys_logger


def custom_exception_handler(exc, context):
    """
    自定义错误处理方式
    """
    if isinstance(exc, exceptions.APIException):  # 默认错误统一处理
        return JsonResponse(
            make_resp_dict(code="1500{}".format(exc.status_code), message=exc.detail),
            is_log_resp=True,
        )
    # 兼容老版异常
    elif isinstance(exc, exceptions.APIException):
        return JsonResponse(make_resp_dict(code="1500{}".format(exc.status_code), message=exc.detail), is_log_resp=True)
    elif isinstance(exc, BaseAPIError):
        return JsonResponse(
            {
                "result": False,
                "data": None,
                "message": exc.message,
                "code": exc.code,
                "errors": exc.errors,
            },
            is_log_resp=True,
        )
    elif isinstance(exc, BaseAuthError):
        return JsonResponse(
            {
                "result": False,
                "data": None,
                "message": exc.message,
                "code": exc.code,
                "errors": None,
            }
        )
    else:
        try:
            exception_reporter = debug.ExceptionReporter(context.get("request"), *sys.exc_info())
            if hasattr(context.get("request"), "log"):
                request_id = context.get("request").log.request_id
            else:
                request_id = generate_request_id()
            module = context["view"].__module__ if context.get("view") else "default"
            logger = get_logger(module)
            logger.error(
                request_id + "\r\n" + "".join(exception_reporter.format_exception()),
                result_code=ErrorCode.LOGIC_ERR,
                extra={"request_id": request_id},
            )
        except Exception as e:
            sys_logger.error("handle exception error, %s" % e, result_code=ErrorCode.LOGIC_ERR)
        finally:
            return JsonResponse(make_resp_dict(code="1500500", message="{}".format(exec)), is_log_resp=True)
