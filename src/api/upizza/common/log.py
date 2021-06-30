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

from common.errorcodes import ErrorCode

DEFAULT_ERROR_CODE = "%s00000" % ErrorCode.BKDATA_PLAT_CODE


class ErrorCodeLogger:
    def __init__(self, ori_logger=None):
        self.logger = ori_logger

    def get_extra(self, result_code, log_type, kwargs_dict):
        # 错误码为7位， 前2位是平台错误码， 数据平台默认为15, 后5位是平台自定义错误码
        if len(result_code) == 5:
            result_code = "{}{}".format(ErrorCode.BKDATA_PLAT_CODE, result_code)
        elif len(result_code) == 3:
            app_name = kwargs_dict.get("app_name", "")
            if app_name in list(ErrorCode.BKDATA_DATAAPI_APPS.keys()):
                dataapi_component = ErrorCode.BKDATA_DATAAPI_APPS[app_name]
            else:
                dataapi_component = ErrorCode.BKDATA_DATAAPI_ADMIN
            result_code = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, dataapi_component, result_code)
        extra = {"result_code": result_code, "log_type": log_type}
        extra.update(kwargs_dict.get("extra", {}))
        if "extra" in list(kwargs_dict.keys()):
            del kwargs_dict["extra"]
        return extra

    def error(self, message="", result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        """
        error 日志
        """
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.error(message, extra=extra, *args, **kwargs)

    def info(self, message="", result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        """
        info 日志
        """
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.info(message, extra=extra, *args, **kwargs)

    def warning(self, message="", result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        """
        warning 日志
        """
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.warning(message, extra=extra, *args, **kwargs)

    def debug(self, message="", result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        """
        debug 日志
        """
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.debug(message, extra=extra, *args, **kwargs)

    def critical(self, message="", result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        """
        critical 日志
        """
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.critical(message, extra=extra, *args, **kwargs)

    def exception(self, exception, result_code=DEFAULT_ERROR_CODE, log_type="STATUS", *args, **kwargs):
        extra = self.get_extra(result_code, log_type, kwargs)
        self.logger.exception(exception, extra=extra, *args, **kwargs)


# logging.Logger.findCaller = find_caller_patch

ori_logger = logging.getLogger("root")
logger = ErrorCodeLogger(ori_logger)

ori_logger_api = logging.getLogger("api")
logger_api = ErrorCodeLogger(ori_logger_api)

ori_logger_sys = logging.getLogger("sys")
sys_logger = logger_sys = ErrorCodeLogger(ori_logger_sys)
