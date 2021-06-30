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

from common.local import set_local_param
from django.utils.deprecation import MiddlewareMixin

from dataflow.shared.log import LOCAL_LOGGER_KEY

from .log import batch_logger, component_logger, flow_logger, modeling_logger, stream_logger, sys_logger, udf_logger


class PatchLogger(MiddlewareMixin):
    """
    DataFlow中间件，对 logger 方法进行 patch，主要处理逻辑：

    1. 对view类中API的调用前，基于访问路径区分当前 submodule，往线程变量中写入对应的 logger
    2. 当调用 common.log 中的 logger 对象，根据调用的对应方法('error', 'info', 'warning', 'debug', 'critical', 'exception')，
        动态转调当前线程变量缓存的 logger 对象同名方法
    """

    @staticmethod
    def set_logger(path):
        sys_logger.info(message="request.path - %s" % path)
        split_path = path.split("/dataflow/")
        if len(split_path) < 2:
            sys_logger.error(message="request.path format error - %s" % path)
            set_local_param(LOCAL_LOGGER_KEY, sys_logger)
        sub_module = split_path[1].split("/")[0]
        if sub_module == "stream":
            set_local_param(LOCAL_LOGGER_KEY, stream_logger)
        elif sub_module == "batch":
            set_local_param(LOCAL_LOGGER_KEY, batch_logger)
        elif sub_module == "flow":
            set_local_param(LOCAL_LOGGER_KEY, flow_logger)
        elif sub_module == "component":
            set_local_param(LOCAL_LOGGER_KEY, component_logger)
        elif sub_module == "udf":
            set_local_param(LOCAL_LOGGER_KEY, udf_logger)
        elif sub_module == "modeling":
            set_local_param(LOCAL_LOGGER_KEY, modeling_logger)
        else:
            sys_logger.error("not support logger type - %s" % sub_module)
            set_local_param(LOCAL_LOGGER_KEY, sys_logger)

    def process_request(self, request):
        PatchLogger.set_logger(request.path)
