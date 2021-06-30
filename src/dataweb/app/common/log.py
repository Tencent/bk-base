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

import logging

from apps.utils.local import get_request_id

logger_detail = logging.getLogger("root")


# ===============================================================================
# 自定义添加打印内容
# ===============================================================================
# traceback--打印详细错误日志
class logger_traceback:
    """
    详细异常信息追踪
    """

    def __init__(self):
        pass

    def error(self, message=""):
        """
        打印 error 日志方法
        """
        message = self.build_message(message)
        logger_detail.error(message)

    def info(self, message=""):
        """
        info 日志
        """
        message = self.build_message(message)
        logger_detail.info(message)

    def warning(self, message=""):
        """
        warning 日志
        """
        message = self.build_message(message)
        logger_detail.warning(message)

    def debug(self, message=""):
        """
        debug 日志
        """
        message = self.build_message(message)
        logger_detail.debug(message)

    def critical(self, message=""):
        """
        critical 日志
        """
        message = self.build_message(message)
        logger_detail.critical(message)

    def exception(self, message=""):
        message = self.build_message(message)
        logger_detail.exception(message)

    @staticmethod
    def build_message(message):
        request_id = get_request_id()
        return "{} | {}".format(request_id, message)


# traceback--打印详细错误日志
logger = logger_traceback()
