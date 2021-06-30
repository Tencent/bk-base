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

from __future__ import absolute_import, unicode_literals

BKDATA_PLAT_CODE = "15"
BKDATA_DATAMANAGE_ENGINE = "40"


class CodeGroup(object):
    class ErrorCode(object):
        def __init__(self, code, message):
            self.code = "{}{}{}".format(BKDATA_PLAT_CODE, BKDATA_DATAMANAGE_ENGINE, code)
            self.message = message

    # Assign code here
    ENGINE_ERR = ErrorCode("000", "")
    LOG_CONFIGURATION_ERR = ErrorCode("001", "Not to set right logger configuration.")
    DB_CONNECT_ERR = ErrorCode("002", "Can not to connect database.")
    REDIS_CONNECT_ERR = ErrorCode("003", "Can not to connect redis.")
    REAL_TASK_EXECUTE_ERR = ErrorCode("004", "Real task in child process fail to execute.")
    CRONTAB_PARSE_ERR = ErrorCode("005", "Invalid crontab pattern.")
    CONFIGURATION_ERR = ErrorCode("006", "Not apply the right configuration.")
    INVALID_EXECUTION_STATUS_ERR = ErrorCode("007", "Invalid execution statue of task.")
    TASK_EXITED_ABNORMALLY = ErrorCode("008", "Task exited abnormally.")


class EngineError(Exception):
    ERROR_CODE = CodeGroup.ENGINE_ERR

    def __init__(self, message=None, code=None):
        self.code = self.ERROR_CODE.code
        self.message = self.ERROR_CODE.message

        if code is not None:
            self._code = code

        if message is not None:
            self.message = message

    def __str__(self):
        return "[{}] {}".format(self.code, self.message)


class LogConfigurationError(EngineError):
    ERROR_CODE = CodeGroup.LOG_CONFIGURATION_ERR


class DBConnectError(EngineError):
    ERROR_CODE = CodeGroup.DB_CONNECT_ERR


class RedisConnectError(EngineError):
    ERROR_CODE = CodeGroup.REDIS_CONNECT_ERR


class RealTaskExecuteError(EngineError):
    ERROR_CODE = CodeGroup.REAL_TASK_EXECUTE_ERR


class CrontabParseError(EngineError):
    ERROR_CODE = CodeGroup.CRONTAB_PARSE_ERR


class ConfigurationError(EngineError):
    ERROR_CODE = CodeGroup.CONFIGURATION_ERR


class InvalidExecutionStatusError(EngineError):
    ERROR_CODE = CodeGroup.INVALID_EXECUTION_STATUS_ERR


class TaskExitedAbnormallyError(EngineError):
    ERROR_CODE = CodeGroup.TASK_EXITED_ABNORMALLY
