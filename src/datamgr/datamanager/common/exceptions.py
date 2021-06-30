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


class ErrorCode(object):
    BKDATA_PLAT_CODE = "15"
    BKDATA_MANAGER = "500"

    BKDATA_MANAGER_METADTA = "501"
    BKDATA_MANAGER_AUTH = "502"
    BKDATA_MANAGER_DATAQUALITY = "503"
    BKDATA_MANAGER_DMONITOR = "504"
    BKDATA_MANAGER_COLLECTION = "505"
    BKDATA_MANAGER_LIFECYCLE = "506"


class BaseError(Exception):
    """
    在 Base 类中申明必要属性
    """

    MODULE_CODE = None
    CODE = None
    MESSAGE = None

    def __init__(self, message=None, message_kv=None, code=None, errors=None):
        self._message_tmp = message if message is not None else self.MESSAGE
        self._message_kv = message_kv if message_kv is not None else {}
        self._errors = errors

        if code is not None:
            self._code = code
        else:
            self._code = "{}{}{}".format(
                ErrorCode.BKDATA_PLAT_CODE, self.MODULE_CODE, self.CODE
            )

    def __str__(self):
        return "[{}] {}".format(self.code, self.message)

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        try:
            return self._message_tmp.format(**self._message_kv)
        except Exception:
            return self._message_tmp

    @property
    def errors(self):
        return self._errors


class CommonError(BaseError):
    MODULE_CODE = ErrorCode.BKDATA_MANAGER


class ValidationError(CommonError):
    CODE = "01"
    MESSAGE = "Parameter invalid."


class ApiRequestError(CommonError):
    CODE = "02"
    MESSAGE = "Fail to request api."


class ApiResultError(CommonError):
    CODE = "03"
    MESSAGE = "API Response is not up to the requirement."


class RedisConnectError(CommonError):
    CODE = "04"
    MESSAGE = "Fail to connect redis."


class KafkaAliasNotFoundError(CommonError):
    CODE = "05"
    MESSAGE = "Fail to get kafka configs."


class DbAliasNotFoundError(CommonError):
    CODE = "06"
    MESSAGE = "Fail to get db configs."


class DbWriterNotRunningError(CommonError):
    CODE = "07"
    MESSAGE = "Fail to write data to dbwriter because of not running."


class KafkaConsumeTimeoutError(CommonError):
    CODE = "08"
    MESSAGE = "Consume from kafka timeout."


class InfluxDBConnectError(CommonError):
    CODE = "09"
    MESSAGE = "Fail to connect influxDB."


class EventReportError(CommonError):
    CODE = "10"
    MESSAGE = "Fail to report event: {error_info}."


class EsBulkDataError(CommonError):
    CODE = "11"
    MESSAGE = "report bulk data:{error_info} to elasticsearch error"


class GetEsClienError(CommonError):
    CODE = "12"
    MESSAGE = "get es client fail, es_name:{es_name}, es_config:{es_config}"
