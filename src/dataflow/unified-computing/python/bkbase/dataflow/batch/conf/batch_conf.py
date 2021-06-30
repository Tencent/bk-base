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

from bkbase.dataflow.batch.gateway.py_gateway import get_spark_conf

conf = get_spark_conf()


def get_batch_spark_conf():
    return conf


def get_string_conf(key, default=None):
    value = str(conf.get(key, default))
    if value is None:
        raise Exception("Can't get spark configuration: %s" % key)
    return value


def get_boolean_conf(key, default=None):
    value = conf._jconf.getBoolean(key, default)
    if value is None:
        raise Exception("Can't get spark configuration: %s" % key)
    return value


ENABLE_TEST = False


DATAFLOWAPI_URL_PREFIX = get_string_conf("spark.uc.batch.code.dataflow.url.prefix")
STOREKITAPI_URL_PREFIX = get_string_conf("spark.uc.batch.code.storekit.url.prefix")
DATABUSAPI_URL_PREFIX = get_string_conf("spark.uc.batch.code.databus.url.prefix")
DATAMANAGEAPI_URL_PREFIX = get_string_conf("spark.uc.batch.code.datamanage.url.prefix")
TIMEZONE = get_string_conf("spark.uc.batch.code.timezone", "Asia/Shanghai")


ENABLE_MONITOR = get_boolean_conf("spark.uc.batch.code.monitor.enable", True)

SELF_DEPENDENCY_PATH_PREFIX = "/api/flow_copy"
SELF_DEPENDENCY_ORIGIN_KEY = "origin_path"
SELF_DEPENDENCY_COPY_KEY = "copy_path"

SCHEDULE_TIME_CONF = "spark.uc.batch.code.schedule.time"

RESERVED_FIELDS = [
    {"field": "thedate", "type": "int"},
    {"field": "dtEventTime", "type": "string"},
    {"field": "dtEventTimeStamp", "type": "long"},
    {"field": "localTime", "type": "string"},
]
