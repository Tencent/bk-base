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
import enum


class FunctionType(enum.Enum):
    BUILTIN = "builtin"
    UDF = "udf"
    MODEL = "model"

    ALL_TYPES = [BUILTIN, UDF, MODEL]


class MetricType(enum.Enum):
    DATA_FLOW = "data_flow"
    DATA_PROFILING = "data_profiling"

    ALL_TYPES = [DATA_FLOW, DATA_PROFILING]


class MetricOrigin(enum.Enum):
    METADATA = "metadata"
    TSDB = "tsdb"
    DATAQUERY = "dataquery"
    CONSTANT = "constant"

    ALL_ORIGINS = [METADATA, TSDB, DATAQUERY, CONSTANT]


class ParamType(enum.Enum):
    FIELD = "field"
    METRIC = "metric"
    CONSTANT = "constant"
    EVENT = "event"
    FUNCTION = "function"
    EXPRESSION = "expression"

    ALL_TYPES = [FIELD, METRIC, CONSTANT, EVENT, FUNCTION, EXPRESSION]


class InputType(enum.Enum):
    METRIC = "metric"
    EVENT = "event"

    ALL_TYPES = [METRIC, EVENT]


class ConstantType(enum.Enum):
    FLOAT = "float"
    STRING = "strings"
    INT = "int"
    LIST = "list"

    ALL_TYPES = [FLOAT, STRING, INT, LIST]


class AuditTaskStatus(enum.Enum):
    WAITING = "waiting"
    RUNNING = "running"
    FAILED = "failed"


class TimerType(enum.Enum):
    CRONTAB = "crontab"
    FIX = "fix"
    INTERVAL = "interval"
    ONCE = "once"


class EventLevel(enum.Enum):
    EXTREMELY_SIGNIFICANT = "extremely_significant"
    SIGNIFICANT = "significant"
    LARGER = "larger"
    COMMON = "common"


class EventNotifyStatus(enum.Enum):
    INIT = "init"
    NOTIFYING = "notifying"
    CONVERGED = "converged"
    SHIELDED = "shielded"


class EventNotifySendStatus(enum.Enum):
    SUCC = "success"
    ERR = "error"
    INIT = "init"
    PART_ERR = "partial_error"
