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

from enum import Enum


class PeriodUnit(Enum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class NodeType(Enum):
    MODEL = "model"
    DATA = "data"


class TableType(Enum):
    RESULT_TABLE = "result_table"
    QUERY_SET = "query_set"
    OTHER = "other"


class DeepLearningFramework(Enum):
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"


class Role(Enum):
    batch = "batch"


class ReservedFiled(object):
    def __init__(self, field_name, field_type):
        self.field_name = field_name
        self.field_type = field_type


class RunMode(Enum):
    PRODUCT = "product"
    DEBUG = "debug"


class ProcessType(Enum):
    UNTRAINED_RUN = "untrained-run"
    TRAINED_RUN = "trained-run"
    TRAIN = "train"


class IcebergReservedField(Enum):
    start_time = ReservedFiled("_startTime_", "varchar")
    end_time = ReservedFiled("_endTime_", "varchar")
    thedate = ReservedFiled("thedate", "int")
    dteventtime = ReservedFiled("dteventtime", "varchar")
    dteventtimestamp = ReservedFiled("dteventtimestamp", "int")
    localtime = ReservedFiled("localtime", "varchar")
