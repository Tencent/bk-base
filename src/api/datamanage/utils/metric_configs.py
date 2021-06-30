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


METRIC_TABLE_MAPPINGS = {
    'input_count': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_input_total',
        'metric': 'data_inc',
        'aggregation': 'sum',
        'sensitivity': 'public',
    },
    'output_count': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_output_total',
        'metric': 'data_inc',
        'aggregation': 'sum',
        'sensitivity': 'public',
    },
    'data_time_delay': {
        'database': 'monitor_data_metric',
        'measurement': 'data_relative_delay',
        'metric': 'ab_delay',
        'aggregation': 'max',
        'sensitivity': 'public',
    },
    'process_time_delay': {
        'database': 'monitor_data_metric',
        'measurement': 'data_relative_delay',
        'metric': 'relative_delay',
        'aggregation': 'max',
        'sensitivity': 'public',
    },
    'drop_rate': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_drop_rate',
        'metric': 'drop_rate',
        'aggregation': 'mean',
        'sensitivity': 'public',
    },
    'drop_count': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_drop_detail',
        'metric': 'drop_cnt',
        'aggregation': 'sum',
        'sensitivity': 'public',
    },
    'loss_rate': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_audit',
        'metric': 'loss_rate',
        'aggregation': 'mean',
        'sensitivity': 'public',
    },
    'loss_count': {
        'database': 'monitor_data_metric',
        'measurement': 'data_loss_audit',
        'metric': 'loss_cnt',
        'aggregation': 'sum',
        'sensitivity': 'public',
    },
    'alert_count': {
        'database': 'monitor_data_metric',
        'measurement': 'dmonitor_alerts',
        'metric': 'message',
        'aggregation': 'count',
        'sensitivity': 'public',
    },
    'profiling': {
        'database': 'monitor_data_metric',
        'measurement': 'data_set_field_profiling',
        'metric': '*',
        'aggregation': 'last',
        'sensitivity': 'private',
    },
}


class FunctionType(object):
    BUILTIN = 'builtin'
    UDF = 'udf'
    MODEL = 'model'

    ALL_TYPES = [BUILTIN, UDF, MODEL]


class ParamType(object):
    FIELD = 'field'
    METRIC = 'metric'
    CONSTANT = 'constant'
    EVENT = 'event'
    FUNCTION = 'function'
    EXPRESSION = 'expression'

    ALL_TYPES = [FIELD, METRIC, CONSTANT, EVENT, FUNCTION, EXPRESSION]


class ConstantType(object):
    DOUBLE = 'double'
    STRING = 'strings'
    INT = 'int'
    LIST = 'list'

    ALL_TYPES = [DOUBLE, STRING, INT, LIST]


class ProfilingWay(object):
    COMPLETE = 'complete'
    SAMPLING = 'sampling'
    CUSTOM = 'custom'

    ALL_TYPES = [CUSTOM, COMPLETE, SAMPLING]
