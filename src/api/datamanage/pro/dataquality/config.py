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


PROFILING_UDF_CONFIGS = {
    'descriptive_statistics': {
        'udf_name': 'udf_descriptive_statistics',
        'field_types': ['double', 'float', 'int', 'long', 'string', 'text'],
    },
    'data_distribution': {
        'udf_name': 'udf_data_distribution',
        'field_types': ['double', 'float', 'int', 'long'],
    },
    'cardinal_distribution': {
        'udf_name': 'udf_cardinal_distribution',
        'field_types': ['double', 'float', 'int', 'long', 'string', 'text'],
    },
}

PROFILING_EXCLUDE_FIELDS = [
    '_rownum',
    'thedate',
    'dtEventTime',
    'dtEventTimeStamp',
    'localTime',
    'time',
]

CORRECTION_EXCLUDE_FIELDS = [
    '_startTime_',
    '_endTime_',
    'timestamp',
]

TEXT_FIELD_TYPES = [
    'string',
    'text',
]

CORRECT_SQL_DATA_TYPES_MAPPINGS = {
    'string': {
        'stream': 'VARCHAR',
        'batch': 'STRING',
    },
    'int': {
        'stream': 'INT',
        'batch': 'INT',
    },
    'long': {
        'stream': 'BIGINT',
        'batch': 'BIGINT',
    },
    'float': {
        'stream': 'FLOAT',
        'batch': 'FLOAT',
    },
    'double': {
        'stream': 'DOUBLE',
        'batch': 'DOUBLE',
    },
}
