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

etl_template = {
    "json_config": '{"extract": {"args": [], "next": {"next": [{"next": {"args": ["iter_data"], "next": {"subtype": '
    '"assign_pos", "next": null, "type": "assign", "assign": [{"assign_to": "log", "type": "string", '
    '"index": 0}], "label": "label01"}, "result": "iter_data", "label": "iter_data", "type": "fun", '
    '"method": "iterate"}, "subtype": "access_obj", "result": "access_data", "key": "_value_", "label": '
    '"access_data", "type": "access"}, {"subtype": "assign_obj", "next": null, "type": "assign", '
    '"assign": [{"assign_to": "ip", "type": "string", "key": "_server_"}, {"assign_to": "utc_time", '
    '"type": "string", "key": "_utctime_"}, {"assign_to": "report_time", "type": "string", "key": '
    '"_time_"}, {"assign_to": "gseindex", "type": "long", "key": "_gseindex_"}, {"assign_to": "path", '
    '"type": "string", "key": "_path_"}], "label": "label02"}], "type": "branch", "name": "", "label": '
    'null}, "result": "json_data", "label": "json_data", "type": "fun", "method": "from_json"}, "conf": '
    '{"timestamp_len": 0, "encoding": "UTF8", "time_format": "yyyy-MM-dd HH:mm:ss", "timezone": 0, '
    '"output_field_name": "timestamp", "time_field_name": "utc_time"}}',
    "fields": [
        {
            "field_index": 1,
            "field_name": "ip",
            "field_alias": "IP",
            "field_type": "string",
            "is_dimension": True,
        },
        {
            "field_index": 2,
            "field_name": "utc_time",
            "field_alias": u"上报UTC时间",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 3,
            "field_name": "gseindex",
            "field_alias": "上报索引ID",
            "field_type": "long",
            "is_dimension": False,
        },
        {
            "field_index": 4,
            "field_name": "path",
            "field_alias": u"文件路径",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 5,
            "field_name": "log",
            "field_alias": "日志内容",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 6,
            "field_name": "report_time",
            "field_alias": u"上报本地时间",
            "field_type": "string",
            "is_dimension": False,
        },
    ],
}

log_etl_template = {
    "json_config": {
        "extract": {
            "args": [],
            "next": {
                "next": [
                    {
                        "subtype": "assign_obj",
                        "next": None,
                        "type": "assign",
                        "assign": [
                            {"assign_to": "ip", "type": "string", "key": "_server_"},
                            {
                                "assign_to": "report_time",
                                "type": "string",
                                "key": "_time_",
                            },
                            {
                                "assign_to": "gseindex",
                                "type": "long",
                                "key": "_gseindex_",
                            },
                            {"assign_to": "path", "type": "string", "key": "_path_"},
                        ],
                    },
                    {
                        "next": {
                            "args": [],
                            "next": {
                                "subtype": "assign_value",
                                "next": None,
                                "type": "assign",
                                "assign": {"assign_to": "log", "type": "string"},
                            },
                            "result": "iter_data",
                            "label": "iter_data",
                            "type": "fun",
                            "method": "iterate",
                        },
                        "subtype": "access_obj",
                        "result": "access_data",
                        "key": "_value_",
                        "type": "access",
                    },
                ],
                "type": "branch",
                "name": "",
                "label": None,
            },
            "result": "json_data",
            "type": "fun",
            "method": "from_json",
        },
        "conf": {
            "timestamp_len": 0,
            "encoding": "UTF-8",
            "time_format": "yyyy-MM-dd HH:mm:ss",
            "timezone": 8,
            "output_field_name": "timestamp",
            "time_field_name": "report_time",
        },
    },
    "fields": [
        {
            "field_index": 1,
            "field_name": "ip",
            "field_alias": u"IP",
            "field_type": "string",
            "is_dimension": True,
        },
        {
            "field_index": 2,
            "field_name": "report_time",
            "field_alias": u"上报本地时间",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 3,
            "field_name": "gseindex",
            "field_alias": u"上报索引ID",
            "field_type": "long",
            "is_dimension": False,
        },
        {
            "field_index": 4,
            "field_name": "path",
            "field_alias": u"文件路径",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 5,
            "field_name": "log",
            "field_alias": u"自定义日志字段",
            "field_type": "string",
            "is_dimension": False,
        },
    ],
}

bkunifylogbeat_etl_template = {
    "json_config": {
        "extract": {
            "args": [],
            "next": {
                "next": [
                    {
                        "subtype": "assign_obj",
                        "next": None,
                        "type": "assign",
                        "assign": [
                            {"assign_to": "ip", "type": "string", "key": "ip"},
                            {
                                "assign_to": "report_time",
                                "type": "string",
                                "key": "datetime",
                            },
                            {
                                "assign_to": "gseindex",
                                "type": "long",
                                "key": "gseindex",
                            },
                            {"assign_to": "path", "type": "string", "key": "filename"},
                        ],
                    },
                    {
                        "next": {
                            "args": [],
                            "next": {
                                "subtype": "assign_obj",
                                "next": None,
                                "type": "assign",
                                "assign": [
                                    {
                                        "assign_to": "log",
                                        "type": "string",
                                        "key": "data",
                                    }
                                ],
                            },
                            "result": "iter_data",
                            "type": "fun",
                            "method": "iterate",
                        },
                        "subtype": "access_obj",
                        "result": "access_data",
                        "key": "items",
                        "type": "access",
                    },
                ],
                "type": "branch",
                "name": "branch",
            },
            "result": "json_data",
            "type": "fun",
            "method": "from_json",
        },
        "conf": {
            "timestamp_len": 0,
            "encoding": "UTF-8",
            "time_format": "yyyy-MM-dd HH:mm:ss",
            "timezone": 8,
            "output_field_name": "timestamp",
            "time_field_name": "report_time",
        },
    },
    "fields": [
        {
            "field_index": 1,
            "field_name": "ip",
            "field_alias": u"IP",
            "field_type": "string",
            "is_dimension": True,
        },
        {
            "field_index": 2,
            "field_name": "report_time",
            "field_alias": u"上报本地时间",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 3,
            "field_name": "gseindex",
            "field_alias": u"上报索引ID",
            "field_type": "long",
            "is_dimension": False,
        },
        {
            "field_index": 4,
            "field_name": "path",
            "field_alias": u"文件路径",
            "field_type": "string",
            "is_dimension": False,
        },
        {
            "field_index": 5,
            "field_name": "log",
            "field_alias": u"自定义日志字段",
            "field_type": "string",
            "is_dimension": False,
        },
    ],
}

db_etl_template = {
    "json_config": {
        "extract": {
            "args": [],
            "next": {
                "next": {
                    "args": [],
                    "result": "value",
                    "type": "fun",
                    "method": "iterate",
                },
                "subtype": "access_obj",
                "result": "valuelist",
                "key": "value",
                "type": "access",
            },
            "result": "json",
            "type": "fun",
            "method": "from_json",
        },
        "conf": {
            "timestamp_len": 0,
            "encoding": "UTF-8",
            "time_format": "yyyy-MM-dd HH:mm:ss",
            "timezone": 8,
        },
    },
    "fields": [],
}
