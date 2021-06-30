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
from datahub.storekit.utils.common_util import get_date_by_diff

fields = [
    {
        "field_alias": "field1 field_alias",
        "field_index": 1,
        "field_name": "field1",
        "field_type": "string",
        "is_dimension": True,
        "is_time": False,
        "physical_field": "field1",
        "physical_field_type": "text",
    },
    {
        "field_alias": "field2 field_alias",
        "field_index": 2,
        "field_name": "field2",
        "field_type": "int",
        "is_dimension": False,
        "is_time": False,
        "physical_field": "field2",
        "physical_field_type": "int",
    },
    {
        "field_name": "timestamp",
        "field_alias": "数据时间，格式：YYYY-mm-dd HH:MM:SS",
        "field_index": 4,
        "field_type": "timestamp",
        "is_dimension": False,
        "is_time": True,
        "physical_field": "dtEventTime",
        "physical_field_type": "string",
    },
    {
        "field_name": "timestamp",
        "field_alias": "数据时间戳，毫秒级别",
        "field_index": 5,
        "field_type": "timestamp",
        "is_dimension": False,
        "is_time": True,
        "physical_field": "dtEventTimeStamp",
        "physical_field_type": "long",
    },
    {
        "field_name": "timestamp",
        "field_alias": "数据时间，格式：YYYYmmdd",
        "field_index": 6,
        "field_type": "timestamp",
        "is_dimension": False,
        "is_time": True,
        "physical_field": "thedate",
        "physical_field_type": "string",
    },
    {
        "field_name": "timestamp",
        "field_alias": "本地时间，格式：YYYY-mm-dd HH:MM:SS",
        "field_index": 7,
        "field_type": "timestamp",
        "is_dimension": False,
        "is_time": True,
        "physical_field": "localTime",
        "physical_field_type": "string",
    },
]

druid = {
    "config": {},
    "fields": fields,
    "sql": "SELECT field1, field2, time\nFROM 591_anonymous_1217_02\n" "WHERE time > '1h'\nORDER BY time DESC LIMIT 10",
}

es = {
    "config": {
        "analyzedFields": ["ip", "report_time"],
        "dateFields": ["dtEventTime", "dtEventTimeStamp", "localTime", "thedate"],
        "storage_expire_days": 7,
    },
    "fields": fields,
}

mysql = {
    "config": {},
    "fields": fields,
    "sql": "SELECT field1, field2, dtEventTime, dtEventTimeStamp, thedate, localTime\n "
    "FROM 591_anonymous_1217_02\n "
    " WHERE thedate>='%s' AND thedate<='%s'\n ORDER BY dtEventTime DESC LIMIT 10"
    % (get_date_by_diff(0), get_date_by_diff(0)),
}
# 测试获取schema和sql（指定存储）
test_get_schema_and_sql_compare_dict = {
    "code": "1500200",
    "data": {
        "order": ["mysql", "druid", "es"],
        "storage": {
            "druid": druid,
            "es": es,
            "hdfs": {},
            "kafka": {},
            "mysql": mysql,
            "queue": {},
        },
    },
    "errors": None,
    "message": "ok",
    "result": True,
}


# 获取schema和sql（全量存储）
test_get_schema_and_sql_compare_dict_all = {
    "code": "1500200",
    "data": {
        "storages": ["druid", "mysql", "es", "queue", "hdfs", "kafka"],
        "storage": {
            "druid": druid,
            "es": es,
            "hdfs": {
                "config": {"indexed_fields": "field_1_anonymous", "storage_expire_days": 30},
                "fields": fields,
            },
            "kafka": {
                "config": {},
                "fields": fields,
            },
            "mysql": mysql,
            "queue": {
                "config": {},
                "fields": fields,
            },
        },
    },
    "errors": None,
    "message": "ok",
    "result": True,
}
