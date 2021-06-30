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

import random
import string
from datetime import datetime

import pytz
from jpype.types import JFloat, JInt
from tencent_bkbase_datalake import tables

TEST_START_DATE = datetime(2020, 8, 1, 0, 0, 0, tzinfo=pytz.utc)
ONE_DAY_IN_MS = 24 * 3600000


def generate_data(fields, record_count, plus_days=7):
    c_names = []
    for (k, _) in fields.items():
        c_names.append(k)

    c_data = []
    for _ in range(0, record_count):
        data = []
        for name in c_names:
            data.append(generate_random_value(fields[name], name, int(plus_days)))
        c_data.append(data)

    return c_names, c_data


def generate_random_value(val_type, column_name, plus_days=7):
    if val_type == tables.INT:
        return JInt(random.randint(1, 1000000))
    elif val_type == tables.LONG:
        if column_name.lower() == tables.DTEVENTTIMESTAMP.lower():
            return round(TEST_START_DATE.timestamp() * 1000) + random.randint(1, plus_days * ONE_DAY_IN_MS)
        else:
            return random.randint(1, 1000000000000)
    elif val_type == tables.FLOAT:
        return JFloat(random.uniform(1, 1000000))
    elif val_type == tables.DOUBLE:
        return random.uniform(1, 1000000000000)
    elif val_type == tables.STRING:
        return "".join(random.sample(string.ascii_letters + string.digits, random.randint(1, 20)))
    elif val_type == tables.TIMESTAMP:
        from java.time import Instant, ZoneOffset

        ts = round(TEST_START_DATE.timestamp() * 1000) + random.randint(1, plus_days * ONE_DAY_IN_MS)
        return Instant.ofEpochMilli(ts).atOffset(ZoneOffset.UTC)
    else:
        return None
