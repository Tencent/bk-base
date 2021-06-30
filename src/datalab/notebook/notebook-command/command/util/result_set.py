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

import json

from command.constants import BK_CHART, LIST, QUERY_RESULT_LIMIT, SELECT_FIELDS_ORDER


class ResultSet(dict):
    def __init__(self, data_list, select_fields_order, total_records, sql, chart_type):
        self.field_names = select_fields_order
        self.sql = sql
        self.total_records = total_records
        if self.total_records > QUERY_RESULT_LIMIT:
            print("页面最多展示%s条数据，请将结果集赋值给变量进行处理，示例：result = _" % str(QUERY_RESULT_LIMIT))
        self.data = data_list
        self.chart_type = chart_type
        dict.__init__(self, {LIST: data_list, SELECT_FIELDS_ORDER: select_fields_order, BK_CHART: chart_type})

    def __str__(self):
        return json.dumps(
            {LIST: self.data[:QUERY_RESULT_LIMIT], SELECT_FIELDS_ORDER: self.field_names, BK_CHART: self.chart_type}
        )

    __repr__ = __str__

    def dict(self):
        result_dict = {field_name: [] for field_name in self.field_names}
        for line in self.data:
            for field_name in result_dict.keys():
                result_dict[field_name].append(line.get(field_name))
        return result_dict

    def dicts(self):
        return self.data

    def DataFrame(self):
        import pandas as pd

        frame = pd.DataFrame(self.data, columns=(self and self.field_names) or [])
        return frame
