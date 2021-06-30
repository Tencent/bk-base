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

import datetime

from dataflow.stream.metrics.base_exception_parser import BaseExceptionParser

TYPICAL_ERR = "java.lang.NumberFormatException: For input string: "
PARSE_INT_ERR = "at java.lang.Integer.parseInt"
PARSE_LONG_ERR = "at java.lang.Long.parseLong"
PARSE_FLOAT_ERR = "at java.lang.Float.parseFloat"
PARSE_DOUBLE_ERR = "at java.lang.Double.parseDouble"

CAST_EX_MESSAGE_TEMPLATE = "实时计算任务[%s]在 %s 时处理数据发生异常，尝试将值 %s 由 VARCHAR 类型转换为 %s 时转换失败"
CAST_EX_MESSAGE_TEMPLATE_EN = (
    "The stream task [%s] occur error at %s, trying to convert the value %s from VARCHAR type to %s type"
)


class CastExceptionParser(BaseExceptionParser):
    def __init__(self, stream_id, ex_timestamp, ex_stacktrace):
        super(CastExceptionParser, self).__init__(stream_id, ex_timestamp, ex_stacktrace)
        self._message = None
        self._message_en = None
        self._full_message = None
        self._full_message_en = None

    def message(self):
        return self._message

    def message_en(self):
        return self._message_en

    def full_message(self):
        return self._full_message

    def full_message_en(self):
        return self._full_message_en

    def parse(self):
        # format exception time eg: 2021-01-25 00:00:00
        ex_time = datetime.datetime.fromtimestamp(self.ex_timestamp)
        # 类型转换失败的异常字段的值
        cast_fail_string_value = ""
        # 转换的目标类型
        cast_to_expect_type = ""
        stacktrace_arr = self.ex_stacktrace.splitlines(False)
        if stacktrace_arr and stacktrace_arr[0]:
            cast_fail_string_value = stacktrace_arr[0].replace(TYPICAL_ERR, "")

        if PARSE_INT_ERR in self.ex_stacktrace:
            cast_to_expect_type = "INT"
        elif PARSE_LONG_ERR in self.ex_stacktrace:
            cast_to_expect_type = "BIGINT"
        elif PARSE_FLOAT_ERR in self.ex_stacktrace:
            cast_to_expect_type = "FLOAT"
        elif PARSE_DOUBLE_ERR in self.ex_stacktrace:
            cast_to_expect_type = "DOUBLE"
        else:
            cast_to_expect_type = "VARCHAR"

        value_list = (
            self.flow_name,
            ex_time,
            cast_fail_string_value,
            cast_to_expect_type,
        )
        self._message = CAST_EX_MESSAGE_TEMPLATE % value_list
        self._message_en = CAST_EX_MESSAGE_TEMPLATE_EN % value_list
        self._full_message = CAST_EX_MESSAGE_TEMPLATE % value_list
        self._full_message_en = CAST_EX_MESSAGE_TEMPLATE_EN % value_list
