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

from dataflow import pizza_settings
from dataflow.models import ParametersInfo

LOG_SPLIT_CHAR = "\x01"

# 从 DB 初始化参数信息
FLOW_MAX_NODE_COUNT = None
for _param in ParametersInfo.list("flow"):
    if _param["key"] == "FLOW_MAX_NODE_COUNT":
        FLOW_MAX_NODE_COUNT = int(_param["value"])

# db 操作失败重试最大次数，当前仅使用于 `retry_db_operation_on_exception` 装饰器
RETRY_DB_OPERATION_MAX_COUNT = 3

SUBMIT_STREAM_JOB_POLLING_TIMES = getattr(pizza_settings, "SUBMIT_STREAM_JOB_POLLING_TIMES", 36)
