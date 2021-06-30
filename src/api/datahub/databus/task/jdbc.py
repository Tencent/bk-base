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

# 采集模式, 前端配置的, 兼容旧版golang的采集器
COLLECTION_TYPE_TIME = "time"
COLLECTION_TYPE_PRI_KEY = "pri"
COLLECTION_TYPE_ALL = "all"
COLLECTION_TYPE_HISTORY_PERIOD_START_AT = 0

# 运行模式
MODE_INCREMENTING = "incrementing"
MODE_UNIX_TIMESTAMP_M = "ts.minutes"
MODE_UNIX_TIMESTAMP_S = "ts.seconds"
MODE_UNIX_TIMESTAMP_MS = "ts.milliseconds"
MODE_TIMESTAMP = "timestamp"
MODE_ALL = "all"
MODE_HISTORY_PERIOD_INCREMENTING = "historyAndPeriod.incrementing"
MODE_HISTORY_PERIOD_TIMESTAMP = "historyAndPeriod.timestamp"

# 时间格式
TIME_FORMAT_UNIX_TIMESTAMP_M = "Unix Time Stamp(mins)"
TIME_FORMAT_UNIX_TIMESTAMP_S = "Unix Time Stamp(seconds)"
TIME_FORMAT_UNIX_TIMESTAMP_MS = "Unix Time Stamp(mins)"


def get_mode(collection_type, time_format, start_at):
    """
    从采集类型和时间格式确定采集模式
    :param collection_type: 采集类型
    :param time_format: 时间格式
    :return: 采集模式
    """
    # mode 取值范围: timestamp, ts.milliseconds, ts.seconds, ts.minutes, incrementing
    # 新增 mode 范围: historyAndPeriod.timestamp / historyAndPeriod.incrementing
    if start_at == COLLECTION_TYPE_HISTORY_PERIOD_START_AT:
        if collection_type == COLLECTION_TYPE_PRI_KEY:
            mode = MODE_HISTORY_PERIOD_INCREMENTING
        else:
            mode = MODE_HISTORY_PERIOD_TIMESTAMP
    elif collection_type == COLLECTION_TYPE_PRI_KEY:
        mode = MODE_INCREMENTING
    elif collection_type == COLLECTION_TYPE_TIME:
        if time_format == TIME_FORMAT_UNIX_TIMESTAMP_M:
            mode = MODE_UNIX_TIMESTAMP_M
        elif time_format == TIME_FORMAT_UNIX_TIMESTAMP_S:
            mode = MODE_UNIX_TIMESTAMP_S
        elif time_format == TIME_FORMAT_UNIX_TIMESTAMP_MS:
            mode = MODE_UNIX_TIMESTAMP_MS
        else:
            mode = MODE_TIMESTAMP
    else:
        # ALL
        mode = MODE_ALL
    return mode
