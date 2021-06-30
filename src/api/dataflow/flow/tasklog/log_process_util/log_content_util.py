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

import re

from dataflow.flow.tasklog.log_process_util.mask_info_util import MaskInfoUtil
from dataflow.pizza_settings import FLINK_LOG_REGEX_STR, JOB_NAVI_LOG_REGEX_STR, SPARK_LOG_REGEX_STR

spark_log_pattern = re.compile(SPARK_LOG_REGEX_STR, re.I)
flink_log_pattern = re.compile(FLINK_LOG_REGEX_STR, re.I)
jobnavi_log_pattern = re.compile(JOB_NAVI_LOG_REGEX_STR, re.I)


def search_log_content(log_content_line, search_words):
    """
    search directly using 'in'
    efficiency compared equal to ac_automaton when search_words is less
    :param log_content_line: log_content_line
    :param search_words: search_words list
    :return:
    """
    if any(one_word.lower() in log_content_line.lower() for one_word in search_words):
        return log_content_line
    else:
        return None


def parse_log_line(log_content=None, log_list=None, search_words=None, log_platform_type="flink"):
    """
    parse log content, search and filter
    :param log_content: log_content (all logs in string)
    :param log_list: log list (all logs in list)
    :param search_words: search_words
    :param log_platform_type: log_platform_type, yarn/jobnavi
    :return:
    """
    pattern = None
    if log_platform_type.startswith("spark"):
        pattern = spark_log_pattern
    elif log_platform_type.startswith("flink"):
        pattern = flink_log_pattern
    elif log_platform_type.startswith("jobnavi"):
        pattern = jobnavi_log_pattern
    if log_list:
        all_lines = log_list
    else:
        all_lines = [s for s in log_content.splitlines() if s]

    json_log_list = []
    # 临时存放日志，供合并日志使用
    tmp_log_list = []
    for one_line in all_lines:
        m = pattern.match(one_line)
        if m:
            log_time = m.group(1)
            log_level = m.group(2)
            log_source = m.group(3)
            log_line_content = m.group(4)
            tmp_log_list.append(
                {
                    "log_time": log_time,
                    "log_level": log_level,
                    "log_source": log_source,
                    "log_content": log_line_content,
                }
            )
        else:
            # 格式不匹配的日志归为上一行
            if len(tmp_log_list) > 0:
                tmp_log_list[-1]["log_content"] = tmp_log_list[-1]["log_content"] + "\n" + one_line
            else:
                tmp_log_list.append(
                    {
                        "log_time": "",
                        "log_level": "",
                        "log_source": "",
                        "log_content": one_line,
                    }
                )

        # 当 tmp_log_list 长度大于1时，确认至少有一条完整的日志
        if len(tmp_log_list) > 1:
            current_log = tmp_log_list[0]["log_content"]
            if search_words:
                current_log = search_log_content(current_log, search_words)
            if current_log:
                current_log = MaskInfoUtil.mask_secret_info(current_log)
            if current_log:
                tmp_log_list[0]["log_content"] = current_log
                json_log_list.append(tmp_log_list.pop(0))

    # 将所有的日志都进行匹配
    for tmp_log in tmp_log_list:
        current_log = tmp_log["log_content"]
        if search_words:
            current_log = search_log_content(current_log, search_words)
        if current_log:
            current_log = MaskInfoUtil.mask_secret_info(current_log)
        if current_log:
            tmp_log["log_content"] = current_log
            json_log_list.append(tmp_log)
    return json_log_list
