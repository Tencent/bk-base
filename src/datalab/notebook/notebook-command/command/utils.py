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

import inspect
import time

from command.constants import (
    CELL_ID,
    DATA,
    DISPATCH_SHELL,
    EXECUTE_REQUEST,
    HEADER,
    HTTP_STATUS_OK,
    ITEM_MAPPING,
    MESSAGE,
    MSG,
    REPLY,
    REQUEST,
    RESULT,
    RESULT_TABLE,
    TRANSPORT_RETRY_INTERVAL,
    TRANSPORT_RETRY_TIMES,
    USERNAME,
)
from command.exceptions import ApiRequestException, ExecuteException


def get_bk_username():
    """
    获取ws header中的用户名
    """
    header = get_request_header()
    return header[USERNAME]


def get_cell_id():
    """
    获取ws header中的cell id
    """
    header = get_request_header()
    return header[CELL_ID]


def get_request_header():
    """
    获取ws header
    """
    frames = {}
    for info in inspect.stack():
        if info[3] == DISPATCH_SHELL:
            frames[REQUEST] = info[0]
        if info[3] == EXECUTE_REQUEST:
            frames[REPLY] = info[0]
    msg = frames[REQUEST].f_locals[MSG]
    return msg[HEADER]


def parse_response(response, message):
    """
    解析http接口返回体

    :param response: 接口返回体
    :param message: 接口返回失败时的提示信息
    :return: 解析结果
    """
    if response.status_code == HTTP_STATUS_OK:
        res_json = response.json()
        if not res_json[RESULT]:
            raise ExecuteException("{}: {}".format(message, res_json[MESSAGE]))
        return res_json[DATA]
    else:
        raise ApiRequestException("{}: {}".format(message, extract_error_message(response)))


def extract_error_message(response):
    """
    提取http非200状态码的报错信息

    :param response: 周边接口返回体
    :return: 错误信息
    """
    try:
        message = response.json().get(MESSAGE)
    except ValueError:
        message = response.content
    return message


def api_retry(func, task_id):
    """
    添加api重试机制

    :param func: 调用的api函数
    :param task_id: 任务id
    :return: 重试结果
    """
    retry_flag, status_result = False, ""
    for i in range(TRANSPORT_RETRY_TIMES):
        time.sleep(TRANSPORT_RETRY_INTERVAL)
        retry_flag, status_result = func(task_id)
        if retry_flag:
            break
    return retry_flag, status_result


def transform_column_name(obj, output_type=RESULT_TABLE):
    """
    转换产出物格式

    :param obj: 产出物
    :param output_type: 产出物类型
    :return: 转换后的产出物
    """
    if isinstance(obj, list):
        return [transform_column_name(element, output_type) for element in obj]
    elif isinstance(obj, dict):
        return {ITEM_MAPPING[output_type].get(key): transform_column_name(value) for key, value in obj.items()}
    else:
        return obj
