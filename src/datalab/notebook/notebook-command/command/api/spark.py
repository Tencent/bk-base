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
import re
import time

import requests
from command.constants import (
    AVAILABLE,
    BK_USER,
    CODE,
    CONTEXT,
    DATA,
    ERROR_LINE,
    ERRORS,
    EVALUE,
    FORBIDDEN_CODE,
    FORBIDDEN_MESSAGE,
    GEOG_AREA_CODE,
    HEADERS,
    HTTP_STATUS_OK,
    IDLE,
    INLAND,
    MESSAGE,
    OK,
    OUTPUT,
    RESULT,
    SCHEDULE_EXIST_CODE,
    SERVER_ID,
    SPARK_STATUS_WAIT_INTERVAL,
    STATE,
    STATUS,
    SYNTAX_ERROR_CODE,
    SYNTAX_ERROR_MESSAGE,
    TEXT_PLAIN,
)
from command.exceptions import ApiRequestException, ExecuteException
from command.settings import DATAFLOW_API_ROOT
from command.util.display_result import DFDisplay
from command.utils import extract_error_message, parse_response


def create_server_and_session(server_id, bk_username):
    """
    创建spark session server连接

    :param server_id: server id
    :param bk_username: 用户名
    :return: 创建结果
    """
    url = "%s/batch/interactive_servers/" % DATAFLOW_API_ROOT
    data = json.dumps({SERVER_ID: server_id, BK_USER: bk_username, GEOG_AREA_CODE: INLAND})
    response = requests.post(url=url, headers=HEADERS, data=data)
    if response.status_code == HTTP_STATUS_OK:
        res_json = response.json()
        if res_json[CODE] == SCHEDULE_EXIST_CODE:
            delete_server(server_id)
            create_server_and_session(server_id, bk_username)
        if not res_json[RESULT]:
            raise ExecuteException("{}: {}".format("创建spark连接失败", res_json[MESSAGE]))
    else:
        raise ApiRequestException("{}: {}".format("dataflow接口异常，创建spark连接失败", extract_error_message(response)))


def get_session_status(server_id):
    """
    获取session状态

    :param server_id: server id
    :return: session状态
    """
    url = "{}/batch/interactive_servers/{}/session_status?geog_area_code=inland".format(
        DATAFLOW_API_ROOT,
        server_id,
    )
    response = requests.get(url=url)
    return parse_response(response, "获取session状态失败")


def run_code(server_id, code, spark_context):
    """
    运行代码

    :param server_id: server id
    :param code: 代码
    :param spark_context: spark上下文
    :return: 运行结果
    """
    url = "{}/batch/interactive_servers/{}/codes/".format(DATAFLOW_API_ROOT, server_id)
    data = json.dumps({CODE: code, CONTEXT: spark_context, GEOG_AREA_CODE: INLAND})
    response = requests.post(url=url, headers=HEADERS, data=data)
    if response.status_code == HTTP_STATUS_OK:
        res_json = response.json()
        if res_json[CODE] in [FORBIDDEN_CODE, SYNTAX_ERROR_CODE] or not res_json[RESULT]:
            message, info = (
                (FORBIDDEN_MESSAGE, res_json[ERRORS][0][ERROR_LINE])
                if res_json[CODE] == FORBIDDEN_CODE
                else (SYNTAX_ERROR_MESSAGE, res_json[ERRORS][0][ERROR_LINE])
                if res_json[CODE] == SYNTAX_ERROR_CODE
                else ("执行代码失败", res_json[MESSAGE])
            )
            raise ExecuteException("{}: {}".format(message, info))
        return res_json[DATA]
    else:
        raise ApiRequestException("{}: {}".format("dataflow接口异常，执行代码失败", extract_error_message(response)))


def get_code_result(server_id, task_id):
    """
    获取代码执行结果

    :param server_id: server id
    :param task_id: task id
    :return: 执行结果
    """
    url = "{}/batch/interactive_servers/{}/codes/{}?geog_area_code=inland".format(
        DATAFLOW_API_ROOT,
        server_id,
        task_id,
    )
    response = requests.get(url=url)
    return parse_response(response, "获取代码执行结果失败")


def delete_server(server_id):
    """
    删除spark session server

    :param server_id: server id
    :return: 删除结果
    """
    url = "{}/batch/interactive_servers/{}/".format(DATAFLOW_API_ROOT, server_id)
    data = json.dumps({GEOG_AREA_CODE: INLAND})
    response = requests.delete(url=url, headers=HEADERS, data=data)
    return parse_response(response, "删除session server失败")


def create_new_server(server_id, bk_username):
    """
    创建新的spark server连接

    :param server_id: server id
    :param bk_username: 用户名
    :return: 创建结果
    """
    create_server_and_session(server_id, bk_username)
    while True:
        time.sleep(SPARK_STATUS_WAIT_INTERVAL)
        session_status = get_session_status(server_id)
        if session_status[STATE] == IDLE:
            break


def make_free_session(server_id, bk_username):
    """
    初始判断，使得session状态保持空闲

    :param server_id: server id
    :param bk_username: 用户名
    :return: 执行状态
    """
    try:
        session_status = get_session_status(server_id)
        if session_status[STATE] != IDLE:
            create_new_server(server_id, bk_username)
    except ExecuteException:
        create_new_server(server_id, bk_username)


def set_spark_context(local_ns, code):
    """
    设置spark上下文，用于代码检测
    """
    output_values = local_ns["_oh"]
    forbidden_codes = []
    spark_context = []
    for output in output_values.values():
        if isinstance(output, str):
            if FORBIDDEN_MESSAGE in output or SYNTAX_ERROR_MESSAGE in output:
                forbidden_codes.append(output.rsplit(": ", 1)[1])
    for key, input_value in local_ns.items():
        if re.search(r"_i(\d+)", key) and "%%spark" in input_value:
            input_value = input_value.replace("%%spark", "").lstrip()
            if not any(forbidden_code in input_value for forbidden_code in forbidden_codes):
                spark_context.append(input_value)
    spark_context.append(code)
    return spark_context


def round_robin_code_res(server_id, task_id):
    """
    轮询代码执行结果
    """
    while True:
        time.sleep(SPARK_STATUS_WAIT_INTERVAL)
        code_result = get_code_result(server_id, task_id)
        if code_result[STATE] == AVAILABLE:
            output = code_result[OUTPUT]
            if output[STATUS] == OK:
                value = output[DATA][TEXT_PLAIN]
                return DFDisplay(value)
            else:
                raise ExecuteException(output[EVALUE])
