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

import ipykernel
import requests
from command.api.meta import get_result_table_info
from command.constants import (
    BK_USERNAME,
    BKDATA_AUTHENTICATION_METHOD,
    DATA_PROCESSINGS,
    FIELDS,
    HEADERS,
    INPUT_RT,
    INPUTS,
    MODEL,
    NAME,
    OUTPUT_NAME,
    OUTPUT_RT,
    OUTPUT_TYPE,
    OUTPUTS,
    PROCESSING_ID,
    PROCESSING_TYPE,
    RESULT_TABLE,
    RESULT_TABLE_ID,
    RESULT_TABLES,
    SQL,
    STORAGES,
    USER,
)
from command.settings import DATALAB_API_ROOT, JUPYTERHUB_USER
from command.utils import get_cell_id, parse_response


def get_user_projects(bk_username):
    """
    获取用户所有的项目信息列表

    :param bk_username: 用户名
    :return: 用户所有的项目信息列表
    """
    url = "{}/projects/mine/?bk_username={}".format(DATALAB_API_ROOT, bk_username)
    res = requests.get(url=url)
    return parse_response(res, "获取项目信息列表失败")


def get_query_tasks(project_id, project_type):
    """
    获取项目的查询任务列表

    :param project_id: 项目id
    :param project_type: 项目类型
    :return: 项目的查询任务列表
    """
    query_task_url = "{}/queries/?project_id={}&project_type={}".format(
        DATALAB_API_ROOT,
        project_id,
        project_type,
    )
    res = requests.get(url=query_task_url)
    return parse_response(res, "获取项目的查询任务列表失败")


def relate_output(notebook_id, cell_id, models, result_tables, bk_username):
    """
    笔记关联产出物

    :param notebook_id: 笔记任务id
    :param cell_id: cell_id
    :param models: 模型列表
    :param result_tables: 结果表列表
    :param bk_username: 用户名
    :return: 关联结果
    """
    url = "{}/notebooks/{}/cells/{}/outputs/".format(DATALAB_API_ROOT, notebook_id, cell_id)
    outputs = [
        {
            OUTPUT_TYPE: MODEL,
            OUTPUT_NAME: model[NAME],
            SQL: model[SQL],
        }
        for model in models
    ]
    outputs.extend(
        [
            {
                OUTPUT_TYPE: RESULT_TABLE,
                OUTPUT_NAME: result_table[NAME],
                SQL: result_table[SQL],
            }
            for result_table in result_tables
        ]
    )
    data = json.dumps({OUTPUTS: outputs, BK_USERNAME: bk_username})
    res = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(res, "关联产出物失败")


def cancel_relate_output(notebook_id, models, result_tables):
    """
    笔记取消关联产出物

    :param notebook_id: 笔记任务id
    :param models: 模型列表
    :param result_tables: 结果表列表
    :return: 取消关联结果
    """
    url = "{}/notebooks/{}/outputs/bulk/".format(DATALAB_API_ROOT, notebook_id)
    data = json.dumps({MODEL: models, RESULT_TABLE: result_tables})
    res = requests.delete(url=url, headers=HEADERS, data=data)
    return parse_response(res, "取消关联产出物失败")


def retrieve_outputs(notebook_id):
    """
    获取笔记产出物列表

    :param notebook_id: 笔记任务id
    :return: 笔记产出物列表
    """
    url = "{}/notebooks/{}/outputs/".format(DATALAB_API_ROOT, notebook_id)
    res = requests.get(url=url)
    return parse_response(res, "获取笔记产出物列表失败")


def get_notebook_id():
    """
    获取notebook_id
    """
    connection_file = ipykernel.get_connection_file()
    kernel_id = connection_file.split("-", 1)[1].split(".")[0]
    retrieve_id_url = "{}/notebooks/retrieve_id/?user={}&kernel_id={}".format(
        DATALAB_API_ROOT,
        JUPYTERHUB_USER,
        kernel_id,
    )
    response = requests.get(url=retrieve_id_url)
    return parse_response(response, "获取笔记id失败")


def create_rt(result_table_conf, bk_username, notebook_id):
    """
    创建结果表

    :param result_table_conf: 结果表配置
    :param bk_username: 用户名
    :param notebook_id: 笔记id
    :return: 创建结果
    """
    general_rt_conf = generate_general_rt_conf(result_table_conf, bk_username)
    url = "{}/notebooks/{}/cells/{}/result_tables/".format(DATALAB_API_ROOT, notebook_id, get_cell_id())
    response = requests.post(url=url, headers=HEADERS, data=json.dumps(general_rt_conf))
    return parse_response(response, "创建结果表失败")


def destroy_rt(result_table_id, notebook_id, bk_username):
    """
    删除结果表

    :param result_table_id: 结果表id
    :param notebook_id: 笔记id
    :param bk_username: 用户名
    :return: 删除结果表
    """
    url = "{}/notebooks/{}/cells/{}/result_tables/{}/".format(
        DATALAB_API_ROOT,
        notebook_id,
        get_cell_id(),
        result_table_id,
    )
    data = json.dumps(
        {
            BKDATA_AUTHENTICATION_METHOD: USER,
            BK_USERNAME: bk_username,
        }
    )
    response = requests.delete(url=url, headers=HEADERS, data=data)
    return parse_response(response, "删除结果表失败")


def generate_general_rt_conf(result_table_conf, bk_username):
    """
    生成通用的结果表配置

    :param result_table_conf: 结果表配置
    :param bk_username: 用户名
    :return: 结果表配置
    """
    input_rt = result_table_conf.get(INPUT_RT)
    output_rt = result_table_conf.get(OUTPUT_RT)
    if input_rt:
        result_table_info = get_result_table_info(input_rt)
        fields = result_table_info.get(FIELDS)
    else:
        fields = result_table_conf[FIELDS]

    general_rt_conf = {
        RESULT_TABLES: [{RESULT_TABLE_ID: output_rt, FIELDS: fields, STORAGES: result_table_conf[STORAGES]}],
        BKDATA_AUTHENTICATION_METHOD: USER,
        BK_USERNAME: bk_username,
        PROCESSING_TYPE: result_table_conf[PROCESSING_TYPE],
    }
    if input_rt:
        general_rt_conf[DATA_PROCESSINGS] = [{INPUTS: [input_rt], OUTPUTS: [output_rt], PROCESSING_ID: output_rt}]
    return general_rt_conf
