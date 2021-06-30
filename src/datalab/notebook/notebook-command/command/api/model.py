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
import time

import ipykernel
import requests
from command.api.auth import check_project_object_auth, check_user_object_auth
from command.api.datalab import cancel_relate_output, relate_output, retrieve_outputs
from command.constants import (
    ADD,
    BK_RELEASE,
    BK_STATUS,
    BK_USERNAME,
    CELL_ID,
    CODE,
    DATA,
    DATALAB,
    DELETE,
    ERRORS,
    EXECUTE_MLSQL_WAIT_INTERVAL,
    HEADERS,
    HTTP_STATUS_OK,
    INLAND,
    KERNEL_ID,
    MESSAGE,
    MLSQL_EXECUTE_ERROR,
    MLSQL_TYPE_DDL,
    MLSQL_TYPE_NORMAL,
    MLSQL_TYPE_SHOW_CREATE_TABLE,
    MLSQL_TYPE_SHOW_MODELS,
    MLSQL_TYPE_SHOW_TABLES,
    MLSQL_TYPE_SHOW_TRAIN_MODEL,
    MODEL,
    MODEL_INFO_DICT,
    MODELS,
    NAME,
    NOTEBOOK_ID,
    PROJECT_ID,
    QUERY_LIST,
    READ,
    RELEASE,
    REMOVE,
    RESULT,
    RESULT_TABLE,
    RESULT_TABLE_INFO_DICT,
    RESULT_TABLES,
    SQL,
    SQL_LIST,
    STAGES,
    STATUS,
    SUCCESS,
    TAGS,
    TASK_ID,
    TYPE,
    VALUE,
)
from command.exceptions import ApiRequestException, AuthException, ExecuteException
from command.settings import DATAFLOW_API_ROOT, DATALAB_API_ROOT
from command.util.result_set import ResultSet
from command.utils import extract_error_message, parse_response, transform_column_name


def execute_mlsql(sql_list, project_id, notebook_id, cell_id, bk_username):
    """
    执行MLSQL

    :param sql_list: 提交执行的sql列表
    :param project_id: 项目id
    :param notebook_id: 笔记任务id
    :param cell_id: cell_id
    :param bk_username: 用户名
    :return: 执行详情
    """
    url = "%s/modeling/jobs/start_modeling/" % DATAFLOW_API_ROOT
    data = json.dumps(
        {
            SQL_LIST: sql_list,
            PROJECT_ID: project_id,
            NOTEBOOK_ID: notebook_id,
            CELL_ID: cell_id,
            BK_USERNAME: bk_username,
            TYPE: DATALAB,
            TAGS: [INLAND],
        }
    )
    res = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(res, "执行MLSQL失败")


def get_mlsql_status(task_id):
    """
    获取MLSQL执行状态

    :param task_id: 任务id
    :return: 执行状态
    """
    url = "{}/modeling/jobs/sync_status/?task_id={}".format(DATAFLOW_API_ROOT, task_id)
    res = requests.get(url=url)
    if res.status_code == HTTP_STATUS_OK:
        res_json = res.json()
        if res_json[RESULT]:
            return res_json[DATA]
        else:
            print(json.dumps({BK_STATUS: res_json[DATA].get(STAGES)}))
            if res_json[CODE] in MLSQL_EXECUTE_ERROR.keys():
                exists_output = res_json[ERRORS].get(NAME)
                message = MLSQL_EXECUTE_ERROR[res_json[CODE]] % exists_output
            else:
                message = res_json[MESSAGE]
            raise ExecuteException("任务id：{}; 失败原因：{}".format(task_id, message))
    else:
        raise ApiRequestException("获取MLSQL执行状态失败: %s" % extract_error_message(res))


def relate_mlsql_info(notebook_id, sql, bk_username):
    """
    关联MLSQL信息

    :param notebook_id: 笔记任务id
    :param sql: sql
    :param bk_username: 用户名
    :return: 关联结果
    """
    connection_file = ipykernel.get_connection_file()
    kernel_id = connection_file.split("-", 1)[1].split(".")[0]
    url = "%s/notebooks/relate_mlsql/" % DATALAB_API_ROOT
    data = json.dumps(
        {
            NOTEBOOK_ID: notebook_id,
            KERNEL_ID: kernel_id,
            SQL: sql,
            BK_USERNAME: bk_username,
        }
    )
    res = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(res, "关联MLSQL信息失败")


def extract_name(sql_list):
    """
    提取mlsql中的表名

    :param sql_list: sql列表
    :return: 表名
    """
    url = "%s/modeling/jobs/parse_modeling/" % DATAFLOW_API_ROOT
    data = json.dumps({SQL_LIST: sql_list})
    res = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(res, "提取mlsql表名失败")


def extract_outputs(task_id, bk_username):
    """
    提取mlsql的产出物

    :param task_id: 任务id
    :param bk_username: 用户名
    :return: 产出物
    """
    url = "{}/modeling/jobs/parse_job_outputs/?bk_username={}&task_id={}".format(
        DATAFLOW_API_ROOT,
        bk_username,
        task_id,
    )
    res = requests.get(url=url, headers=HEADERS)
    return parse_response(res, "提取mlsql产出物失败")


def mlsql_auth_check(project_id, notebook_id, bk_username, sql_list):
    """
    校验笔记内mlsql的执行权限

    :param project_id: 项目id
    :param notebook_id: 笔记id
    :param bk_username: 用户名
    :param sql_list: sql列表
    :return: 结果
    """
    # 校验用户是否有笔记执行的权限
    check_user_object_auth(bk_username, [project_id])

    # 提取mlsql中的结果表名
    input_names = extract_name(sql_list)
    # 校验项目是否有queryset类型结果表查询权限
    check_project_object_auth(project_id, input_names[READ].get(RESULT_TABLE, []))

    # 如果存在drop model或者drop table语句，校验笔记是否有产出物删除权限
    if input_names[DELETE].get(MODEL) or input_names[DELETE].get(RESULT_TABLE):
        mlsql_drop_auth_check(notebook_id, input_names)


def mlsql_drop_auth_check(notebook_id, outputs):
    """
    校验mlsql drop产出物的权限

    :param notebook_id: 笔记id
    :param outputs: 产出物
    :return: drop产出物校验结果
    """
    outputs_info = retrieve_outputs(notebook_id)
    models = [model[NAME] for model in outputs_info[MODELS]]
    result_tables = [result_table[NAME] for result_table in outputs_info[RESULT_TABLES]]
    for model_name in outputs[DELETE].get(MODEL, []):
        if model_name not in models:
            raise AuthException("此笔记没有模型 %s 的drop权限，请先执行show models查看笔记有权限删除的模型列表" % model_name)
    for result_table_name in outputs[DELETE].get(RESULT_TABLE, []):
        if result_table_name not in result_tables:
            raise AuthException("此笔记没有结果表 %s 的drop权限，请先执行show tables查看笔记有权限删除的结果表列表" % result_table_name)


def get_normal_sql_result(notebook_id, cell_id, task_id, bk_username):
    """
    获取通用mlsql执行结果

    :param notebook_id: 笔记id
    :param cell_id: cell_id
    :param task_id: mlsql任务id
    :param bk_username: 用户名
    :return: 通用mlsql执行结果
    """
    while True:
        # 获取mlsql执行状态
        mlsql_status = get_mlsql_status(task_id)
        status = mlsql_status[STATUS]
        if status == SUCCESS:
            # 获取此次mlsql执行的产出物
            outputs = extract_outputs(task_id, bk_username)
            # 更新产出物和笔记的关联关系
            update_output_relation(notebook_id, cell_id, bk_username, outputs)
            mlsql_status[BK_STATUS] = mlsql_status.pop(STAGES)
            mlsql_status[BK_RELEASE] = mlsql_status.pop(RELEASE)
            print(json.dumps(mlsql_status))
            return "执行成功"
        else:
            print(json.dumps({BK_STATUS: mlsql_status[STAGES]}))
            time.sleep(EXECUTE_MLSQL_WAIT_INTERVAL)
            continue


def update_output_relation(notebook_id, cell_id, bk_username, outputs):
    """
    更新产出物和笔记的关联关系

    :param notebook_id: 笔记id
    :param cell_id: cell id
    :param bk_username: 用户名
    :param outputs: 产出物
    :return: 更新结果
    """
    # 取消关联drop语句中的产出物
    remove_models = outputs[MODEL][REMOVE]
    remove_result_tables = outputs[RESULT_TABLE][REMOVE]
    cancel_relate_output(notebook_id, remove_models, remove_result_tables)

    # 关联create和train语句中的产出物
    add_models = outputs[MODEL][ADD]
    add_result_tables = outputs[RESULT_TABLE][ADD]
    relate_output(notebook_id, cell_id, add_models, add_result_tables, bk_username)


class MLSqlResult(object):
    """
    根据不同的mlsql类型处理执行结果
    """

    def __init__(self, notebook_id, cell_id, bk_username, res_data, sql):
        self.notebook_id = notebook_id
        self.cell_id = cell_id
        self.res_data = res_data
        self.sql = sql
        self.bk_username = bk_username

    def show_tables(self):
        result_tables = transform_column_name(self.res_data.get(VALUE))
        return ResultSet(result_tables, list(RESULT_TABLE_INFO_DICT.values()), len(result_tables), self.sql, QUERY_LIST)

    def show_models(self):
        models = transform_column_name(self.res_data.get(VALUE), MODEL)
        return ResultSet(models, list(MODEL_INFO_DICT.values()), len(models), self.sql, QUERY_LIST)

    def show_sql(self):
        return ResultSet([{SQL: self.res_data.get(VALUE)}], [SQL], 1, self.sql, QUERY_LIST)

    def normal(self):
        if self.res_data[RESULT] and self.res_data[RESULT].get(TYPE) == MLSQL_TYPE_DDL:
            remove_outputs = self.res_data[RESULT][RESULT]
            cancel_relate_output(self.notebook_id, remove_outputs.get(MODEL), remove_outputs.get(RESULT_TABLE))
        task_id = self.res_data[TASK_ID]
        return get_normal_sql_result(self.notebook_id, self.cell_id, task_id, self.bk_username)

    def ddl(self):
        # ddl类型是drop或truncate，这种语句不生成task_id，产出物直接在提交mlsql的接口中进行返回
        remove_outputs = self.res_data[RESULT]
        cancel_relate_output(self.notebook_id, remove_outputs.get(MODEL), remove_outputs.get(RESULT_TABLE))
        return self.res_data[VALUE]

    def get_mlsql_result(self, query_type):
        return {
            MLSQL_TYPE_NORMAL: self.normal,
            MLSQL_TYPE_DDL: self.ddl,
            MLSQL_TYPE_SHOW_TABLES: self.show_tables,
            MLSQL_TYPE_SHOW_MODELS: self.show_models,
            MLSQL_TYPE_SHOW_CREATE_TABLE: self.show_sql,
            MLSQL_TYPE_SHOW_TRAIN_MODEL: self.show_sql,
        }.get(query_type)
