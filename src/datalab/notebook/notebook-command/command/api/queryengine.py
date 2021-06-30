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
import os
import time

import requests
from command.api.datalab import relate_output
from command.constants import (
    BK_APP_CODE,
    BK_APP_SECRET,
    BK_USERNAME,
    BKDATA_AUTHENTICATION_METHOD,
    CELL_ID,
    CREATED_RESULT_TABLE_ID,
    DATA,
    DDL_CREATE,
    DDL_CTAS,
    DML_DELETE,
    DML_SELECT,
    DML_UPDATE,
    ERROR,
    ERROR_MESSAGE,
    ERRORS,
    FAILED,
    FINISHED,
    HEADERS,
    HTTP_STATUS_OK,
    INFO,
    LIST,
    MESSAGE,
    NAME,
    NOTEBOOK_ID,
    PREFER_STORAGE,
    PROPERTIES,
    QUERY_ID,
    QUERY_LIST,
    QUERY_STATUS_WAIT_INTERVAL,
    QUERY_TABLE,
    RESULT,
    RESULT_TABLE_INFO_DICT,
    SELECT_FIELDS_ORDER,
    SHOW_SQL,
    SHOW_TABLES,
    SQL,
    STAGE_STATUS,
    STAGE_TYPE,
    STATEMENT_TYPE,
    TOTAL_RECORDS,
    TOTALRECORDS,
    USER,
    VALUE,
    VALUES,
    WRITECACHE,
)
from command.exceptions import ApiRequestException, ExecuteException
from command.settings import QUERYENGINE_API_ROOT
from command.util.result_set import ResultSet
from command.utils import extract_error_message, parse_response, transform_column_name


def submit_query(sql, bk_username, notebook_id, cell_id):
    """
    提交查询

    :param sql: sql
    :param bk_username: 用户名
    :param notebook_id: 笔记id
    :param cell_id: cell id
    :return: 执行结果
    """
    url = "%s/query_async/" % QUERYENGINE_API_ROOT
    data = json.dumps(
        {
            SQL: sql,
            PREFER_STORAGE: "",
            BK_APP_CODE: os.environ.get(BK_APP_CODE.upper()),
            BK_USERNAME: bk_username,
            BK_APP_SECRET: os.environ.get(BK_APP_SECRET.upper()),
            BKDATA_AUTHENTICATION_METHOD: USER,
            PROPERTIES: {NOTEBOOK_ID: str(notebook_id), CELL_ID: cell_id},
        }
    )
    response = requests.post(url=url, headers=HEADERS, data=data)
    if response.status_code == HTTP_STATUS_OK:
        res = response.json()
        if res[RESULT]:
            # 获取sql类型
            statement_type = res[DATA][STATEMENT_TYPE]
            query_result = QueryResult(notebook_id, cell_id, bk_username, res[DATA], sql)
            return query_result.get_query_result(statement_type)()
        else:
            if res.get(ERRORS) and res[ERRORS].get(ERROR):
                raise ExecuteException("{}: {}".format(res[MESSAGE], res[ERRORS][ERROR]))
            else:
                raise ExecuteException(res[MESSAGE])
    else:
        raise ApiRequestException("查询引擎调用失败：%s" % extract_error_message(response))


def get_query_result(query_task_id, sql, statement_type=DML_SELECT):
    """
    轮询执行状态，获取查询结果集

    :param query_task_id: queryengine id
    :param sql: sql
    :param statement_type: sql类型
    :return: 查询结果集
    """
    round_robin_stage(query_task_id)
    if statement_type == DDL_CTAS:
        return "创建成功"

    # 获取结果集信息
    dataset_info = get_dataset_info(query_task_id)
    # 获取查询结果集
    query_result = get_result(query_task_id, dataset_info[TOTAL_RECORDS])
    # 组装查询结果集
    result_set = ResultSet(
        query_result[LIST], query_result[SELECT_FIELDS_ORDER], query_result[TOTALRECORDS], sql, QUERY_TABLE
    )
    return result_set


def round_robin_stage(query_task_id):
    """
    stage三种情况：
        finished：最后一个状态是writeCache并且为finished
        failed：最后一个状态是failed
        running：除了以上两种情况的剩余情况

    :param query_task_id: queryengine的查询id
    """
    while True:
        last_stage = get_stage(query_task_id)[-1]
        if last_stage[STAGE_TYPE] == WRITECACHE and last_stage[STAGE_STATUS] == FINISHED:
            break
        elif last_stage[STAGE_STATUS] == FAILED:
            error_message = json.loads(last_stage[ERROR_MESSAGE])
            message = (
                "{}: {}".format(error_message.get(MESSAGE), error_message.get(INFO))
                if INFO in error_message
                else error_message.get(MESSAGE)
            )
            raise ExecuteException(message)
        else:
            time.sleep(QUERY_STATUS_WAIT_INTERVAL)


def get_stage(query_task_id):
    """
    获取sql执行阶段

    :param query_task_id: 查询任务id
    :return: stage信息
    """
    url = "{}/query_async/stage/{}".format(QUERYENGINE_API_ROOT, query_task_id)
    response = requests.get(url=url, headers=HEADERS, params="")
    return parse_response(response, "提取stage信息失败")


def get_sql_info(sql):
    """
    获取sql详情，包括：sql类型、sql中使用的表名

    :param sql: sql
    :return: 类型和表名
    """
    url = "%s/sqlparse/sqltype_and_result_tables/" % QUERYENGINE_API_ROOT
    data = json.dumps(
        {
            SQL: sql,
        }
    )
    response = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(response, "提取表名失败")


def get_dataset_info(query_task_id):
    """
    获取查询结果集详情

    :param query_task_id: queryengine id
    :return: 查询结果集详情
    """
    url = "{}/dataset/{}".format(QUERYENGINE_API_ROOT, query_task_id)
    res = requests.get(url=url, headers=HEADERS)
    return parse_response(res, "获取 %s 查询结果集详情失败" % query_task_id)


def get_result(query_task_id, size):
    """
    获取查询结果集

    :param query_task_id: queryengine id
    :param size: 条数
    :return: 获取查询结果集
    """
    url = "{}/query_async/result/{}?size={}".format(QUERYENGINE_API_ROOT, query_task_id, size)
    res = requests.get(url=url, headers=HEADERS)
    return parse_response(res, "获取 %s 查询结果集失败" % query_task_id)


class QueryResult(object):
    """
    根据不同的sql类型处理执行结果
    """

    def __init__(self, notebook_id, cell_id, bk_username, res_data, sql):
        self.notebook_id = notebook_id
        self.cell_id = cell_id
        self.res_data = res_data
        self.sql = sql
        self.bk_username = bk_username

    def show_tables(self):
        result_tables = transform_column_name(self.res_data.get(VALUES))
        return ResultSet(result_tables, list(RESULT_TABLE_INFO_DICT.values()), len(result_tables), self.sql, QUERY_LIST)

    def show_sql(self):
        return ResultSet([{SQL: self.res_data.get(VALUE)}], [SQL], 1, self.sql, QUERY_LIST)

    def ddl_create(self):
        return "创建成功"

    def ddl_ctas(self):
        query_task_id = self.res_data.get(QUERY_ID)
        query_result = get_query_result(query_task_id, self.sql, DDL_CTAS)
        result_tables = [{NAME: self.res_data.get(CREATED_RESULT_TABLE_ID), SQL: self.sql}]
        relate_output(self.notebook_id, self.cell_id, [], result_tables, self.bk_username)
        return query_result

    def dml_select(self):
        query_task_id = self.res_data.get(QUERY_ID)
        query_result = get_query_result(query_task_id, self.sql, self.res_data[STATEMENT_TYPE])
        return query_result

    def dml_update(self):
        return "更新操作执行成功，共更新%s条数据" % self.res_data.get(TOTALRECORDS)

    def dml_delete(self):
        return "删除操作执行成功，共删除%s条数据" % self.res_data.get(TOTALRECORDS)

    def get_query_result(self, query_type):
        return {
            DML_SELECT: self.dml_select,
            DML_UPDATE: self.dml_update,
            DML_DELETE: self.dml_delete,
            DDL_CREATE: self.ddl_create,
            DDL_CTAS: self.ddl_ctas,
            SHOW_TABLES: self.show_tables,
            SHOW_SQL: self.show_sql,
        }.get(query_type)
