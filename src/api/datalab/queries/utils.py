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

from common.auth.perms import ProjectPerm
from common.local import get_request_app_code, get_request_username
from datalab.api.queryengine import QueryEngineApi
from datalab.constants import (
    BK_APP_CODE,
    BK_APP_SECRET,
    BK_USERNAME,
    BKDATA_AUTHENTICATION_METHOD,
    COMMON,
    DML_SELECT,
    PREFER_STORAGE,
    QUERY_NAME,
    QUERY_PATTERN,
    RESULT_TABLE_IDS,
    SQL,
    STATEMENT_TYPE,
    USER,
)
from datalab.queries.models import DatalabQueryTaskModel
from django.utils.translation import ugettext as _


def create_query_name(objs):
    """
    创建查询任务名，判断有没有满足 query_x 形式的任务名，如果有的话，新创建的查询任务名为 query_max(x)+1，否则为query_1

    :param objs: DatalabQueryTaskModel objs
    :return: 查询任务名
    """
    max_query_num = 0
    for obj in objs:
        if re.match(QUERY_PATTERN, obj[QUERY_NAME]):
            max_query_num = max(max_query_num, int(obj[QUERY_NAME].split("_")[1]))
    return "query_" + str(max_query_num + 1) if max_query_num else "query_1"


def check_query_name_exists(project_id, query_name):
    """
    判断此项目下是否存在同名查询任务

    :param project_id: 项目id
    :param query_name: 查询任务名
    """
    task_objs = list(DatalabQueryTaskModel.objects.filter(project_id=project_id).values())
    for obj in task_objs:
        if obj[QUERY_NAME] == query_name:
            raise Exception(_("此项目下已存在同名查询任务"))


def sql_query(params, query_task_obj):
    """
    sql查询

    :param params: 请求参数
    :param query_task_obj: DatalabQueryTaskModel obj
    :return: 查询结果
    """
    res = QueryEngineApi.get_sqltype_and_result_tables({SQL: params[SQL]}, raise_exception=True)
    if res.data[STATEMENT_TYPE] != DML_SELECT:
        raise Exception(_("查询页面只支持检索数据，数据修改等操作请在笔记下执行"))

    if query_task_obj.project_type == COMMON:
        for _rt in res.data[RESULT_TABLE_IDS]:
            auth_res = ProjectPerm(query_task_obj.project_id).check_data(_rt)
            if not auth_res:
                raise Exception(_("该项目没有结果表 %s 的查询权限，请先给项目申请结果表权限") % _rt)

    response = QueryEngineApi.query_async(
        {
            SQL: params[SQL],
            PREFER_STORAGE: "",
            BKDATA_AUTHENTICATION_METHOD: USER,
            BK_USERNAME: get_request_username(),
            BK_APP_CODE: get_request_app_code(),
            BK_APP_SECRET: params.get(BK_APP_SECRET, ""),
        }
    )
    return response
