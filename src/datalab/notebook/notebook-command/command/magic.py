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

from command.api.auth import check_project_object_auth, check_user_object_auth
from command.api.datalab import get_notebook_id
from command.api.model import (
    MLSqlResult,
    execute_mlsql,
    extract_name,
    mlsql_auth_check,
    relate_mlsql_info,
)
from command.api.queryengine import get_sql_info, submit_query
from command.api.spark import (
    make_free_session,
    round_robin_code_res,
    run_code,
    set_spark_context,
)
from command.base_logger import BaseLogger
from command.constants import (
    BKSQL,
    DML_DELETE,
    DML_UPDATE,
    ID,
    MLSQL,
    NOTEBOOK,
    READ,
    RESULT_TABLE,
    RESULT_TABLE_IDS,
    RESULT_TABLE_QUERY,
    RESULT_TABLE_UPDATE,
    SPARK,
    STATEMENT_TYPE,
    TYPE,
)
from command.exceptions import AuthException, MagicException
from command.settings import JUPYTERHUB_USER
from command.utils import get_bk_username, get_cell_id
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    magics_class,
    needs_local_scope,
)


@magics_class
class BkSqlMagic(Magics):
    @needs_local_scope
    @line_cell_magic(BKSQL)
    @BaseLogger.save_log_deco(content_type=BKSQL)
    def execute(self, line, cell=None, local_ns={}):
        sql = extract_sql(line, cell, local_ns)
        bk_username = get_bk_username()
        result_table_ids = []
        try:
            # 获取sql信息，包括sql类型和sql中引用的结果表列表
            sql_info = get_sql_info(sql)
            result_table_ids, statement_type = sql_info[RESULT_TABLE_IDS], sql_info[STATEMENT_TYPE].lower()

            # jupyterhub的启动账号分两种："项目"是以project_id作为启动账号，"个人"是以bk_username作为启动账号
            if JUPYTERHUB_USER.isdigit():
                project_id = JUPYTERHUB_USER
                action_id = RESULT_TABLE_UPDATE if statement_type in [DML_UPDATE, DML_DELETE] else RESULT_TABLE_QUERY
                # 校验项目有没有结果表相应动作的权限
                check_project_object_auth(project_id, result_table_ids, action_id)
                # 校验用户有没有项目执行的权限
                check_user_object_auth(bk_username, [project_id])
            else:
                # update和delete操作不能在个人下执行
                if statement_type in [DML_UPDATE, DML_DELETE]:
                    raise AuthException('暂时不支持在"个人"中执行数据更新和删除操作，请在"项目"下的笔记内使用')
            query_result = submit_query(sql, bk_username, get_notebook_id(), get_cell_id())
            return dict(data=query_result, content=sql, result_table_ids=result_table_ids)
        except MagicException as e:
            return dict(message=str(e), content=sql, result_table_ids=result_table_ids, result=False)


@magics_class
class SparkMagic(Magics):
    @needs_local_scope
    @cell_magic(SPARK)
    @BaseLogger.save_log_deco(content_type=SPARK)
    def execute(self, line, cell=None, local_ns={}):
        code = extract_sql(line, cell, local_ns)
        bk_username = get_bk_username()
        try:
            if JUPYTERHUB_USER.isdigit():
                project_id = JUPYTERHUB_USER
                check_user_object_auth(bk_username, [project_id])
            else:
                raise AuthException("暂时不支持在个人项目中使用spark，请在公共项目笔记内使用")

            # 获取笔记的notebook_id作为server_id
            notebook_id = get_notebook_id()
            server_id = "{}_{}".format(NOTEBOOK, notebook_id)

            make_free_session(server_id, bk_username)
            # 设置spark上下文，执行代码
            spark_context = set_spark_context(local_ns, code)
            run_code_result = run_code(server_id, code, spark_context)
            task_id = run_code_result[ID]
            code_result = round_robin_code_res(server_id, task_id)
            return dict(data=code_result, content=code)
        except MagicException as e:
            return dict(message=str(e), content=code, result=False)


@magics_class
class MLSqlMagic(Magics):
    @needs_local_scope
    @cell_magic(MLSQL)
    @BaseLogger.save_log_deco(content_type=MLSQL)
    def execute(self, line, cell=None, local_ns={}):
        sql = extract_sql(line, cell, local_ns)
        sql_list = [sql_line.lstrip() for sql_line in sql.rstrip().split(";")]
        bk_username = get_bk_username()
        result_table_ids = []
        try:
            # 获取笔记id
            notebook_id = get_notebook_id()
            result_table_ids = extract_name(sql_list)[READ].get(RESULT_TABLE, [])
            # jupyterhub的启动账号分两种："项目"是以project_id作为启动账号，"个人"是以bk_username作为启动账号
            if JUPYTERHUB_USER.isdigit():
                project_id = JUPYTERHUB_USER
                mlsql_auth_check(project_id, notebook_id, bk_username, sql_list)
            else:
                raise AuthException("暂时不支持在个人下使用mlsql，请在项目的笔记内使用")
            # 记录mlsql执行信息
            relate_mlsql_info(notebook_id, sql, bk_username)
            # 执行mlsql
            cell_id = get_cell_id()
            execute_info = execute_mlsql(sql_list, project_id, notebook_id, cell_id, bk_username)
            # 获取sql类型
            statement_type = execute_info[TYPE]
            mlsql_result = MLSqlResult(notebook_id, cell_id, bk_username, execute_info, sql).get_mlsql_result(
                statement_type
            )()
            return dict(data=mlsql_result, content=sql, result_table_ids=result_table_ids)
        except MagicException as e:
            return dict(message=str(e), content=sql, result_table_ids=result_table_ids, result=False)


def extract_sql(line, cell, local_ns):
    if cell:
        pattern = r"\{(.*?)\}"
        pattern_vars = re.findall(pattern, cell)
        for var in pattern_vars:
            if var in local_ns.keys():
                cell = cell.replace("{%s}" % var, local_ns[var])
        return cell
    else:
        return line
