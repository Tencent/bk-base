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

from common.django_utils import DataResponse
from rest_framework.response import Response

from dataflow.flow.handlers.modeling_handler import ModelingDatalabTaskHandler
from dataflow.flow.tasks import start_modeling_job, stop_model_job
from dataflow.flow.utils.modeling_params import DatalabParams
from dataflow.modeling.exceptions.comp_exceptions import (
    DEFAULT_MODELING_ERR,
    DEFAULT_SUCCESS_CODE,
    DDLSqlExecuteError,
    SQLNotSupportedError,
    SqlParseError,
)
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.shared.api.modules.modeling import ModelingAPI
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.modeling.modeling_helper import ModelingHelper


class ModelingController(object):
    def __init__(self, task_id):
        self.task = MLSqlExecuteLogHandler.get(task_id)

    def kill(self):
        stop_model_job(self.task)
        return Response(self.task.id)

    def check(self):
        task_handle = ModelingDatalabTaskHandler(self.task)
        return task_handle.check()

    def clear(self):
        task_handle = ModelingDatalabTaskHandler(self.task)
        return task_handle.clear()

    @classmethod
    def start(cls, exec_params):
        operator = FlowModelingOperator(exec_params)
        ddl_result = {}
        normal_list, ddl_list = operator.split_sql_list(exec_params["sql_list"])
        if not normal_list and not ddl_list:
            # 没有任何sql可执行
            result = {"type": "ddl", "value": "执行完成"}
            return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, data=result)

        if ddl_list:
            ddl_result = operator.execute_ddl_sql(ddl_list)
        if not normal_list:
            return ddl_result
        else:
            task_id = operator.start_job(normal_list)
            # task_id = operator.start_job(exec_params['sql_list'])
            result = {"type": "normal", "task_id": task_id, "result": ddl_result}
            return Response(result)

    @classmethod
    def parse(cls, sql_list):
        return FlowModelingOperator.parse_job(sql_list)

    @classmethod
    def get_task_id(cls, notebook_id):
        return_obj = {}
        result = MLSqlExecuteLog.objects.filter(notebook_id=notebook_id).order_by("-id").first()
        if result:
            return_obj["task_id"] = result.id
            return_obj["status"] = result.status
        return return_obj


class FlowModelingOperator(object):
    def __init__(self, params):
        use_type = "datalab" if "type" not in params else params["type"]
        params["use_type"] = use_type
        if "component_type" not in params:
            params["component_type"] = "spark_mllib"
        if use_type == "datalab":
            datalab_params = DatalabParams(params)
            self.operator = ModelingDatalabOperator(datalab_params)

    def start_job(self, sql_list):
        return self.operator.start_job(sql_list)

    @classmethod
    def parse_job(cls, sql_list):
        normal_list, ddl_list = cls.split_sql_list(sql_list)
        if not normal_list:
            empty_res = {"read": [], "write": []}
            return Response(empty_res)
        parse_args = {"sql_list": normal_list}
        parse_result = ModelingAPI.processing.parse_mlsql_tables(parse_args)
        try:
            res_util.check_response(parse_result)
            return Response(parse_result.data)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e), data="{}".format(e))

    def execute_ddl_sql(self, sql_list):
        return self.operator.execute_ddl_sql(sql_list)

    @classmethod
    def is_ddl_or_show(cls, sql):
        if sql.startswith("show") or sql.startswith("truncate") or sql.startswith("drop"):
            return True
        else:
            return False

    @classmethod
    def split_sql_list(cls, sql_list):
        """
        注意：分开两类sql语句
             这里加了目前执行sql的限制，普通sql(train,run)及其它sql(show drop等)，
             目前仅支持在普通sql前执行其它sql
        """
        ddl_list = []
        normal_list = []
        comment_regex = r"--.*"
        for sql in sql_list:
            if not sql:
                continue
            # 去除原始语句两边的空格
            sql = sql.strip()
            # 去除单条注释内容
            sql = re.sub(comment_regex, "", sql, 0)
            # 替换后仍旧可能出现回车等，所以需要再次strip
            sql = sql.strip()
            if not sql:
                continue
            if cls.is_ddl_or_show(sql):
                if normal_list:
                    # 当前已经有了普通sql，不能再有其它ddl类的sql
                    raise SQLNotSupportedError()
                ddl_list.append(sql)
            else:
                normal_list.append(sql)
        return normal_list, ddl_list


class ModelingDatalabOperator(object):
    def __init__(self, params):
        self.bk_biz_id = params.bk_biz_id
        self.project_id = params.project_id
        self.bk_username = params.bk_username
        self.tags = params.tags
        self.notebook_id = params.notebook_id
        self.cell_id = params.cell_id
        self.component_type = params.component_type
        self.use_type = params.use_type
        self.message_args = {"consuming_mode": "continue"}
        self.common_args = {
            "bk_biz_id": self.bk_biz_id,
            "project_id": self.project_id,
            "type": self.use_type,
            "bk_username": self.bk_username,
            "tags": json.dumps(self.tags),
            "notebook_id": self.notebook_id,
            "cell_id": self.cell_id,
        }

    def start_job(self, sql_list):
        consuming_mode = (
            "continue" if "consuming_mode" not in self.message_args else self.message_args["consuming_mode"]
        )
        cluster_group = None if "cluster_group" not in self.message_args else self.message_args["cluster_group"]
        args = {}
        args.update(self.common_args)
        args["sql_list"] = sql_list
        context = {
            "consuming_mode": consuming_mode,
            "cluster_group": cluster_group,
            "progress": 0.0,
            "args": args,
        }
        session_id = self.common_args["session_id"] if "session_id" in self.common_args else "mock_session"
        cell_id = self.common_args["cell_id"] if "cell_id" in self.common_args else "mock_cell"
        notebook_id = self.common_args["notebook_id"] if "notebook_id" in self.common_args else 0
        task = ModelingDatalabTaskHandler.create(
            session_id=session_id,
            cell_id=cell_id,
            notebook_id=notebook_id,
            operator=self.common_args["bk_username"],
            action="start",
            context=context,
            version="0.1",
        )
        task_id = start_modeling_job(task)
        return task_id

    def is_ddl_sql(self, sql_list):
        has_ddl = False
        has_sql = False
        all_ddl = True
        for sql in sql_list:
            if sql.startswith("show") or sql.startswith("truncate") or sql.startswith("drop"):
                has_ddl = True
            else:
                has_sql = True
                all_ddl = False
        if has_ddl and has_sql:
            raise SQLNotSupportedError()
        return all_ddl

    def execute_single_sql(self, ind, ddl_type, ddl_data):
        if ddl_type == "show":
            return ModelingHelper.show_models(self.notebook_id, ddl_data=ddl_data)
        elif ddl_type == "drop_model":
            return ModelingHelper.delete_model(ddl_data)
        elif ddl_type == "drop_table":
            return ModelingHelper.drop_table(ddl_data, self.notebook_id, self.cell_id)
        elif ddl_type == "truncate":
            return ModelingHelper.truncate_table(ddl_data, self.notebook_id, self.cell_id)
        else:
            raise SqlParseError(message_kv={"number": ind, "content": "Not supported operation"})

    def execute_ddl_sql(self, sql_list):
        last_result = None
        sql_index = 0
        drop_result = {"model": [], "query_set": []}
        for sql in sql_list:
            sql_index = sql_index + 1
            ddl_args = {
                "tags": self.tags,
                "sql_list": [sql],
                "component_type": self.component_type,
                "type": "datalab",
                "project_id": self.project_id,
                "bk_biz_id": self.bk_biz_id,
                "notebook_id": self.notebook_id,
                "cell_id": self.cell_id,
                "bk_username": self.bk_username,
            }
            process_result = ModelingHelper.create_model_processing(ddl_args)
            # process_result = ModelingProcessController.parse_ddl_sql(sql)
            content = process_result["content"]
            ddl_type = content["type"]
            ddl_data = content["data"] if "data" in content else None
            if_exists = content["ifExists"] if "ifExists" in content else None
            try:
                last_result = self.execute_single_sql(sql_index, ddl_type, ddl_data)
                if ddl_type == "drop_model":
                    drop_result["model"].append(ddl_data)
                elif ddl_type == "drop_table":
                    drop_result["query_set"].append(ddl_data)
            except DDLSqlExecuteError as e:
                # 模型不存在或无权限的时候，根据ifexists决定是否报错
                # 其它条件下必须报错
                if e.code == DEFAULT_MODELING_ERR or not if_exists:
                    new_message = "第" + str(sql_index) + "个sql执行异常:" + e.message
                    raise DDLSqlExecuteError(message_kv={"content": new_message})
                else:
                    last_result = "执行完成"
            except Exception as e:
                logger.exception(e)
                raise e

        op_type = "ddl"
        if isinstance(last_result, list):
            op_type = "show"
        result = {"type": op_type, "value": last_result, "result": drop_result}
        resp = DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, data=result)
        return resp
