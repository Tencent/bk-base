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
from datetime import datetime

from common.django_utils import DataResponse
from common.exceptions import ApiRequestError
from rest_framework.response import Response

from dataflow.flow.handlers.modeling_pipeline import ModelingFlowPipeLine
from dataflow.modeling.exceptions.comp_exceptions import (
    DEFAULT_MODELING_ERR,
    DEFAULT_SUCCESS_CODE,
    CreateModelProcessingException,
    EntityAlreadyExistsError,
    ModelCreateError,
    ModelingCode,
    RTCreateError,
    SQLForbiddenError,
    SqlParseError,
    TableNotQuerySetError,
)
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.job.modeling_job_pipeline import ModelingJobPipeLine
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper


class ModelingDatalabTaskHandler(object):
    STAGES = [
        {
            "stage": "parse",
            "stage_zh": "SQL解析",
            "description": "Parse sql and create processing.",
        },
        {"stage": "submit", "stage_zh": "提交任务", "description": "Submit mlsql job."},
        {"stage": "execute", "stage_zh": "任务执行", "description": "Execute mlsql job."},
        {"stage": "evaluate", "stage_zh": "获取结果", "description": "Evaluate mlsql job."},
    ]

    def __init__(self, task):
        self.flow_task = task

    @classmethod
    def create(
        cls,
        session_id,
        cell_id,
        notebook_id,
        operator,
        action,
        context=None,
        version=None,
    ):
        kwargs = {
            "session_id": session_id,
            "cell_id": cell_id,
            "notebook_id": notebook_id,
            "created_by": operator,
            "action": action,
        }
        if context is not None:
            kwargs["context"] = json.dumps(context)

        o_mlsql_task = MLSqlExecuteLog.objects.create(**kwargs)
        return cls(o_mlsql_task)

    @classmethod
    def start(cls, args):
        pass

    def format_stage(self, current_stage, action="start", level="INFO"):
        stage_count = len(ModelingDatalabTaskHandler.STAGES)
        stage_info = "{current_stage}/{total_stage}".format(current_stage=current_stage, total_stage=stage_count)
        return stage_info

    def execute(self):
        self.flow_task.set_status(MLSqlExecuteLog.STATUS.RUNNING)
        flow_context = json.loads(self.flow_task.context)
        args = flow_context["args"]
        bk_biz_id = args["bk_biz_id"]
        project_id = args["project_id"]
        bk_username = args["bk_username"]
        tags = [TagHelper.get_geog_area_code_by_project(project_id)]
        notebook_id = args["notebook_id"]
        cell_id = args["cell_id"]
        component_type = args["component_type"] if "component_type" in args else "spark_mllib"
        use_type = args["type"]
        sql_list = args["sql_list"]
        processing_ids = []
        stage_index = 1
        try:
            processing_params = {
                "sql_list": sql_list,
                "component_type": component_type,
                "project_id": project_id,
                "bk_biz_id": bk_biz_id,
                "notebook_id": notebook_id,
                "cell_id": cell_id,
                "bk_username": bk_username,
                "tags": tags,
                "type": use_type,
            }

            # 创建processing
            logger.info("create processing...")
            self.add_log(stage_index, status=MLSqlExecuteLog.STATUS.RUNNING)
            processing_info = ModelingJobPipeLine.create_model_processing(processing_params)
            if "processing_ids" not in processing_info:
                # 解析过程正常，但没有任何process生成，表示传到bksql的是show或drop或是truncate
                # 一般情况下不会出现，但当这些语句前有/**/的注释就会出现
                message = "sql语法有误，请注意目前尚不支持多行注释"
                raise CreateModelProcessingException(message=message)
                # return Response(processing_info['content'])
            logger.info("processing result:" + json.dumps(processing_info))
            if "contains_create_run_sql" in processing_info:
                # 我们需要写入一个标记，即是否有create_run语句，以供前端来判断显示是否有发布按钮
                args["contains_create_run_sql"] = processing_info["contains_create_run_sql"]
            if "disable" in processing_info:
                args["disable"] = processing_info["disable"]
                if processing_info["disable"]:
                    args["disable_message"] = "不支持含有Join子查询的模型进行发布：{processing_id}".format(
                        processing_id=processing_info["sub_query_processing_id"]
                    )
            if "evaluate_map" in processing_info and processing_info["evaluate_map"]:
                evaluate_map = processing_info["evaluate_map"]
                last_table = processing_info["result_table_ids"][-1]
                args["need_evaluate"] = True
                args["evaluate_map"] = evaluate_map
                args["last_table"] = last_table
                # 将评估信息写入日志表
                MLSqlExecuteLogHandler.update_execute_log(self.flow_task.id, flow_context)

            self.add_log(stage_index, status=MLSqlExecuteLog.STATUS.SUCCESS)

            # 创建job
            stage_index = stage_index + 1
            logger.info("create job...")
            self.add_log(stage_index, status=MLSqlExecuteLog.STATUS.RUNNING)
            processing_ids = processing_info["processing_ids"]
            job_params = {
                "project_id": project_id,
                "use_type": use_type,
                "component_type": component_type,
                "notebook_id": notebook_id,
                "cell_id": cell_id,
            }
            job_id = ModelingJobPipeLine.create_mlsql_job(processing_info, job_params)
            self.add_log(stage_index, status=MLSqlExecuteLog.STATUS.SUCCESS)

            # 启动job
            stage_index = stage_index + 1
            self.add_log(stage_index, status=MLSqlExecuteLog.STATUS.RUNNING)
            pipeline = ModelingJobPipeLine(job_id)
            logger.info("start job...")
            exec_id = pipeline.start_mlsql_job()

            # 汇总返回信息, args需要扩充，所以会传入
            stage_index = stage_index + 1
            return_object = pipeline.merge_return_object(exec_id)
            args["exec_id"] = exec_id
            args["job_id"] = pipeline.job.job_id
            args["geog_area_code"] = pipeline.geog_area_code
            args["cluster_id"] = pipeline.cluster_id

            # 将执行信息写入log表以备check时使用
            MLSqlExecuteLogHandler.update_execute_log(self.flow_task.id, flow_context)
            return Response(return_object)
        except (
            CreateModelProcessingException,
            EntityAlreadyExistsError,
            SqlParseError,
            ModelCreateError,
            RTCreateError,
            SQLForbiddenError,
            TableNotQuerySetError,
        ) as e:
            logger.error("创建Process失败:%s" % e)
            self.flow_task.set_status(MLSqlExecuteLog.STATUS.FAILURE)
            self.add_log(
                stage_index,
                level="ERROR",
                detail="创建Process失败:%s" % e,
                status=MLSqlExecuteLog.STATUS.FAILURE,
            )
            return DataResponse(result=False, code=e.code, message=e.message, errors=e.errors)
        except Exception as e:
            logger.error("运行异常", e)
            # 回滚清理操作
            notebook_info = {
                "notebook_id": notebook_id,
                "cell_id": cell_id,
                "bk_username": bk_username,
            }
            ModelingUtils.clear(processing_ids, notebook_info)
            self.flow_task.set_status(MLSqlExecuteLog.STATUS.FAILURE)
            self.add_log(
                stage_index,
                level="ERROR",
                detail="运行异常:%s" % e,
                status=MLSqlExecuteLog.STATUS.FAILURE,
            )
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    def add_log(
        self,
        stage_index,
        level="INFO",
        detail="",
        status=MLSqlExecuteLog.STATUS.RUNNING,
    ):
        self.log(
            ModelingDatalabTaskHandler.STAGES[stage_index - 1]["description"],
            level=level,
            stage=self.format_stage(stage_index),
            detail=detail,
            status=status,
        )

    def check(self):
        try:
            params = json.loads(self.flow_task.context)["args"]
            if self.flow_task.status == "failure" or "exec_id" not in params or "job_id" not in params:
                code, msg, errors = ModelingDatalabTaskHandler.match_code_and_message(self.flow_task.logs_zh)
                if not msg:
                    msg = "无法获取失败信息，请联系管理员"
                resp = DataResponse(result=False, code=code)
                resp.message = msg
                resp.data = "failed"
                resp.errors = errors
                return resp
            job_id = params["job_id"]
            res = ModelingFlowPipeLine.check_mlsql_job(job_id)
            final_resp = DataResponse(message=res.message)
            if res.data == "failed":
                self.flow_task.set_status(MLSqlExecuteLog.STATUS.FAILURE)
                final_resp.result = False
                final_resp.data = res.data
                final_resp.message = res.message
                final_resp.code = res.code
            elif res.data == "finished":
                self.flow_task.set_status(MLSqlExecuteLog.STATUS.SUCCESS)
                final_resp.result = True
                final_resp.data = res.data
                final_resp.message = res.message
            elif res.data == "killed":
                self.flow_task.set_status(MLSqlExecuteLog.STATUS.CANCELLED)
                final_resp.result = True
                final_resp.data = res.data
                final_resp.message = res.message
            else:
                # still running, do nothing
                final_resp.result = True
                final_resp.data = res.data
                final_resp.message = res.message
            return final_resp
        except ApiRequestError as e:
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @classmethod
    def match_code_and_message(cls, log_line):
        log_message = "Unknown error"
        try:
            log_lines = log_line.split("\n")
            last_line = log_lines[-1]
            log_array = last_line.split("|")
            log_message = log_array[-1]
            regex_pattern = r"(.*)\[(.+)\](.+)"
            match_object = re.match(regex_pattern, log_message)
            match_groups = match_object.groups()
            if len(match_groups) >= 3:
                code = match_groups[1]
                msg = match_groups[2]
                mlsql_code = code[-3:]
                if mlsql_code in [
                    ModelingCode.MODEL_EXISTS_ERR[0],
                    ModelingCode.TABLE_EXISTS_ERR[0],
                    ModelingCode.DP_EXISTS_ERR[0],
                ]:
                    # 实体不存在的异常时，需要解析出具体哪些实体不存在
                    error_regex_pattern = r"(.*):(.*):(.*)"
                    error_match_object = re.match(error_regex_pattern, msg)
                    error_match_group = error_match_object.groups()
                    error_entity = error_match_group[-1]
                    errors = {"name": error_entity}
                else:
                    errors = None
            else:
                code = DEFAULT_MODELING_ERR
                msg = log_message
                errors = None
        except Exception as e:
            logger.exception(e)
            code = DEFAULT_MODELING_ERR
            msg = log_message
            errors = None
        return code, msg, errors

    def clear(self):
        try:
            params = json.loads(self.flow_task.context)["args"]
            if "job_id" not in params:
                return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE)
            status = self.flow_task.status
            job_id = params["job_id"]
            return ModelingFlowPipeLine.clear_mlsql_job(job_id, status)
        except ApiRequestError as e:
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    def log(self, msg, level="INFO", time=None, stage=None, detail="", status=None):
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.flow_task.add_log(msg, level=level, time=_time, stage=stage, detail=detail, status=status)

    @staticmethod
    def get_task_id(notebook_id):
        return_obj = {}
        result = MLSqlExecuteLog.objects.filter(notebook_id=notebook_id).order_by("-id").first()
        if result:
            return_obj["task_id"] = result.id
            status = result.status
            last_status = status
            if status == "cancelled":
                last_status = "killed"
            elif status == "success":
                last_status = "finished"
            elif status == "failure":
                last_status = "failed"
            return_obj["status"] = last_status

        return return_obj
