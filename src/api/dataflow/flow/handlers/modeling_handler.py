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
)
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.shared.log import modeling_logger as logger


class ModelingDatalabTaskHandler(object):
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
            "updated_by": operator,
            "action": action,
        }
        if context is not None:
            kwargs["context"] = json.dumps(context)

        o_mlsql_task = MLSqlExecuteLog.objects.create(**kwargs)
        return cls(o_mlsql_task)

    @classmethod
    def start(cls, args):
        pass

    def execute(self):
        self.flow_task.set_status(MLSqlExecuteLog.STATUS.RUNNING)
        flow_context = json.loads(self.flow_task.context)
        args = flow_context["args"]
        bk_biz_id = args["bk_biz_id"]
        project_id = args["project_id"]
        bk_username = args["bk_username"]
        tags = args["tags"]
        notebook_id = args["notebook_id"]
        cell_id = args["cell_id"]
        component_type = args["component_type"] if "component_type" in args else "spark_mllib"
        use_type = args["type"]
        sql_list = args["sql_list"]
        processing_ids = []
        try:
            pipeline = ModelingFlowPipeLine(
                bk_biz_id,
                project_id,
                bk_username,
                tags,
                notebook_id,
                cell_id,
                component_type,
                use_type,
                sql_list,
            )
            # 创建processing
            logger.info("create processing...")
            processing_info = pipeline.create_model_processing()
            if "processing_ids" not in processing_info:
                # 解析过程正常，但没有任何process生成，表示传到bksql的是show或drop或是truncate
                # 一般情况下不会出现，但当这些语句前有/**/的注释就会出现
                message = "sql语法有误，请注意目前尚不支持多行注释"
                raise CreateModelProcessingException(message=message)
                # return Response(processing_info['content'])

            # 创建job
            logger.info("create job...")
            processing_ids = processing_info["processing_ids"]
            job_id = pipeline.create_mlsql_job(processing_info)

            # 启动job
            logger.info("start job...")
            exec_id = pipeline.start_mlsql_job(job_id)
            # 汇总返回信息, args需要扩充，所以会传入
            return_object = pipeline.merge_return_object(job_id, exec_id, args)

            # 将执行信息写入log表以备check时使用
            MLSqlExecuteLog.objects.filter(id=self.flow_task.id).update(context=json.dumps(flow_context))
            return Response(return_object)
        except CreateModelProcessingException as e:
            logger.error("创建Process失败:%s" % e)
            self.flow_task.set_status(MLSqlExecuteLog.STATUS.FAILURE)
            self.log("创建Process失败:%s" % e)
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            logger.error("运行异常:%s" % e)
            # 回滚清理操作
            notebook_info = {
                "notebook_id": notebook_id,
                "cell_id": cell_id,
                "bk_username": bk_username,
            }
            ModelingUtils.clear(processing_ids, notebook_info)
            self.flow_task.set_status(MLSqlExecuteLog.STATUS.FAILURE)
            self.log("运行异常:%s" % e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    def stop(self):
        try:
            task_args = json.loads(self.flow_task.context)["args"]
            if "exec_id" not in task_args or "job_id" not in task_args:
                return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="任务未启动")
            job_id = task_args["job_id"]
            res = ModelingFlowPipeLine.stop_mlsql_job(job_id)
            self.flow_task.set_status(MLSqlExecuteLog.STATUS.CANCELLED)
            return res
        except ApiRequestError as e:
            logger.error("cancel error:%s" % e)
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            logger.error("cancel error:%s" % e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    def check(self):
        try:
            params = json.loads(self.flow_task.context)["args"]
            if self.flow_task.status == "failure" or "exec_id" not in params or "job_id" not in params:
                code, msg = ModelingDatalabTaskHandler.match_code_and_message(self.flow_task.logs_zh)
                if not msg:
                    msg = "无法获取失败信息，请联系管理员"
                resp = DataResponse(result=False, code=code)
                resp.message = msg
                resp.data = "failed"
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
        try:
            last_line = log_line.split("\n")[-1]
            log_array = last_line.split("|")
            log_message = log_array[3]
            regex_pattern = r"(.*)\[(.+)\](.+)"
            match_object = re.match(regex_pattern, log_message)
            match_groups = match_object.groups()
            if len(match_groups) >= 3:
                code = match_groups[1]
                msg = match_groups[2]
            else:
                code = DEFAULT_MODELING_ERR
                msg = log_message
        except Exception:
            code = DEFAULT_MODELING_ERR
            msg = log_line
        return code, msg

    def clear(self):
        try:
            params = json.loads(self.flow_task.context)["args"]
            if "job_id" not in params:
                return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, message="ok")
            status = self.flow_task.status
            job_id = params["job_id"]
            bk_username = self.flow_task.created_by
            return ModelingFlowPipeLine.clear_mlsql_job(job_id, status, bk_username)
        except ApiRequestError as e:
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    def log(self, msg, level="INFO", time=None):
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.flow_task.add_log(msg, level=level, time=_time)

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
