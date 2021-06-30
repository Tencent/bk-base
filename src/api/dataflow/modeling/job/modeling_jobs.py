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
from common.django_utils import DataResponse
from common.local import get_request_username

from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.modeling.exceptions.comp_exceptions import DEFAULT_MODELING_ERR, DEFAULT_SUCCESS_CODE, KillTaskError
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.job.tensorflow.tensorflow_job_driver import TensorFlowJobDriver
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.pizza_settings import BASE_DATAFLOW_URL, BASE_JOBNAVI_URL, MLSQL_NODE_LABEL
from dataflow.shared.flow.flow_helper import FlowHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import modeling_logger as logger

SYSTEM_ERROR_MSG = "系统执行异常，请联系管理员"


class SparkMLLibDatalabOperator(object):
    def __init__(self, job_id, notebook_id, cell_id, jobserver_config):
        self.job_id = job_id
        self.notebook_id = notebook_id
        self.cell_id = cell_id
        self.jobserver_config = jobserver_config

    def prepare_job(self):
        cluster_id = self.jobserver_config["cluster_id"]
        component_type = "spark_mllib"
        url = BASE_DATAFLOW_URL
        # url = url.rstrip('/').rstrip('flow')
        extra_info = {
            "job_id": self.job_id,
            "job_type": component_type,
            "run_mode": "product",
            "api_url": {"base_dataflow_url": url},
            "jobnavi": {},
        }
        params = {
            "schedule_id": self.job_id,
            "description": self.job_id,
            "type_id": extra_info["job_type"],
            "active": True,
            "exec_oncreate": True,
            "extra_info": json.dumps(extra_info),
            "cluster_id": cluster_id,
            "node_label": MLSQL_NODE_LABEL,
        }
        return params

    def stop_job(self, exec_id):
        try:
            geog_area_code = self.jobserver_config["geog_area_code"]
            cluster_id = self.jobserver_config["cluster_id"]
            jobnavi = JobNaviHelper(geog_area_code, cluster_id)
            # 停止任务
            data = jobnavi.get_execute_status(exec_id)
            if data and (data["status"] == "running" or data["status"] == "preparing"):
                jobnavi.kill_execute(exec_id)
                # 删除调度信息
                self.delete_scheduler_info()
                return exec_id
            else:
                raise Exception("任务已经停止")
        except Exception as e:
            logger.error("cancel error:%s" % e)
            raise KillTaskError(message_kv={"content": "{}".format(e)})

    def delete_scheduler_info(self):
        url = BASE_JOBNAVI_URL + "cluster/${cluster}/scheduler/${job_id}/".format(
            cluster=self.jobserver_config["cluster_id"], job_id=self.job_id
        )
        requests.delete(url)

    def check_job(self, exec_id):
        check_result = {}
        # 任务提交至Yarn执行
        status_data = {}
        try:
            status_data = self.get_job_status(exec_id)
        except Exception as e:
            logger.error("check jobnavi status error:%s" % e)
            logger.exception(e)
            # 如果检查状态过程出错，那么返回状态running，这样前端可以继续发起重试
            status_data["status"] = "running"
        status = status_data["status"]
        check_result["data"] = status
        # 关注三种最终状态
        if status == "failed":
            check_result["code"] = DEFAULT_MODELING_ERR
            check_result["result"] = False
            default_message = "未能获取错误信息"
            if "info" in status_data and status_data["info"]:
                default_message = status_data["info"]
            check_result["message"] = self.get_runtime_exception(self.job_id, exec_id, default_message)
        elif status == "killed":
            check_result["code"] = DEFAULT_MODELING_ERR
            check_result["result"] = False
            check_result["message"] = "killed"
        elif status == "finished":
            check_result["code"] = DEFAULT_SUCCESS_CODE
            check_result["result"] = True
            check_result["message"] = "ok"
        else:
            # still running
            running_info = {}
            if "execute_info" in status_data:
                running_info = status_data["execute_info"]
            logger.info(
                "job {job_id} is running,  info:{running_info}".format(
                    job_id=self.job_id, running_info=json.dumps(running_info)
                )
            )
            check_result["code"] = DEFAULT_SUCCESS_CODE
            check_result["result"] = True
            check_result["message"] = "ok"
        return check_result

    def get_runtime_exception(self, job_id, exec_id, default_msg=None):
        try:
            res_data = FlowHelper.get_flow_job_submit_log_file_size(exec_id, job_id=job_id)
            file_size = int(res_data["file_size"])
            start = 1
            end = start + 10000
            error_msg = None
            while not error_msg and start < file_size:
                search_words = "RuntimeException ModelException"
                log_info = FlowHelper.get_flow_job_submit_log(
                    exec_id,
                    start,
                    end,
                    search_words=search_words,
                    job_id=job_id,
                    enable_errors=True,
                )
                if "log_data" in log_info and log_info["log_data"]:
                    log_data = log_info["log_data"][0]
                    if "inner_log_data" in log_data and log_data["inner_log_data"]:
                        inner_log = log_data["inner_log_data"][0]
                        content = inner_log["log_content"]
                        if "ModelException" in content:
                            error_msg = self.refractor_error_msg(content)
                        else:
                            logger.error(content)
                            error_msg = SYSTEM_ERROR_MSG
                        break
                start = log_info["end"]
                end = start + 10000
            if not error_msg:
                error_msg = default_msg
            return error_msg
        except Exception as e:
            logger.error("未能获取错误信息：%s" % e)
            return default_msg

    def refractor_error_msg(self, line):
        regex = r"ModelException.*\[([0-9]+)\]\s+(.*)"
        match_obj = re.search(regex, line)
        match_groups = match_obj.groups()
        if len(match_groups) < 2:
            return SYSTEM_ERROR_MSG
        else:
            code = match_groups[0]
            msg = match_groups[1]
            type_label = int(code[-2])
            if type_label < 8:
                return msg
            else:
                return SYSTEM_ERROR_MSG
        return line

    def clear_job(self, status, bk_username):
        notebook_info = {
            "notebook_id": self.notebook_id,
            "cell_id": self.cell_id,
            "bk_username": bk_username,
        }
        processings = ProcessingBatchInfo.objects.filter(batch_id=self.job_id)
        processing_list = list(processings)
        processing_ids = []
        for processing in processing_list:
            processing_ids.append(processing.processing_id)
        # 关注三种最终状态
        if status == MLSqlExecuteLog.STATUS.FAILURE:
            ModelingUtils.clear(processing_ids, notebook_info)
        elif status == MLSqlExecuteLog.STATUS.CANCELLED:
            ModelingUtils.clear(processing_ids, notebook_info)
        elif status == MLSqlExecuteLog.STATUS.SUCCESS:
            ModelingUtils.update_model_status(processing_ids)
        return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, data=None, message="ok")

    @classmethod
    def get_task_info(cls, task_id):
        mlsql_task = MLSqlExecuteLogHandler.get(task_id)
        return mlsql_task

    def get_job_status(self, exec_id):
        jobnavi_helper = JobNaviHelper(self.jobserver_config["geog_area_code"], self.jobserver_config["cluster_id"])
        res = jobnavi_helper.get_execute_status(exec_id)
        return res

    def submit_job(self, params):
        try:
            register_job_info = ProcessingJobInfoHandler.get_processing_job_info(self.job_id)
            jobnavi_helper = JobNaviHelper(self.jobserver_config["geog_area_code"], self.jobserver_config["cluster_id"])
            res_data = jobnavi_helper.create_schedule_info(params)
            submit_args = {"exec_id": int(res_data)}
            current_time_mills = int(time.time() * 1000)
            ProcessingBatchJobHandler.save_proc_batch_job(
                self.job_id,
                register_job_info,
                current_time_mills,
                register_job_info.created_by,
                submit_args=json.dumps(submit_args),
            )
            return int(res_data)
        except Exception as e:
            logger.error("create schedule info error:{}".format(e))
            return 0


class TensorflowOperator(SparkMLLibDatalabOperator):
    def __init__(self, job_id, jobserver_config):
        super(TensorflowOperator, self).__init__(job_id, 0, 0, jobserver_config)

    def prepare_job(self):
        return {}

    def submit_job(self, params):
        TensorFlowJobDriver.start_job(self.job_id, params)
        return True

    def update_job(self, params):
        return TensorFlowJobDriver.update_job(params)

    def stop_job(self, exec_id):
        TensorFlowJobDriver.stop_job(self.job_id)
        return True

    def delete_job(self, params):
        TensorFlowJobDriver.delete_job(self.job_id, with_data=params["with_data"])


class ModelingSparkMLLibJob(object):
    def __init__(self, job):
        self.job = job
        self.operator = self.get_operator()

    def get_operator(self):
        job_config = json.loads(self.job.job_config)
        job_id = self.job.job_id
        use_type = job_config["use_type"]
        if use_type == "datalab":
            jobserver_config = json.loads(self.job.jobserver_config)
            notebook_id = job_config["notebook_id"]
            cell_id = job_config["cell_id"]
            return SparkMLLibDatalabOperator(job_id, notebook_id, cell_id, jobserver_config)

    def prepare_job(self, params=None):
        return self.operator.prepare_job()

    def submit_job(self, params):
        return self.operator.submit_job(params)

    def stop_job(self, task):
        # job_config = json.loads(self.job.job_config)
        task_args = json.loads(task.context)["args"]
        if "exec_id" not in task_args:
            raise KillTaskError(message_kv={"content": "任务尚未启动"})
        batch_job = ProcessingBatchJobHandler.get_proc_batch_job(self.job.job_id)
        submit_args = json.loads(batch_job.submit_args)
        return self.operator.stop_job(submit_args["exec_id"])

    def update_job(self, params):
        pass

    def delete_job(self, params):
        pass

    def clear_job(self, status, bk_username):
        return self.operator.clear_job(status, bk_username)

    def check_job(self):
        # job_config = json.loads(self.job.job_config)
        batch_job = ProcessingBatchJobHandler.get_proc_batch_job(self.job.job_id)
        submit_args = json.loads(batch_job.submit_args)
        return self.operator.check_job(submit_args["exec_id"])

    def record_exec_id(self, exec_id):
        job_info = ProcessingJobInfoHandler.get_proc_job_info(self.job.job_id)
        job_config = json.loads(job_info.job_config)
        job_config["exec_id"] = exec_id
        new_job_info = {
            "job_id": self.job.job_id,
            "job_config": json.dumps(job_config),
            "bk_username": get_request_username(),
            "jobserver_config": json.loads(self.job.jobserver_config),
        }
        ProcessingJobInfoHandler.update_processing_job_info(new_job_info)


class ModelingTensorflowJob(ModelingSparkMLLibJob):
    def __init__(self, job):
        super(ModelingTensorflowJob, self).__init__(job)
        self.operator = self.get_operator()

    def get_operator(self):
        job_id = self.job.job_id
        jobserver_config = json.loads(self.job.jobserver_config)
        return TensorflowOperator(job_id, jobserver_config)

    def update_job(self, params):
        return self.operator.update_job(params)

    def prepare_job(self, params=None):
        return params

    def submit_job(self, params):
        return self.operator.submit_job(params)

    def stop_job(self, task=None):
        return self.operator.stop_job(None)

    def delete_job(self, params):
        return self.operator.delete_job(params)
