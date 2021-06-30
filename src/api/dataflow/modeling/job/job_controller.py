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
import pickle
import random
import re
import time
import uuid
from datetime import datetime

import grpc
from common.django_utils import DataResponse
from common.local import get_request_username
from kazoo.client import KazooClient
from rest_framework.response import Response

from dataflow.modeling.basic_model.basic_model_serializer import ModelSerializer
from dataflow.modeling.exceptions.comp_exceptions import (
    DEFAULT_MODELING_ERR,
    DEFAULT_SUCCESS_CODE,
    EVALUATE_FAILED_CODE,
    EVALUATE_RUNNING_CODE,
    EVALUATE_SUCCESS_CODE,
    DDLSqlExecuteError,
    DropModelError,
    DropModelNotExistsError,
    DropTableNotExistsError,
    GrpcServiceNotFoundError,
    KillTaskError,
    SQLNotSupportedError,
    SqlParseError,
)
from dataflow.modeling.grpc import direct_access_pb2, direct_access_pb2_grpc
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.job.modeling_job_pipeline import ModelingJobPipeLine
from dataflow.modeling.job.task_handler import ModelingDatalabTaskHandler
from dataflow.modeling.job.tensorflow.tensorflow_job_driver import TensorFlowJobDriver
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.processing.process_controller import ModelingProcessController
from dataflow.modeling.tasks import start_modeling_job
from dataflow.modeling.utils.modeling_params import DatalabParams
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.pizza_settings import MLSQL_ZK_PATH, MLSQL_ZK_SERVER
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper

SUB_TASK_PREFIX = "subtask_"
EVALAUTE_METRIC_NAME_MAP = {"recall": "召回率", "precision": "精确率"}


class ModelingJobController(object):
    task_id = None
    job_id = None
    pipeline = None
    task = None

    def __init__(self, job_id=None, task_id=None):
        if job_id:
            self.job_id = job_id
            self.pipeline = ModelingJobPipeLine(self.job_id)
        elif task_id:
            self.task = MLSqlExecuteLogHandler.get_with_default(task_id)
            if self.task:
                self.params = json.loads(self.task.context)["args"]
                if "job_id" in self.params:
                    self.job_id = self.params["job_id"]
                    self.pipeline = ModelingJobPipeLine(self.job_id)

    def merge_task_log(self):
        # 这里需要将task.logs_zh的信息进行整合，抽取出如下信息：当前步骤，步骤状态，步骤描述，耗时等
        # 注意：每个步骤的执行有两条记录，开始结束各有一条，格式是stage|level|time|msg|detail ,其中detail是可选的
        # 另外，每次查询这里返回的是完整的步骤，即包括尚未执行的步骤，只不过状态为pending
        # 初始化步骤
        response_data = []
        index = 1
        for stage in ModelingDatalabTaskHandler.STAGES:
            stage_info = {
                "stage_type": stage["stage"],
                "stage_status": "pending",
                "description": stage["stage_zh"],
                "stage_seq": index,
                "time_taken": 0,
            }
            response_data.append(stage_info)
            index = index + 1

        if self.task and self.task.logs_zh:
            log_lines = self.task.get_logs()
            log_index = 0
            while log_index < len(log_lines):
                logger.info("start logs:" + json.dumps(log_lines[log_index]))
                start_log = log_lines[log_index]
                stage_index = int(start_log["stage"].split("/")[0]) - 1
                start_time = datetime.strptime(start_log["time"], "%Y-%m-%d %H:%M:%S")
                response_data[stage_index]["stage_status"] = start_log["status"]
                response_data[stage_index]["stage_start"] = start_time
                timespan = (datetime.now() - start_time).total_seconds()

                log_index = log_index + 1
                if log_index < len(log_lines):
                    end_log = log_lines[log_index]
                    # 步骤执行完成（即开始结束各一条）
                    end_time = datetime.strptime(end_log["time"], "%Y-%m-%d %H:%M:%S")
                    response_data[stage_index]["stage_status"] = end_log["status"]
                    timespan = (end_time - start_time).total_seconds()
                response_data[stage_index]["time_taken"] = int(timespan) if timespan > 0 else 1

                log_index = log_index + 1
        return response_data

    # 任务是否已经完成（成功，失败或终止）
    def task_finished(self):
        if self.task.status == "failure" or self.task.status == "success" or self.task.status == "cancelled":
            return True
        return False

    # 直接获取已完成任务的日志
    def get_finished_task_resp(self):
        current_logs = self.merge_task_log()
        response_data = {"stages": current_logs}
        params = json.loads(self.task.context)["args"]
        if self.task.status == "failure" or "exec_id" not in params or "job_id" not in params:
            code, msg, errors = ModelingDatalabTaskHandler.match_code_and_message(self.task.logs_zh)
            if not msg:
                msg = "无法获取失败信息，请联系管理员"
            resp = DataResponse(result=False, code=code)
            resp.message = msg
            response_data["status"] = "failed"
            response_data["release"] = False
            resp.data = response_data
            resp.errors = errors
            return resp
        elif self.task.status == "success":
            resp = DataResponse(result=True, code=DEFAULT_SUCCESS_CODE)
            resp.message = "ok"
            response_data["status"] = "success"
            resp.data = response_data
            params = json.loads(self.task.context)["args"]
            response_data["release"] = (
                params["contains_create_run_sql"] if "contains_create_run_sql" in params else False
            )
            response_data["disable"] = params["disable"] if "disable" in params else False
            if response_data["disable"]:
                response_data["disable_message"] = params["disable_message"] if "disable_message" in params else ""
            if "evaluate" in self.params:
                response_data["evaluate"] = self.params["evaluate"]
            return resp
        elif self.task.status == "cancelled":
            resp = DataResponse(result=False, code=DEFAULT_MODELING_ERR)
            resp.message = "cancelled"
            response_data["status"] = "cancelled"
            response_data["release"] = False
            resp.data = response_data
            return resp

    @staticmethod
    def get_zk_server(zk_server, zk_path):
        """
        从zk上获取模型评估服务（为适应测试环境的不稳定性，增加若干次重试）
        @param zk_server: zk服务地址
        @param zk_path: zk路径
        @return:
        """
        service_list = []
        retry_times = 0
        while not service_list and retry_times < 5:
            try:
                zk = KazooClient(hosts=zk_server)
                zk.start()
                children = zk.get_children(zk_path)
                for child in children:
                    data, stat = zk.get(zk_path + child)
                    data = json.loads(data)
                    if "direct_access" in data and "type" in data["direct_access"]:
                        logger.info(data["direct_access"])
                        if data["direct_access"]["type"] == "grpc":
                            data_access = data["direct_access"]
                            service_list.append("{}:{}".format(data_access["host"], data_access["port"]))
                zk.stop()
                zk.close()
                if not service_list:
                    time.sleep(5)
            finally:
                retry_times = retry_times + 1
        logger.info("service list:" + str(service_list))
        if not service_list:
            raise GrpcServiceNotFoundError(message="ZK上未找到模型评估服务")
        return service_list[random.randint(0, len(service_list) - 1)]

    @staticmethod
    def get_default_evaluate_function(algorithm_type):
        pass

    @staticmethod
    def format_evaluate_object(table_name, evaluate_map):
        """
        生成评估任务需要的配置信息, 这里需要注意有以下几种情况：
        1. 用户输入了function与label,那直接校验，通过后直接提交评估任务
        2. 用户仅输入了function与label，未输入部分采取默认，校验通过后提交评估任务
        3. 用户未输入function与label(说明用户并不关心)，则取默认值，校验通过提交评估任务 ，不通过则不进行评估
        @param table_name:结果表名
        @param evaluate_map:评估需要的信息，如评估函数，评估列等
        @return:
        """
        evaluate_function = evaluate_map["evaluate_function"]
        if "evaluate_label" in evaluate_map:
            evaluate_label = evaluate_map["evaluate_label"]
        else:
            evaluate_label = "label"
        predict_label = evaluate_map["predict_label"]
        table_fields = ResultTableHelper.get_result_table_fields(table_name)
        table_field_map = {item["field_name"]: item for item in table_fields}
        logger.info("fields map:" + json.dumps(table_field_map))
        table_storage = ResultTableHelper.get_result_table_storage(table_name, "hdfs")
        connection_info = json.loads(table_storage["hdfs"]["storage_cluster"]["connection_info"])
        input_path = "{}/{}".format(connection_info["hdfs_url"], table_storage["hdfs"]["physical_table_name"])

        algorithm_version = AlgorithmVersionHandler.get_alorithm_by_name(evaluate_function)
        evaluate_output = json.loads(algorithm_version.config)["evaluate_output"]
        apply_id = "{}_{}".format(table_name, uuid.uuid4().hex[0:8])
        apply_request = {
            "options": {},
            "instruction_name": "run_py_spark_evaluation",
            "dispatcher_name": "default",
        }
        target_dict = {
            "declared_instruction": "run_py_spark_evaluation",
            "worker_name": "*",
            "worker_set_name": "*",
            "worker_group_name": "python_backend",
        }
        apply_request["target"] = target_dict
        apply_request["args"] = []
        apply_request["session_id"] = apply_id
        apply_request["id"] = apply_id
        kwargs = {
            "init_kwargs": {
                "predicted_train_df_direct_hdfs_path": input_path,
                "predicted_validate_df_direct_hdfs_path": "",
            },
        }

        init_config = {}
        predict_label_column = table_field_map[predict_label]
        predicted_colulmn = {
            "default_value": 1,
            "field_type": predict_label_column["field_type"],
            "field_alias": predict_label_column["field_alias"],
            "sample_value": 1,
            "value": None,
            "allowed_values": [],
            "field_name": "prediction",
            "data_field_name": predict_label,
            "field_index": predict_label_column["field_index"],
        }
        init_config["predicted_columns"] = [predicted_colulmn]
        init_config["evaluate_output"] = evaluate_output
        init_config["use_field_mapping"] = True
        algorithm_properties = {
            "algorithm_name": evaluate_function,
            "algorithm_version": 1,
            "algorithm_framework": "pyspark_mllib",
            "logic": algorithm_version.logic,
        }
        real_label_column = table_field_map[evaluate_label]
        label_column = {
            "default_value": 1,
            "field_type": real_label_column["field_type"],
            "field_alias": real_label_column["field_alias"],
            "sample_value": 1,
            "value": None,
            "allowed_values": [],
            "field_name": "label",
            "data_field_name": evaluate_label,
            "field_index": real_label_column["field_index"],
        }
        init_config["label_columns"] = [label_column]
        init_config["timestamp_columns"] = []
        init_config["field_groups"] = ["group_columns"]
        init_config["group_columns"] = []
        init_config["feature_columns"] = []
        init_config["evaluate_args"] = []
        init_config["algorithm_properties"] = algorithm_properties
        kwargs["init_kwargs"]["config"] = init_config
        apply_request["kwargs"] = kwargs
        return apply_request

    @staticmethod
    def format_get_result_object(task_id, worker_name):
        """
        生成获取评估结果时需要的配置
        @param task_id: apply时生成的评估任务id
        @param worker_name: apply时返回的执行具体评估任务的worker名称
        @return:
        """
        evaluate_obj = {
            "options": {},
            "instruction_name": "run_py_spark_evaluation",
            "dispatcher_name": "default",
        }
        target_dict = {
            "declared_instruction": "run_py_spark_evaluation",
            "worker_name": worker_name,
            "worker_set_name": "*",
            "worker_group_name": "python_backend",
        }
        evaluate_obj["target"] = target_dict
        evaluate_obj["args"] = []
        evaluate_obj["kwargs"] = {}
        evaluate_obj["id"] = task_id
        evaluate_obj["session_id"] = task_id
        return evaluate_obj

    @staticmethod
    def apply(table_name, evaluate_map):
        """
        提交评做任务，注意：此过程是一定能成功返回的，具体失败原因需要通过get_result来返回
        @param table_name: 结果表的名称
        @param evaluate_map: 评估需要的相关信息（评估函数，标签列等）
        @return:
        """
        apply_request = ModelingJobController.format_evaluate_object(table_name, evaluate_map)
        logger.info("apply request:" + json.dumps(apply_request))
        apply_worker_name = None
        service = ModelingJobController.get_zk_server(MLSQL_ZK_SERVER, MLSQL_ZK_PATH)
        try:
            service_channel = grpc.insecure_channel(service)
            sevice_stub = direct_access_pb2_grpc.DirectAccessStub(service_channel)
            request_body = ModelingUtils.convert_to_unicode(apply_request)
            response = sevice_stub.apply(
                direct_access_pb2.Request(
                    task=pickle.dumps(request_body, protocol=2),
                    pickle_protocol=2,
                    block=True,
                )
            )
            content = pickle.loads(response.content)
            logger.info(ModelingUtils.convert_to_string(content))
            apply_worker_name = content["target"]["worker_name"]
        except Exception as e:
            logger.exception(e)
        return apply_request["id"], apply_worker_name

    @staticmethod
    def get_result(task_id, worker_name):
        """
        获取评估过程执行的结果，目前是根据返回值的结构来判断的，当返回是dict且train信息存在时，表示成功
        当获取过程直接出异常时则返回评估失败，其它情况则返回评估运行中
        @param task_id: apply时生成的评估任务id
        @param worker_name: apply时返回的执行具体评估任务的worker名称
        @return:
        """
        try:
            if not worker_name:
                # 提交时出现异常
                response_result = {
                    "status": "failed",
                    "message": "submit evaluate task failed",
                }
            else:
                get_result_object = ModelingJobController.format_get_result_object(task_id, worker_name)
                logger.info("get result object:" + json.dumps(get_result_object))
                service = ModelingJobController.get_zk_server(MLSQL_ZK_SERVER, MLSQL_ZK_PATH)
                service_channel = grpc.insecure_channel(service)
                # service_channel = ''
                sevice_stub = direct_access_pb2_grpc.DirectAccessStub(service_channel)
                request_body = ModelingUtils.convert_to_unicode(get_result_object)
                response = sevice_stub.get_result(
                    direct_access_pb2.Request(
                        task=pickle.dumps(request_body, protocol=2),
                        block=True,
                        pickle_protocol=2,
                        timeout=20,
                    )
                )
                response_content = ModelingUtils.convert_to_string(pickle.loads(response.content))
                if isinstance(response_content, dict) and "train" in response_content:
                    train_result = response_content["train"]
                    train_result_with_name = {}
                    for metric_name in train_result:
                        train_result_with_name[metric_name] = {
                            "name": EVALAUTE_METRIC_NAME_MAP[metric_name]
                            if metric_name in EVALAUTE_METRIC_NAME_MAP
                            else metric_name,
                            "value": train_result[metric_name],
                        }
                    response_result = {
                        "status": "finished",
                        "result": {"train": train_result_with_name},
                    }
                else:
                    response_result = {"status": "running"}
        except Exception as e:
            logger.exception(e)
            response_result = {"status": "failed", "message": "{}".format(e)}
        return response_result

    def check_job_running(self, last_log, stage_logs):
        """
        用于检查正在集群上正在执行的任务的执行情况，所以只有在任务提交成功后才会进入此函数
        即任务执行第3步（index为2）与第4步（index为3）时才会进入此函数，这个函数的过程比较复杂：
          1. 当前任务在jobnavi正在运行，或是已失败或被终止，则直接返回
          2. 如果当前任务在jobnavi上已经运行成功了，那么就有如下处理：
             2.1: 记录任务执行这一步的运行结果日志
             2.2：检查是否需要评估，如果否，直接返回，如果需要评估，则提交评估任务
             2.3：如果任务已经在评估阶段，则检查评估任务是否需要继续执行
        @param last_log 当前运行的最后一条日志的详细信息，包括当前这个步骤的索引等
        @param stage_logs 返回给前端的每个步骤的详细信息,，包括时间，耗时等信息
        @return:
        """
        logger.info("last log:" + json.dumps(last_log))
        current_stage = last_log["stage"]
        current_index = int(current_stage.split("/")[0])
        if current_index == 3:
            next_index = current_index + 1
            next_stage = "{}/{}".format(next_index, len(ModelingDatalabTaskHandler.STAGES))
            # 任务在执行，需要调用接口检查集群上的任务状态
            check_result = self.pipeline.check_job()
            logger.info("job check result:" + json.dumps(check_result))
            if check_result["data"] != "finished":
                # 任务执行中或已经失败，直接返回
                check_result["stage"] = "{}/{}".format(3, len(ModelingDatalabTaskHandler.STAGES))
                return check_result
            else:
                # 成功了，记录上一步骤(job执行)的相关信息
                logger.info("record job running finished log.")
                # 数据库里的日志
                self.task.add_log(
                    ModelingDatalabTaskHandler.STAGES[2]["description"],
                    status=MLSqlExecuteLog.STATUS.SUCCESS,
                    stage=current_stage,
                    level="INFO",
                    detail=check_result["message"],
                )
                # 返回给前端的日志信息
                stage_logs[current_index - 1]["stage_status"] = "success"
                stage_logs[current_index - 1]["time_taken"] = int(
                    (datetime.now() - stage_logs[current_index - 1]["stage_start"]).total_seconds()
                )
                # 添加下一步的日志
                stage_logs[next_index - 1]["stage_status"] = "running"
                logger.info("record job evalaute starting log.")
                self.task.add_log(
                    ModelingDatalabTaskHandler.STAGES[3]["description"],
                    status=MLSqlExecuteLog.STATUS.RUNNING,
                    stage=next_stage,
                    level="INFO",
                    detail=ModelingDatalabTaskHandler.STAGES[3]["description"],
                )
                need_evaluate = self.params["need_evaluate"] if "need_evaluate" in self.params else False
                if need_evaluate:
                    logger.info("start evaluating job.")
                    # 需要评估的时候启动评估，否则的话这一步为虚拟的，仅记录步骤，但不执行任何操作
                    evaluate_map = self.params["evaluate_map"]
                    last_table = self.params["last_table"]
                    evaluate_task_id, worker_name = ModelingJobController.apply(last_table, evaluate_map)
                    self.params["evaluate_task_id"] = evaluate_task_id
                    self.params["evaluate_worker_name"] = worker_name
                    self.params["evaluate_task_status"] = "running"
                    task_context = json.loads(self.task.context)
                    task_context["args"] = self.params
                    # 记录评估相关信息
                    MLSqlExecuteLogHandler.update_execute_log(self.task.id, task_context)
                return {
                    "data": "running",
                    "code": EVALUATE_RUNNING_CODE,
                    "message": "Evaluating running",
                    "stage": next_stage,
                }
        else:
            # 任务在评估阶段
            need_evaluate = self.params["need_evaluate"] if "need_evaluate" in self.params else False
            if not need_evaluate:
                # todo:目前先返回成功，后续在不需要评估下，可以返回生成模型或表的相关信息
                logger.info("do not need evalaute.")
                return {
                    "data": "finished",
                    "code": EVALUATE_SUCCESS_CODE,
                    "message": "Evaluate succesfully",
                    "stage": current_stage,
                }
            else:
                # 评估过程正在进行
                evaluate_task_id = self.params["evaluate_task_id"]
                evaluate_worker_name = self.params["evaluate_worker_name"]
                response = ModelingJobController.get_result(evaluate_task_id, evaluate_worker_name)
                logger.info("get evalaute status:" + json.dumps(response))
                evaluate_task_status = response["status"]
                evaluate_result = {}
                if evaluate_task_status != "running":
                    # 评估已经完成
                    if evaluate_task_status == "finished":
                        data = "finished"
                        code = EVALUATE_SUCCESS_CODE
                        message = "Evaluate succesfully"
                        final_status = MLSqlExecuteLog.STATUS.SUCCESS
                        evaluate_result = response["result"]
                        self.params["evaluate"] = evaluate_result
                    else:
                        # 评估失败
                        data = "failed"
                        code = EVALUATE_FAILED_CODE
                        message = "Evaluate failed:" + response["message"]
                        final_status = MLSqlExecuteLog.STATUS.FAILURE
                    self.task.add_log(
                        ModelingDatalabTaskHandler.STAGES[3]["description"],
                        status=final_status,
                        stage=current_stage,
                        level="INFO",
                        detail=ModelingDatalabTaskHandler.STAGES[3]["description"],
                    )
                    task_context = json.loads(self.task.context)
                    task_context["args"] = self.params
                    MLSqlExecuteLogHandler.update_execute_log(self.task.id, task_context)
                    result_data = {
                        "data": data,
                        "code": code,
                        "message": message,
                        "stage": current_stage,
                    }
                    if evaluate_result:
                        result_data["evaluate"] = evaluate_result
                    return result_data
                else:
                    # 评估尚未完成
                    logger.info("evaluating is running...")
                    return {
                        "data": "running",
                        "code": EVALUATE_RUNNING_CODE,
                        "message": "Evaluating running",
                        "stage": current_stage,
                    }

    def check_job(self):
        """
        返回任务的执行情况，注意response.data中的submit用于标识是否可以发布，条件为：
            1. 是否包含run语句，这个在提交的时候已经设置到args中的contains_create_run_sql变量内,有run语句时值为True
            2. 任务执行是否成功：只要不成功，此值均为False
        @return:{json}
        {
            'code': 'xxxx',
            'result': true,
            'message': 'ok',
            'data': {
                'status': 'running',
                'stages': [
                    {
                        'stage': 'xxxx',
                        'description': 'xxx'
                    }
                ]
            }
        }
        """

        try:
            # 当前状态
            current_logs = self.merge_task_log()
            response_data = {"stages": current_logs}
            if not self.task:
                # 未生成任务，默认正在执行第一个步骤
                final_resp = DataResponse(message="running")
                current_logs[0]["stage_status"] = "running"
                final_resp.result = True
                response_data["status"] = "running"
                final_resp.code = DEFAULT_SUCCESS_CODE
                final_resp.data = response_data
                final_resp.message = "ok"
                return final_resp

            params = json.loads(self.task.context)["args"]
            if self.task_finished():
                # 任务已经完成了，不需要到平台上去check了
                return self.get_finished_task_resp()

            if "exec_id" not in params or "job_id" not in params:
                # 任务正在执行，但尚未向jobnavi成功提交任务
                check_result = {
                    "data": "running",
                    "message": "ok",
                    "code": DEFAULT_SUCCESS_CODE,
                }
            else:
                # 还在执行，Job已经生成， 需要去check
                last_log = (self.task.get_logs())[-1]
                check_result = self.check_job_running(last_log, current_logs)
            logger.info(current_logs)
            # 设置返回值
            logger.info("check result:" + json.dumps(check_result))
            final_resp = DataResponse(code=check_result["code"])
            final_resp.message = check_result["message"]
            if check_result["data"] in ["failed", "finished", "cancelled"]:
                # 公共信息
                stage = check_result["stage"]
                stage_index = int(stage.split("/")[0]) - 1
                response_data["status"] = check_result["data"]
                current_logs[stage_index]["time_taken"] = int(
                    (datetime.now() - current_logs[stage_index]["stage_start"]).total_seconds()
                )
                if check_result["data"] == "failed":
                    status = MLSqlExecuteLog.STATUS.FAILURE
                    final_resp.result = False
                    response_data["release"] = False
                elif check_result["data"] == "finished":
                    status = MLSqlExecuteLog.STATUS.SUCCESS
                    response_data["release"] = (
                        params["contains_create_run_sql"] if "contains_create_run_sql" in params else False
                    )
                    response_data["disable"] = params["disable"] if "disable" in params else False
                    if response_data["disable"]:
                        response_data["disable_message"] = (
                            params["disable_message"] if "disable_message" in params else ""
                        )
                    if "evaluate" in check_result:
                        response_data["evaluate"] = check_result["evaluate"]
                    final_resp.result = True
                else:
                    final_resp.result = True
                    response_data["release"] = False
                    status = MLSqlExecuteLog.STATUS.CANCELLED
                self.task.set_status(status)
                self.task.add_log(
                    ModelingDatalabTaskHandler.STAGES[stage_index]["description"],
                    status=status,
                    stage=check_result["stage"],
                    level="INFO",
                    detail=check_result["message"],
                )
                current_logs[stage_index]["stage_status"] = status
            else:
                # running or preparing or pending in jobnavi, return running and do nothing
                final_resp.result = True
                response_data["status"] = "running"
                response_data["release"] = False
            final_resp.data = response_data
            return final_resp
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @classmethod
    def create_job(cls, params):
        TensorFlowJobDriver.create_job(params)

    def clear_job(self, status=None):
        try:
            if not self.pipeline:
                return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, message="ok", data=None)
            if not status:
                status = self.task.status
            return self.pipeline.clear_job(status, self.task.created_by)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e), data="error")

    @classmethod
    def parse(cls, sql_list):
        return FlowModelingOperator.parse_job(sql_list)

    @classmethod
    def get_task_id(cls, notebook_id):
        return_obj = {}
        result = MLSqlExecuteLogHandler.get_task_id_by_notebook(notebook_id)
        if result:
            return_obj["task_id"] = result.id
            return_obj["status"] = result.status
        return return_obj

    def stop_job(self):
        if not self.pipeline:
            raise KillTaskError(message_kv={"content": "任务尚未启动"})
        return self.pipeline.stop_job()

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
            return Response(ddl_result)
        else:
            task_id = operator.start_job(normal_list)
            result = {"type": "normal", "task_id": task_id, "result": ddl_result}
            return Response(result)

    def update_job(self, params):
        return self.pipeline.update_job(params)

    def delete_job(self, params):
        return self.pipeline.delete_job(params)

    def submit_job(self, params=None):
        return self.pipeline.start_mlsql_job(params)

    def record_exec_id(self, exec_id):
        self.pipeline.record_exec_id(exec_id)

    def start_multi_jobs(self, is_restart):
        return self.pipeline.start_multi_jobs(is_restart)

    def multi_stop_jobs(self, type="batch"):
        if not self.pipeline:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="任务未启动")
        self.pipeline.multi_stop_jobs(type)

    @classmethod
    def create_multi_jobs(cls, jobserver_config, processing_id, cluster_group, cluster_name):
        return ModelingJobPipeLine.create_multi_jobs(jobserver_config, processing_id, cluster_group, cluster_name)

    def update_multi_jobs(self, jobserver_config):
        return self.pipeline.update_multi_jobs(jobserver_config)

    def multi_delete_jobs(self):
        self.pipeline.multi_delete_jobs()

    def get_mlsql_result(self):
        if not self.task:
            return {}
        else:
            return ModelingJobPipeLine.get_mlsql_result(self.task)


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
        default_res = {"read": {}, "write": {}, "delete": {}}
        try:
            normal_list, ddl_list = cls.split_sql_list(sql_list)
            if ddl_list:
                default_res.update(ModelingProcessController.parse_ddl_tables(ddl_list))
            if normal_list:
                normal_result = ModelingProcessController.parse_mlsql_tables(normal_list)
                for key in normal_result:
                    if key not in default_res:
                        default_res[key] = normal_result[key]
                    else:
                        for sub_key in normal_result[key]:
                            if sub_key not in default_res[key]:
                                default_res[key][sub_key] = normal_result[key][sub_key]
                            else:
                                default_res[key][sub_key].extend(normal_result[key][sub_key])
            return Response(default_res)
        except Exception as e:
            return DataResponse(
                result=False,
                code=DEFAULT_MODELING_ERR,
                message="{}".format(e),
                data="{}".format(e),
            )

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

    def execute_single_sql(self, ind, ddl_type, ddl_data, other_info=None):
        """
        执行DDL语句
        @param ind:语句的索引，主要用于错误信息提示
        @param ddl_type:操作类型，show_model,drop_model等
        @param ddl_data:语句中的实体信息，比如show models like中的匹配表达式，drop语句中的表名或模型名
        @param other_info:其它限制信息，比如drop中的if exists，主要用于控制执行中的逻辑行为，比如实体
                           不存在的时候是否报错等
        @return: 执行结果
        """
        if ddl_type == "show_models":
            return ModelingHelper.show_models(self.notebook_id, ddl_data=ddl_data)
        elif ddl_type == "show_tables":
            return ModelingHelper.show_tables(self.notebook_id, ddl_data=ddl_data)
        elif ddl_type == "drop_model":
            return self.delete_model(ddl_data, if_exists=other_info)
        elif ddl_type == "drop_table":
            return ModelingHelper.drop_table(ddl_data, self.notebook_id, self.cell_id, if_exists=other_info)
        elif ddl_type == "truncate":
            return ModelingHelper.truncate_table(ddl_data, self.notebook_id, self.cell_id)
        elif ddl_type == "show_train_model" or ddl_type == "show_create_table":
            if ddl_type == "show_train_model":
                detail = ModelingHelper.show_sql(ddl_data, self.notebook_id, "model")
            else:
                detail = ModelingHelper.show_sql(ddl_data, self.notebook_id, "result_table")
            if detail:
                return detail["sql"]
            else:
                raise SqlParseError(
                    message_kv={
                        "number": ind,
                        "content": "No sql found for:" + ddl_data,
                    }
                )
        else:
            raise SqlParseError(message_kv={"number": ind, "content": "Not supported operation"})

    def show_models(self, reg=None):
        params = {
            "active": 1,
            "created_by": get_request_username(),
        }
        result_models = MLSqlModelInfoHandler.filter(**params)
        model_list = ModelSerializer(result_models, many=True)
        if not reg:
            return model_list
        else:
            result_list = []
            match_regex = "^" + reg.replace("%", "(.)*").replace("_", ".") + "$"
            for model in model_list:
                model_name = model["model_name"]
                if re.search(match_regex, model_name):
                    result_list.append(model)
            return result_list

    def delete_model(self, model_name, if_exists=False):
        return MLSqlModelInfoHandler.delete_model_by_project(
            model_name,
            self.project_id,
            self.notebook_id,
            self.cell_id,
            if_exists=if_exists,
        )

    def execute_ddl_sql(self, sql_list):
        last_result = None
        sql_index = 0
        drop_result = {"model": [], "result_table": []}
        # 记录最后个语句操作的类型，默认为show，如果是drop语句，则为ddl
        last_op_type = "show"
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
            process_result = ModelingProcessController.create_bulk_processing(ddl_args)
            content = process_result["content"]
            ddl_type = content["type"]
            ddl_data = content["data"] if "data" in content else None
            if_exists = content["ifExists"] if "ifExists" in content else None
            if ddl_type == "drop_model" or ddl_type == "drop_table":
                last_op_type = "ddl"
            try:
                last_result = self.execute_single_sql(sql_index, ddl_type, ddl_data, other_info=if_exists)
                if ddl_type == "drop_model":
                    drop_result["model"].append(ddl_data)
                elif ddl_type == "drop_table":
                    drop_result["result_table"].append(ddl_data)
                else:
                    last_op_type = ddl_type
            except (DropModelNotExistsError, DropTableNotExistsError) as e:
                raise e
            except (DDLSqlExecuteError, DropModelError) as e:
                new_message = "第" + str(sql_index) + "个sql执行异常:" + e.message
                raise DDLSqlExecuteError(message_kv={"content": new_message})
            except Exception as e:
                logger.exception(e)
                raise e

        # type -  操作类型
        # value - 最后一个sql执行结果
        # result - ddl操作的结果，目前仅对drop类操作有效
        result = {"type": last_op_type, "value": last_result, "result": drop_result}
        return result
