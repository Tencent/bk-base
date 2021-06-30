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

import copy
import datetime
import json

import pytest
from rest_framework.test import APITestCase

import dataflow
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchJob
from dataflow.batch.periodic.param_info.periodic_job_info_params import PeriodicJobInfoParams
from dataflow.modeling.exceptions.comp_exceptions import (
    EVALUATE_FAILED_CODE,
    EVALUATE_RUNNING_CODE,
    EVALUATE_SUCCESS_CODE,
)
from dataflow.modeling.job.job_controller import ModelingJobController
from dataflow.modeling.job.tensorflow.tensorflow_job_driver import TensorFlowJobDriver
from dataflow.modeling.models import AlgorithmVersion, MLSqlExecuteLog
from dataflow.models import ProcessingJobInfo
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper


def model_evaluate_apply_mock(evalaute_task_id, evaluate_worker_name):
    def mock_func(*args, **kwargs):
        return evalaute_task_id, evaluate_worker_name

    return mock_func


def model_evaluate_get_result_mock(status, recall, f1score, precision):
    def mock_func(*args, **kwargs):
        data = {"train": {"recall": recall, "f1score": f1score, "precision": precision}}
        if status == "running":
            return {"status": status}
        if status == "failed":
            return {"status": status, "message": "failed"}
        return {"status": status, "result": data}

    return mock_func


def pipeline_check_job_mock(status, message):
    def mock_func(*args, **kwargs):
        data = {"data": status, "message": message}
        return data

    return mock_func


def get_zk_service_mock(server, path):
    def mock_func(*args, **kwargs):
        return "{}:{}".format(server, path)

    return mock_func


@pytest.fixture(scope="function")
def mock_evaluation_service():
    dataflow.modeling.job.job_controller.ModelingJobController.apply = staticmethod(
        model_evaluate_apply_mock("591_rfc_result_307_15dc05c7", "914612748_7")
    )
    dataflow.modeling.job.modeling_job_pipeline.ModelingJobPipeLine.check_job = pipeline_check_job_mock(
        "finished", "successfully"
    )
    dataflow.modeling.job.job_controller.ModelingJobController.get_zk_server = staticmethod(
        get_zk_service_mock("localhost", "13105")
    )


@pytest.fixture(scope="function")
def mock_evaluation_get_result_success():
    dataflow.modeling.job.job_controller.ModelingJobController.get_result = staticmethod(
        model_evaluate_get_result_mock("finished", 1.0, 0.9, 0.8)
    )


@pytest.fixture(scope="function")
def mock_evaluation_get_result_running():
    dataflow.modeling.job.job_controller.ModelingJobController.get_result = staticmethod(
        model_evaluate_get_result_mock("running", 1.0, 0.9, 0.8)
    )


@pytest.fixture(scope="function")
def mock_evaluation_get_result_failed():
    dataflow.modeling.job.job_controller.ModelingJobController.get_result = staticmethod(
        model_evaluate_get_result_mock("failed", 1.0, 0.9, 0.8)
    )


@pytest.fixture(scope="function")
def mock_param_create_job_info():
    job_config = {"resource_group_id": "default", "processings": ["591_tensorflow_example_1"]}

    def mock_func(param_obj, *args, **kwargs):
        assert param_obj.processing_id == "591_tensorflow_example_1"
        assert param_obj.job_config.to_db_json() == job_config

    PeriodicJobInfoParams.save_to_processing_job_info_db = mock_func


@pytest.fixture(scope="function")
def mock_param_update_job_info():
    job_config = {"resource_group_id": "default", "processings": ["591_tensorflow_example_2"]}

    def mock_func(param_obj, *args, **kwargs):
        assert param_obj.processing_id == "591_tensorflow_example_2"
        assert param_obj.job_config.to_db_json() == job_config

    PeriodicJobInfoParams.update_to_processing_job_info_db = mock_func


@pytest.fixture(scope="function")
def mock_jobnavi_get_info():
    def mock_func(jobnavi, *args, **kwargs):
        return None

    JobNaviHelper.get_schedule_info = mock_func


@pytest.fixture(scope="function")
def mock_jobnavi_create_info():
    def mock_func(jobnavi, args):
        assert args["schedule_id"] == "591_tensorflow_example_1"

    JobNaviHelper.create_schedule_info = mock_func


@pytest.fixture(scope="function")
def mock_jobnavi_start_schedule():
    def mock_func(jobnavi, schedule_id):
        pass

    JobNaviHelper.start_schedule = mock_func


@pytest.fixture(scope="function")
def mock_batch_job_exist():
    def mock_func(batch_id):
        return False

    ProcessingBatchJobHandler.is_proc_batch_job_exist = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_save_batch_job():
    def mock_func(*args, **kwargs):
        return None

    ProcessingBatchJobHandler.save_proc_batch_job_v2 = staticmethod(mock_func)


class TestModelingJobController(APITestCase):
    databases = "__all__"

    service_list = []
    task_id = None
    last_log = None
    stage_log = None

    def prepare_task(
        self,
        need_evalaute=True,
        evaluate_map={},
        evaluate_task_id=None,
        evaluate_worker_name=None,
        evaluate_status=None,
    ):
        basic_context = {
            "progress": 0.0,
            "args": {
                "bk_biz_id": 0,
                "geog_area_code": "inland",
                "need_evaluate": need_evalaute,
                "evaluate_map": {
                    "evaluate_label": "label",
                    "predict_label": "prediction",
                    "evaluate_function": "pyspark_classify_evaluation",
                },
                "cell_id": "a7709153c80846818abb3398102fb38d",
                "job_id": "13105_bbf8292604e34f5089c35894239b6e40",
                "notebook_id": 888,
                "tags": '["inland"]',
                "bk_username": "xxxx",
                "exec_id": 8337775,
                "disable": False,
                "cluster_id": "default",
                "contains_create_run_sql": True,
                "last_table": "591_rfc_result_310_xxxx_test",
                "project_id": "13105",
                "type": "datalab",
                "sql_list": [
                    "train model 591_string_indexer_310_xxxx_test options(algorithm='string_indexer',input_col=char_1,"
                    "output_col='indexed')  from 591_new_modeling_query_set",
                    "create table 591_string_indexer_result_310_xxxx_test as run 591_string_indexer_310_xxxx_test("
                    "input_col=char_1,handle_invalid='keep') as indexed, double_1,double_2  "
                    "from 591_new_modeling_query_set",
                    "train model 591_rfc_model_310_xxxx_test options("
                    "algorithm='random_forest_classifier',features_col=<double_1,double_2>,label_col=indexed) "
                    " from 591_string_indexer_result_310_xxxx_test",
                    "create table 591_rfc_result_310_xxxx_test as run 591_rfc_model_310_xxxx_test("
                    "features_col=<double_1,double_2>, evaluate_function='pyspark_classify_evaluation') "
                    "as prediction,double_1 as label,double_2,indexed  from 591_string_indexer_result_310_xxxx_test",
                ],
            },
            "consuming_mode": "continue",
            "cluster_group": None,
        }
        if evaluate_map:
            basic_context["args"]["evaluate_map"] = evaluate_map
        if evaluate_task_id:
            basic_context["args"]["evaluate_task_id"] = evaluate_task_id
        if evaluate_worker_name:
            basic_context["args"]["evaluate_worker_name"] = evaluate_worker_name
        if evaluate_status:
            basic_context["args"]["evaluate_task_status"] = evaluate_status
        # 写入一个task
        task = MLSqlExecuteLog.objects.create(
            session_id="mock_session",
            notebook_id=888,
            cell_id="a7709153c80846818abb3398102fb38d",
            action="start",
            status="running",
            logs_zh="",
            logs_en="",
            created_by="xxxxx",
            context=json.dumps(basic_context),
        )
        self.task_id = task.id

    def setUp(self):
        self.stage_log = [
            {
                "stage_type": "parse",
                "stage_status": "success",
                "description": "SQL解析",
                "stage_seq": 1,
                "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 6),
                "time_taken": 0,
            },
            {
                "stage_type": "submit",
                "stage_status": "success",
                "description": "提交任务",
                "stage_seq": 2,
                "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 16),
                "time_taken": 0,
            },
            {
                "stage_type": "execute",
                "stage_status": "running",
                "description": "任务执行",
                "stage_seq": 3,
                "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
                "time_taken": 0,
            },
            {
                "stage_type": "evaluate",
                "stage_status": "pending",
                "description": "获取结果",
                "stage_seq": 4,
                "time_taken": 0,
            },
        ]
        job_id = "13105_bbf8292604e34f5089c35894239b6e40"
        if not ProcessingJobInfo.objects.filter(job_id=job_id).exists():
            ProcessingJobInfo.objects.create(
                job_id=job_id,
                processing_type="batch",
                code_version="0.1.0",
                component_type="spark_mllib",
                jobserver_config=json.dumps({"geog_area_code": "inland", "cluster_id": "default"}),
                cluster_group="default",
                cluster_name="root.dataflow.batch.default",
                job_config=json.dumps(
                    {
                        "cell_id": "a7709153c80846818abb3398102fb38d",
                        "heads": ["591_new_modeling_query_set"],
                        "component_type": "spark_mllib",
                        "notebook_id": 888,
                        "schedule_period": "once",
                        "delay": 0,
                        "submit_args": "{}",
                        "tails": ["591_rfc_result_310_xxxx_test"],
                        "processor_logic": '{"sql": "train model 591_string_indexer_310_xxxx_test options('
                        "algorithm='string_indexer',input_col=char_1,output_col='indexed')  "
                        'from 591_new_modeling_query_set", "operate_type": "train", "task_config":'
                        ' {"evaluate_map": {}, "write_mode": "overwrite", '
                        '"name": "591_string_indexer_310_xxxx_test_train", "fields": [], '
                        '"processor": {"args": {"input_col": "char_1", "output_col": "indexed"}, '
                        '"type": "train", "name": "string_indexer"}, "table_need_create": true,'
                        ' "parents": ["591_new_modeling_query_set"], "task_type": "mlsql_query", '
                        '"interpreter": {}, "model_name": "591_string_indexer_310_xxxx_test", '
                        '"type": "model", "id": "591_string_indexer_310_xxxx_test_train", '
                        '"description": "591_string_indexer_310_xxxx_test_train"}, '
                        '"mode": "overwrite", "logic": {"interpreter": {}, '
                        '"task_type": "mlsql_query", "type": "model", '
                        '"processor": {"args": {"input_col": "char_1", "output_col": "indexed"}, '
                        '"type": "train", "name": "string_indexer"}}, '
                        '"model": {'
                        '"path": "hdfs://xxxx/app/mlsql/models/'
                        '591_string_indexer_310_xxxx_test", '
                        '"config_id": 24, "type": "file", '
                        '"name": "591_string_indexer_310_xxxx_test"}}',
                        "count_freq": 0,
                        "project_id": "13105",
                        "use_type": "datalab",
                        "processor_type": "sql",
                    }
                ),
                deploy_mode="yarn",
                deploy_config="{}",
                locked=0,
                created_by="xxxx",
                updated_by="xxxx",
            )
        if not ProcessingBatchJob.objects.filter(batch_id=job_id).exists():
            ProcessingBatchJob.objects.create(
                batch_id=job_id,
                processor_type="sql",
                processor_logic=json.dumps(
                    {
                        "sql": "train model 591_string_indexer_310_xxxx_test options(algorithm='string_indexer',"
                        "input_col=char_1,output_col='indexed')  from 591_new_modeling_query_set",
                        "operate_type": "train",
                        "task_config": {
                            "evaluate_map": {},
                            "write_mode": "overwrite",
                            "name": "591_string_indexer_310_xxxx_test_train",
                            "fields": [],
                            "processor": {
                                "args": {
                                    "input_col": "char_1",
                                    "output_col": "indexed",
                                },
                                "type": "train",
                                "name": "string_indexer",
                            },
                            "table_need_create": True,
                            "parents": ["591_new_modeling_query_set"],
                            "task_type": "mlsql_query",
                            "interpreter": {},
                            "model_name": "591_string_indexer_310_xxxx_test",
                            "type": "model",
                            "id": "591_string_indexer_310_xxxx_test_train",
                            "description": "591_string_indexer_310_xxxx_test_train",
                        },
                        "mode": "overwrite",
                        "logic": {
                            "interpreter": {},
                            "task_type": "mlsql_query",
                            "type": "model",
                            "processor": {
                                "args": {
                                    "input_col": "char_1",
                                    "output_col": "indexed",
                                },
                                "type": "train",
                                "name": "string_indexer",
                            },
                        },
                        "model": {
                            "path": "hdfs://xxxx/path/591_string_indexer_310_xxxx_test",
                            "config_id": 24,
                            "type": "file",
                            "name": "591_string_indexer_310_xxxx_test",
                        },
                    }
                ),
                schedule_time=1612148827201,
                schedule_period="once",
                count_freq=0,
                delay=0,
                submit_args=json.dumps({"exec_id": 8337775}),
                storage_args="{}",
                running_version="0.1.0",
                jobserver_config=json.dumps({"geog_area_code": "inland", "cluster_id": "default"}),
                cluster_group="default",
                cluster_name="root.dataflow.batch.default",
                deploy_mode="yarn",
                deploy_config="{}",
                active=1,
            )
        if not AlgorithmVersion.objects.filter(algorithm_name="pyspark_classify_evaluation").exists():
            AlgorithmVersion.objects.create(
                algorithm_name="pyspark_classify_evaluation",
                version=1,
                logic="demo",
                config=json.dumps(
                    {
                        "predicted_columns": [
                            {
                                "default_value": 1,
                                "field_type": "double",
                                "field_alias": "预测结果",
                                "sample_value": 1,
                                "value": None,
                                "allowed_values": [],
                                "field_name": "prediction",
                                "field_index": 0,
                            }
                        ],
                        "evaluate_output": [
                            {
                                "default_value": 0.5,
                                "field_type": "double",
                                "field_alias": "f1score",
                                "sample_value": 0.5,
                                "value": None,
                                "allowed_values": [],
                                "field_name": "f1score",
                                "field_index": 0,
                            },
                            {
                                "default_value": 0.5,
                                "field_type": "double",
                                "field_alias": "精确率",
                                "sample_value": 0.5,
                                "value": None,
                                "allowed_values": [],
                                "field_name": "precision",
                                "field_index": 1,
                            },
                            {
                                "default_value": 0.5,
                                "field_type": "double",
                                "field_alias": "召回率",
                                "sample_value": 0.5,
                                "value": None,
                                "allowed_values": [],
                                "field_name": "recall",
                                "field_index": 2,
                            },
                        ],
                        "evaluate_args": [],
                        "label_columns": [
                            {
                                "default_value": None,
                                "field_type": "double",
                                "field_alias": "标注",
                                "sample_value": None,
                                "value": None,
                                "allowed_values": None,
                                "field_name": "label",
                                "properties": {
                                    "support": True,
                                    "allow_null": True,
                                    "allow_modified": True,
                                    "constraint_type": "none",
                                    "is_advanced": False,
                                    "used_by": "user",
                                    "constraints": {},
                                },
                                "field_index": 0,
                            }
                        ],
                    }
                ),
                execute_config="{}",
                properties="{}",
                created_by="xxxx",
                updated_by="xxxx",
            )

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_success")
    def test_job_running_evaluate_start(self):
        """
        任务执行完成，不需要进行评估时，提交虚拟的评估任务
        """
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "3/4",
        }
        stage_log = copy.deepcopy(self.stage_log)
        self.prepare_task(False)
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "running",
            "code": EVALUATE_RUNNING_CODE,
            "message": "Evaluating running",
            "stage": "4/4",
        }
        result = controller.check_job_running(last_log, stage_log)

        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]
        assert stage_log[2]["stage_status"] == "success"
        assert stage_log[3]["stage_status"] == "running"

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_success")
    def test_job_running_evaluate_end(self):
        """
        任务执行完成，不需要进行评估时，虚拟的评估任务完成
        """
        stage_log = copy.deepcopy(self.stage_log)
        stage_log[2] = {
            "stage_type": "execute",
            "stage_status": "success",
            "description": "任务执行",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        stage_log[3] = {
            "stage_type": "evaluate",
            "stage_status": "running",
            "description": "获取结果",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "4/4",
        }
        self.prepare_task(False)
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "finished",
            "code": EVALUATE_SUCCESS_CODE,
            "message": "Evaluate succesfully",
            "stage": "4/4",
        }
        result = controller.check_job_running(last_log, stage_log)
        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_success")
    def test_running_evaluate_apply_start(self):
        """
        任务执行完成，提交评估任务，返回评估id, worker_name以及评估状态变更为running
        """
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "3/4",
        }
        stage_log = copy.deepcopy(self.stage_log)
        self.prepare_task(
            True,
            evaluate_map={
                "evaluate_label": "label",
                "predict_label": "prediction",
                "evaluate_function": "pyspark_classify_evaluation",
            },
        )
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "running",
            "code": EVALUATE_RUNNING_CODE,
            "message": "Evaluating running",
            "stage": "4/4",
        }
        result = controller.check_job_running(last_log, stage_log)

        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]
        assert stage_log[2]["stage_status"] == "success"
        assert stage_log[3]["stage_status"] == "running"
        task = MLSqlExecuteLog.objects.get(id=self.task_id)
        new_context = json.loads(task.context)
        new_args = new_context["args"]
        assert new_args["evaluate_task_status"] == "running"
        assert new_args["evaluate_task_id"] == "591_rfc_result_307_15dc05c7"
        assert new_args["evaluate_worker_name"] == "914612748_7"

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_success")
    def test_running_evaluate_apply_success(self):
        """
        任务执行完成，提交评估任务后，获取评估结果
        """
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "4/4",
        }
        stage_log = copy.deepcopy(self.stage_log)
        stage_log[2] = {
            "stage_type": "execute",
            "stage_status": "success",
            "description": "任务执行",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        stage_log[3] = {
            "stage_type": "evaluate",
            "stage_status": "running",
            "description": "获取结果",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        self.prepare_task(
            True,
            evaluate_map={
                "evaluate_label": "label",
                "predict_label": "prediction",
                "evaluate_function": "pyspark_classify_evaluation",
            },
            evaluate_status="running",
            evaluate_task_id="591_rfc_result_307_15dc05c7",
            evaluate_worker_name="914612748_7",
        )
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "finished",
            "code": EVALUATE_SUCCESS_CODE,
            "message": "Evaluate succesfully",
            "stage": "4/4",
            "evaluate": {"train": {"f1score": 0.9, "precision": 0.8, "recall": 1.0}},
        }
        result = controller.check_job_running(last_log, stage_log)

        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]
        assert stage_log[2]["stage_status"] == "success"
        assert stage_log[3]["stage_status"] == "running"
        task = MLSqlExecuteLog.objects.get(id=self.task_id)
        new_context = json.loads(task.context)
        new_args = new_context["args"]
        assert new_args["evaluate"]["train"]["recall"] == 1.0
        assert new_args["evaluate"]["train"]["f1score"] == 0.9
        assert new_args["evaluate"]["train"]["precision"] == 0.8

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_running")
    def test_running_evaluate_apply_running(self):
        """
        任务执行完成，提交评估任务后，获取评估结果
        """
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "4/4",
        }
        stage_log = copy.deepcopy(self.stage_log)
        stage_log[2] = {
            "stage_type": "execute",
            "stage_status": "success",
            "description": "任务执行",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        stage_log[3] = {
            "stage_type": "evaluate",
            "stage_status": "running",
            "description": "获取结果",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        self.prepare_task(
            True,
            evaluate_map={
                "evaluate_label": "label",
                "predict_label": "prediction",
                "evaluate_function": "pyspark_classify_evaluation",
            },
            evaluate_status="running",
            evaluate_task_id="591_rfc_result_307_15dc05c7",
            evaluate_worker_name="914612748_7",
        )
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "running",
            "code": EVALUATE_RUNNING_CODE,
            "message": "Evaluating running",
            "stage": "4/4",
        }
        result = controller.check_job_running(last_log, stage_log)

        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]
        assert stage_log[2]["stage_status"] == "success"
        assert stage_log[3]["stage_status"] == "running"

    @pytest.mark.usefixtures("mock_evaluation_service")
    @pytest.mark.usefixtures("mock_evaluation_get_result_failed")
    def test_running_evaluate_apply_failed(self):
        """
        任务执行完成，提交评估任务后，获取评估结果
        """
        last_log = {
            "status": "running",
            "level": "INFO",
            "detail": "",
            "time": "2021-02-03 18:27:16",
            "message": "Execute mlsql job.",
            "stage": "4/4",
        }
        stage_log = copy.deepcopy(self.stage_log)
        stage_log[2] = {
            "stage_type": "execute",
            "stage_status": "success",
            "description": "任务执行",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        stage_log[3] = {
            "stage_type": "evaluate",
            "stage_status": "running",
            "description": "获取结果",
            "stage_seq": 3,
            "stage_start": datetime.datetime(2021, 2, 3, 18, 27, 26),
            "time_taken": 0,
        }
        self.prepare_task(
            True,
            evaluate_map={
                "evaluate_label": "label",
                "predict_label": "prediction",
                "evaluate_function": "pyspark_classify_evaluation",
            },
            evaluate_status="running",
            evaluate_task_id="591_rfc_result_307_15dc05c7",
            evaluate_worker_name="914612748_7",
        )
        controller = ModelingJobController(task_id=self.task_id)
        expected_result = {
            "data": "failed",
            "code": EVALUATE_FAILED_CODE,
            "message": "Evaluate failed:failed",
            "stage": "4/4",
        }
        result = controller.check_job_running(last_log, stage_log)

        assert len(result) == len(expected_result)
        assert result["data"] == expected_result["data"]
        assert result["code"] == expected_result["code"]
        assert result["message"] == expected_result["message"]
        assert result["stage"] == expected_result["stage"]
        assert stage_log[2]["stage_status"] == "success"
        assert stage_log[3]["stage_status"] == "running"

    @pytest.mark.usefixtures("mock_param_create_job_info")
    @pytest.mark.usefixtures("mock_get_proc_batch_by_id")
    def test_tensorflow_create_job(self):
        params = {
            "api_version": "v2",
            "processing_id": "591_tensorflow_example_1",
            "project_id": 13105,
            "bk_username": "xxxx",
            "code_version": "0.1.0",
            "cluster_group": "gem",
            "deploy_mode": "k8s",
            "jobserver_config": {"geog_area_code": "xxxx", "cluster_id": "default"},
            "deploy_config": {},
        }
        TensorFlowJobDriver.create_job(params)

    @pytest.mark.usefixtures("mock_param_update_job_info")
    @pytest.mark.usefixtures("mock_get_proc_batch_by_id")
    def test_tensorflow_update_job(self):
        params = {
            "api_version": "v2",
            "processing_id": "591_tensorflow_example_2",
            "project_id": 13105,
            "bk_username": "xxxx",
            "code_version": "0.1.0",
            "cluster_group": "gem",
            "deploy_mode": "k8s",
            "jobserver_config": {"geog_area_code": "xxxx", "cluster_id": "default"},
            "deploy_config": {},
        }
        TensorFlowJobDriver.update_job(params)

    @pytest.mark.usefixtures("mock_jobnavi_get_info")
    @pytest.mark.usefixtures("mock_get_proc_job_info")
    @pytest.mark.usefixtures("mock_jobnavi_create_info")
    @pytest.mark.usefixtures("mock_jobnavi_start_schedule")
    @pytest.mark.usefixtures("mock_get_result_table_storage")
    @pytest.mark.usefixtures("mock_get_result_table")
    @pytest.mark.usefixtures("mock_get_model")
    @pytest.mark.usefixtures("mock_save_batch_job", "mock_batch_job_exist")
    def test_tensorflow_start_job(self):
        job_id = "591_tensorflow_example_1"
        params = {"bk_username": "xxxx", "is_restart": False}
        TensorFlowJobDriver.start_job(job_id, params)
