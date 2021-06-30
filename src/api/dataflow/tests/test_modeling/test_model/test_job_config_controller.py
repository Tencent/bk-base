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

import pytest
from rest_framework.test import APITestCase

from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchJob
from dataflow.modeling.job.job_config_controller import ModelingJobConfigController


@pytest.fixture(scope="function")
def mock_get_proc_batch_job():
    def mock_func(batch_id):
        batch_job = ProcessingBatchJob()
        batch_job.implement_type = "code"
        batch_job.programming_language = "python"
        if batch_id == "591_tensorflow_example_1":
            submit_args = {
                "recovery_info": {"recovery_enable": False, "recovery_interval": "60m", "retry_times": 1},
                "deploy_config": {"user_engine_conf": {"worker": 2, "cpu": 1, "memory": "1.0Gi"}},
                "resource": {"resource_group_id": "default", "queue_name": "default", "cluster_group": "default"},
                "job_id": "591_tensorflow_example_1",
                "schedule_info": {
                    "geog_area_code": "inland",
                    "count_freq": 1,
                    "schedule_period": "hour",
                    "is_restart": False,
                    "cluster_id": "default",
                    "jobnavi_task_type": "tensorflow",
                    "start_time": 1615392000000,
                },
                "udf": [],
                "nodes": {
                    "source": {
                        "591_tensorflow_example_input_1": {
                            "feature_shape": 0,
                            "parent_node_url": "http://xxxx",
                            "storage_conf": {
                                "data_type": "iceberg",
                                "cluster_name": "xxxx",
                                "name_service": "hdfs://xxxx",
                                "storekit_hdfs_conf": {},
                                "cluster_group": "default",
                                "physical_table_name": "591_tensorflow_example_input_1",
                            },
                            "window_offset": "0H",
                            "storage_type": "hdfs",
                            "dependency_rule": "at_least_one_finished",
                            "id": "591_tensorflow_example_input_1",
                            "window_type": "scroll",
                            "window_size": "1H",
                            "name": "591_tensorflow_example_input_1",
                            "fields": [
                                {
                                    "origin": "591_tensorflow_example_input_1",
                                    "field": "field1",
                                    "type": "double",
                                    "description": "CRIM",
                                }
                            ],
                            "label_shape": 0,
                            "role": "batch",
                            "is_managed": 1,
                            "self_dependency_mode": "",
                        },
                        "591_tensorflow_example_1": {
                            "input": {
                                "path": "hdfs://xxxx/591_tensorflow_example_1/tmppath/",
                                "type": "hdfs",
                                "format": "",
                            },
                            "description": "591_tensorflow_example_1",
                            "type": "model",
                            "id": 2334,
                            "name": "591_tensorflow_example_1",
                        },
                    },
                    "transform": {
                        "591_tensorflow_example_1": {
                            "processor_logic": {
                                "programming_language": "Python",
                                "user_package": "hdfs://xxxx/example.zip",
                                "user_args": "optimizer=xxxx epochs=5 batch_size=5",
                                "script": "tensorflow_example.py",
                            },
                            "processor": {
                                "user_main_module": "tensorflow_example",
                                "args": {"epochs": "5", "optimizer": "xxxx", "batch_size": "5"},
                                "type": "untrained-run",
                            },
                            "id": "591_tensorflow_example_1",
                            "processor_type": "tensorflow",
                            "name": "591_tensorflow_example_1",
                        }
                    },
                    "sink": {
                        "591_tensorflow_result_1": {
                            "storage_type": "hdfs",
                            "name": "591_tensorflow_result_1",
                            "call_databus_shipper": False,
                            "storage_conf": {
                                "data_type": "iceberg",
                                "cluster_name": "xxxx",
                                "name_service": "hdfs://xxxx",
                                "storekit_hdfs_conf": {},
                                "cluster_group": "default",
                                "physical_table_name": "591_tensorflow_result_1",
                            },
                            "fields": [
                                {
                                    "origin": "591_tensorflow_result_1",
                                    "field": "field1",
                                    "type": "double",
                                    "description": "CRIM",
                                }
                            ],
                            "data_time_offset": "1H",
                            "only_from_whole_data": False,
                            "id": "591_tensorflow_result_1",
                        }
                    },
                },
                "bk_username": "xxxx",
                "job_name": None,
                "batch_type": "tensorflow",
            }
            batch_job.submit_args = json.dumps(submit_args)
        elif batch_id == "591_spark_simple":
            pass
        return batch_job

    ProcessingBatchJobHandler.get_proc_batch_job = staticmethod(mock_func)


class TestModelingJobConfigController(APITestCase):
    @pytest.mark.usefixtures(
        "mock_get_result_table",
        "mock_get_result_table_storage",
        "mock_get_result_table_fields",
        "mock_get_proc_batch_job",
        "mock_get_model",
        "mock_get_proc_job_info",
        "mock_get_proc_batch_by_id",
    )
    def test_generate_tensorflow_job_config(self):
        job_id = "591_tensorflow_example_1"
        job_type = "tensorflow"
        run_mode = "product"
        is_debug = False
        controller = ModelingJobConfigController(job_id, job_type, run_mode, is_debug)
        param_config = controller.generate_tensorflow_job_config()
        # 模型信息更新过，检查是否为最终更新过后的模型信息
        nodes = param_config["nodes"]
        source_nodes = nodes["source"]
        model_node = source_nodes["591_tensorflow_example_1"]
        model_path = model_node["input"]["path"]
        # 路径不再是旧路径
        assert model_path != "hdfs://xxxx/591_tensorflow_example_1/tmppath/"
        # 路径为最新的路径
        assert model_path == "hdfs://xxxx/"

    @pytest.mark.usefixtures(
        "mock_get_result_table_storage",
        "mock_get_proc_batch_job",
        "mock_release_handler_where",
        "mock_get_proc_job_info",
        "mock_get_result_table_fields",
    )
    def test_generate_spark_job_config(self):
        job_id = "591_spark_simple"
        job_type = "spark_mllib"
        run_mode = "product"
        is_debug = False
        controller = ModelingJobConfigController(job_id, job_type, run_mode, is_debug)
        param_config = controller.generate_spark_job_config()
        # 检查关键信息，按设置，取最新的模型配置
        nodes = param_config["nodes"]
        source_nodes = nodes["source"]
        # 源里面必须要有我们指定的输入表
        assert "591_tensorflow_example_input_1" in source_nodes
        # fields内为新增表的字段信息
        assert len(source_nodes["591_tensorflow_example_input_1"]["fields"]) == 1
        assert source_nodes["591_tensorflow_example_input_1"]["fields"][0]["field"] == "field1"
        transform_nodes = nodes["transform"]
        assert "591_string_indexer_result_1" in transform_nodes
        assert transform_nodes["591_string_indexer_result_1"]["id"] == "591_random_forest_output_table_b2cc4a"

    def test_get_subquery_config_input(self):
        model_config = {
            "source": {
                "fields": [
                    {"type": "double", "field": "column1", "description": "column1"},
                    {"type": "double", "field": "column2", "description": "column2"},
                ]
            },
            "transform": {"processor": {"args": {"column": ["column1", "column2"]}}},
        }
        input_result = ModelingJobConfigController.get_subquery_config_input(model_config)
        assert "feature_columns" in input_result
        assert len(input_result["feature_columns"]) == 2
        feature_columns = input_result["feature_columns"]
        feature_list = [column["field_name"] for column in feature_columns]
        assert "column1" in feature_list and "column2" in feature_list

    @pytest.mark.usefixtures("mock_get_algorithm_by_name")
    def test_get_mlsql_config_input(self):
        model_config = {
            "source": {
                "fields": [
                    {"type": "double", "field": "column1", "description": "column1"},
                    {"type": "double", "field": "column2", "description": "column2"},
                    {"type": "double", "field": "column3", "description": "column3"},
                    {"type": "double", "field": "column4", "description": "column3"},
                ]
            },
            "transform": {
                "fields": [{"field": "new_column_4", "origin": "column4"}],
                "processor": {
                    "name": "decision_tree_classifier",
                    "args": {"other_col": "column3", "features_col": "features_col", "thresholds": 0.1},
                },
                "interpreter": {
                    "features_col": {
                        "implement": "Vectors",
                        "value": ["column1", "column2"],
                    }
                },
            },
        }
        input_result = ModelingJobConfigController.get_mlsql_config_input(model_config)
        assert len(input_result["feature_columns"]) == 4
        column_list = [column["field_name"] for column in input_result["feature_columns"]]
        assert (
            "column1" in column_list
            and "column2" in column_list
            and "column3" in column_list
            and "column4" in column_list
        )
        assert len(input_result["predict_args"]) == 1
        args_list = [args["real_name"] for args in input_result["predict_args"]]
        assert "thresholds" in args_list
