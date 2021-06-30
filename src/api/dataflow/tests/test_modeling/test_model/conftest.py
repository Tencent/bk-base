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
from datetime import datetime, timedelta

import pytest

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.model_release import ModelReleaseHandler
from dataflow.modeling.job.job_config_controller import MODEL_AUTO_UPDATE_POLICY_DELAY
from dataflow.modeling.models import AlgorithmVersion, ModelRelease
from dataflow.modeling.settings import PARSED_TASK_TYPE, ProcessorType
from dataflow.models import ProcessingJobInfo
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper


@pytest.fixture(scope="function")
def mock_get_result_table():
    def mock_func(*args, **kwargs):
        result = {
            "processing_type": "batch",
            "storages": {"hdfs": {"storage_cluster": {"storage_cluster_config_id": 3}}},
        }
        return result

    ResultTableHelper.get_result_table = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_model():
    def mock_func(model_id):
        input_model_info = {
            "id": "591_tensorflow_example_1",
            "model_name": "591_tensorflow_example_1",
            "model_alias": "591_tensorflow_example_1",
            "algorithm_name": "adnet",
            "storage": {"path": "hdfs://xxxx/"},
        }
        return input_model_info

    ModelingHelper.get_model = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_result_table_storage():
    def mock_func(*args, **kwargs):
        connection_info = {"hdfs_url": "hdfs://xxxx/"}
        result = {
            "hdfs": {
                "data_type": "parquet",
                "physical_table_name": "test_name",
                "storage_cluster": {
                    "connection_info": json.dumps(connection_info),
                    "storage_cluster_config_id": 3,
                    "cluster_name": "default",
                    "cluster_group": "default",
                },
            }
        }
        return result

    ResultTableHelper.get_result_table_storage = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_result_table_fields():
    def mock_func(*args, **kwargs):
        return [{"field_name": "field1", "field_type": "string", "field_alias": "alias"}]

    ResultTableHelper.get_result_table_fields = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_proc_job_info():
    def mock_func(job_id):
        job_info = ProcessingJobInfo()
        if job_id == "591_spark_simple":
            # with no heads and tails
            update_hour = datetime.now().hour + 1
            submit_args = {
                "node": {
                    "deployed_model_info": {
                        "model_params": {
                            "double_2": "mock_doule_2",
                            "char_1": "mock_char_1",
                            "double_1": "mock_double_1",
                        },
                        "input_model": "591_tensorflow_example_1",
                        "model_version": "7",
                        "enable_auto_update": True,
                        "auto_update": {
                            "update_policy": MODEL_AUTO_UPDATE_POLICY_DELAY,
                            "update_time": "{update_hour}:10".format(update_hour=update_hour),
                        },
                    },
                    "output": {
                        "591_random_forest_output_table": {
                            "fields": [
                                {
                                    "is_enabled": 1,
                                    "field_type": "double",
                                    "field_alias": "category",
                                    "is_dimension": False,
                                    "field_name": "category",
                                    "field_index": 0,
                                }
                            ],
                            "table_alias": "591_random_forest_output_table",
                        }
                    },
                    "schedule_info": {
                        "count_freq": 1,
                        "data_end": -1,
                        "schedule_period": "day",
                        "data_start": -1,
                        "delay": 0,
                        "accumulate": False,
                        "advanced": {
                            "self_dependency": False,
                            "start_time": None,
                            "recovery_enable": None,
                            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
                            "recovery_interval": "5m",
                            "recovery_times": 1,
                        },
                    },
                    "extra_info": {"cluster": "xxxx", "static_data": [], "batch_type": "default"},
                },
                "dependence": {
                    "591_tensorflow_example_input_1": {
                        "window_delay": 0,
                        "window_type": "fixed",
                        "window_size": 1,
                        "dependency_rule": "all_finished",
                        "window_size_period": "hour",
                    }
                },
            }
            processor_logic = {
                "source": {
                    "591_string_indexer_model": {
                        "input": {
                            "path": "hdfs://xxxx/591_string_indexer_model",
                            "type": "hdfs",
                            "format": "string_indexer",
                        },
                        "type": "model",
                        "id": "591_string_indexer_model",
                        "name": "591_string_indexer_model",
                    },
                    "591_rfc_model": {
                        "input": {
                            "path": "hdfs://xxxx/591_rfc_model",
                            "type": "hdfs",
                            "format": "random_forest_classifier",
                        },
                        "type": "model",
                        "id": "591_rfc_model_204",
                        "name": "591_rfc_model_204",
                    },
                    "591_tensorflow_example_input_1": {
                        "name": "591_tensorflow_example_input_1",
                        "fields": [{"origin": "", "field": "mock_int_1", "type": "int", "description": "mock_int_1"}],
                        "window": {
                            "window_type": "fixed",
                            "window_size": 1,
                            "count_freq": 1,
                            "data_end": -1,
                            "window_delay": 0,
                            "schedule_period": "day",
                            "window_size_period": "hour",
                            "data_start": -1,
                            "delay": 0,
                            "accumulate": False,
                            "dependency_rule": "all_finished",
                            "advanced": {},
                        },
                        "input": {
                            "path": "hdfs://xxxx/591_tensorflow_example_input_1",
                            "table_type": "result_table",
                            "type": "hdfs",
                            "iceberg_config": {
                                "hdfs_config": {},
                                "physical_table_name": "591_tensorflow_example_input_1",
                            },
                            "format": "iceberg",
                        },
                        "type": "data",
                        "id": "591_tensorflow_example_input_1",
                    },
                },
                "transform": {
                    "591_string_indexer_model_abcde": {
                        "description": "591_string_indexer_model_abcde",
                        "fields": [{"origin": "", "field": "indexed", "type": "double"}],
                        "processor": {
                            "args": {"handle_invalid": "keep", "input_col": "char_1", "output_col": "indexed"},
                            "type": "trained-run",
                            "name": "string_indexer",
                        },
                        "parents": ["591_string_indexer_model", "591_tensorflow_example_input_1"],
                        "interpreter": {},
                        "type": "data",
                        "id": "591_string_indexer_model_abcde",
                        "name": "591_string_indexer_model_abcde",
                    },
                    "591_rfc_result_abcde": {
                        "description": "591_rfc_result_abcde",
                        "fields": [{"origin": "", "field": "category", "type": "double"}],
                        "id": "591_rfc_result_abcde",
                        "parents": ["591_string_indexer_model_abcde", "591_rfc_model"],
                        "interpreter": {
                            "features_col": {"implement": "Vectors", "value": ["test_double_1", "test_double_2"]}
                        },
                        "type": "data",
                        "processor": {
                            "args": {"features_col": "features_col", "prediction_col": "category"},
                            "type": "trained-run",
                            "name": "random_forest_classifier",
                        },
                        "name": "591_rfc_result_abcde",
                    },
                },
                "sink": {
                    "591_random_forest_output_table": {
                        "description": "591_random_forest_output_table",
                        "fields": [{"origin": [], "field": "category", "type": "double", "description": "category"}],
                        "table_type": "result_table",
                        "output": {"type": "hdfs", "mode": "overwrite", "format": "parquet"},
                        "type": "data",
                        "id": "591_random_forest_output_table",
                        "name": "591_random_forest_output_table",
                    }
                },
            }
            job_config = {"submit_args": json.dumps(submit_args), "processor_logic": json.dumps(processor_logic)}
            job_info.component_type = "spark_mllib"
        elif job_id == "591_spark_complicated":
            # with head and tail
            job_config = {"heads": [], "tails": []}
            job_info.component_type = "spark_mllib"
        else:
            job_info.component_type = "tensorflow"
            job_config = {"resource_group_id": "default", "processings": [job_id]}
        job_info.created_by = "xxxx"
        job_info.job_id = job_id
        job_info.code_version = "0.1.0"
        job_info.cluster_group = "default"
        job_info.cluster_name = "default"
        job_info.deploy_mode = "k8s"
        job_info.deploy_config = "{}"
        job_info.job_config = json.dumps(job_config)
        job_server_config = {"geog_area_code": "default", "cluster_id": "default"}
        job_info.jobserver_config = json.dumps(job_server_config)
        job_info.implement_type = "code"
        job_info.programming_language = "python"
        return job_info

    ProcessingJobInfoHandler.get_proc_job_info = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_proc_batch_by_id():
    def mock_func(proc_id):
        batch_info = ProcessingBatchInfo()
        batch_info.id = proc_id
        batch_info.processing_id = proc_id
        batch_info.batch_id = proc_id
        batch_info.count_freq = 1
        batch_info.schedule_period = "hour"
        process_logic = {
            "programming_language": "Python",
            "user_package": "hdfs://xxxx/example.zip",
            "user_args": "optimizer=xxxx epochs=5 batch_size=5",
            "script": "tensorflow_example_1.py",
        }
        batch_info.processor_logic = json.dumps(process_logic)
        batch_info.processor_type = "tensorflow"
        batch_info.created_by = "xxxx"
        submit_args = {
            "self_dependency_fields": None,
            "tags": ["default"],
            "start_time": 1615392000000,
            "recovery_enable": False,
            "recovery_times": 1,
            "processing_id": proc_id,
            "is_self_dependency": False,
            "output_result_tables": [
                {
                    "bk_biz_id": 591,
                    "output_baseline": None,
                    "table_name": "591_tensorflow_example_result_1",
                    "output_offset": 0,
                    "fields": [
                        {
                            "field_type": "double",
                            "field_alias": "field1",
                            "field_name": "field1",
                            "is_dimension": False,
                            "field_index": 0,
                        }
                    ],
                    "enable_customize_output": False,
                    "storages": {},
                    "output_baseline_location": False,
                    "only_from_whole_data": False,
                    "result_table_id": "591_tensorflow_example_result_1",
                }
            ],
            "input_result_tables": [
                {
                    "is_static": False,
                    "window_end_offset": None,
                    "feature_shape": 0,
                    "label_shape": 0,
                    "dependency_rule": "at_least_one_finished",
                    "window_start_offset": None,
                    "window_type": "scroll",
                    "window_size": "1H",
                    "window_offset": "0H",
                    "accumulate_start_time": None,
                    "result_table_id": "591_tensorflow_example_input_1",
                }
            ],
            "input_models": [{"model_name": "591_tensorflow_example_1"}],
            "batch_type": "tensorflow",
            "recovery_interval": "60m",
            "project_id": 13105,
            "output_models": [],
            "self_dependency_rule": None,
        }
        batch_info.submit_args = json.dumps(submit_args)
        return batch_info

    ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_release_handler_where():
    def mock_func(*args, **kwargs):
        release = ModelRelease()
        release.version_index = 8
        release.updated_at = datetime.now() - timedelta(days=1)
        model_config_template = {
            "source": {
                "591_rfc_model_1": {
                    "input": {
                        "path": "hdfs://xxxx/591_rfc_model_1",
                        "type": "hdfs",
                        "format": "random_forest_classifier",
                    },
                    "type": "model",
                    "id": "591_rfc_model_1",
                    "name": "591_rfc_model_1",
                },
                "591_string_indexer_1": {
                    "input": {"path": "hdfs://xxxx/591_string_indexer_1", "type": "hdfs", "format": "string_indexer"},
                    "type": "model",
                    "id": "591_string_indexer_1",
                    "name": "591_string_indexer_1",
                },
                "__DATA_SOURCE_ID__": {
                    "input": "__DATA_SOURCE_INPUT__",
                    "type": "data",
                    "id": "__DATA_SOURCE_ID__",
                    "name": "__DATA_SOURCE_ID__",
                    "fields": "__DATA_SOURCE_FIELDS__",
                },
            },
            "transform": {
                "591_rfc_result_1": {
                    "description": "591_rfc_result_1",
                    "fields": [{"origin": "", "field": "category1", "type": "double"}],
                    "processor": {
                        "args": {"features_col": "features_col", "prediction_col": "category1"},
                        "type": "trained-run",
                        "name": "random_forest_classifier",
                    },
                    "parents": ["591_rfc_model_1", "__TRANSFORM_ID___b2cc4a"],
                    "interpreter": {"features_col": {"implement": "Vectors", "value": ["double_1", "double_2"]}},
                    "type": "data",
                    "id": "__TRANSFORM_ID__",
                    "name": "__TRANSFORM_ID__",
                    "task_type": PARSED_TASK_TYPE.MLSQL_QUERY.value,
                },
                "591_string_indexer_result_1": {
                    "description": "591_string_indexer_result_1",
                    "fields": [{"origin": "", "field": "indexed", "type": "double"}],
                    "processor": {
                        "args": {"handle_invalid": "keep", "input_col": "char_1", "output_col": "indexed"},
                        "type": "trained-run",
                        "name": "string_indexer",
                    },
                    "parents": ["591_string_indexer_1", "__DATA_SOURCE_ID__"],
                    "interpreter": {},
                    "type": "data",
                    "id": "__TRANSFORM_ID___b2cc4a",
                    "name": "__TRANSFORM_ID___b2cc4a",
                    "task_type": PARSED_TASK_TYPE.MLSQL_QUERY.value,
                },
            },
        }
        release.model_config_template = json.dumps(model_config_template)
        return [release]

    ModelReleaseHandler.where = classmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_algorithm_by_name():
    def mock_func(*args, **kwargs):
        algorithm_version = AlgorithmVersion()
        algorithm_version.id = 1
        algorithm_version.algorithm_name = "spark_decision_tree_classifier"
        algorithm_version.version = 1
        algorithm_version.logic = "{}"
        algorithm_version.config = (
            "{"
            '"feature_columns": [], '
            '"label_columns": ['
            '{"field_name": "label", '
            '"field_type": "double", '
            '"field_alias": "alias", '
            '"field_index": 1, '
            '"value": null, '
            '"default_value": "label", '
            '"allowed_values": null, '
            '"sample_value": null, '
            '"real_name": "label_col"}'
            "], "
            '"predict_output": ['
            '{"field_name": "features_col", '
            '"field_type": "double", '
            '"field_alias": "alias", '
            '"field_index": 0, '
            '"value": null, '
            '"default_value": "prediction", '
            '"allowed_values": null, '
            '"sample_value": null, '
            '"real_name": "features_col"}], '
            '"training_args": ['
            '{"field_name": "cache_node_ids", '
            '"field_type": "boolean", '
            '"field_alias": "alias", '
            '"field_index": 0, "value": null, '
            '"default_value": "false", '
            '"allowed_values": ["true", "false"], '
            '"sample_value": null, "real_name": "cache_node_ids", "is_advanced": false}], '
            '"predict_args": ['
            '{"field_name": "thresholds", '
            '"field_type": "double[]", '
            '"field_alias": "alias", '
            '"field_index": 0, '
            '"value": null, "default_value": null, "allowed_values": null, '
            '"sample_value": null, "real_name": "thresholds"}], '
            '"feature_columns_mapping": {"spark": '
            '{"multi": [{'
            '"field_name": "features_col", "field_type": "double", '
            '"field_alias": "alias", "field_index": 0, "value": null, '
            '"default_value": "features", '
            '"allowed_values": null, "sample_value": null, '
            '"real_name": "features_col", "composite_type": "Vector", '
            '"need_interprete": true}], '
            '"single": [{'
            '"field_name": "other_col", "field_type": "double",'
            '"field_alias": "alias", "field_index": 0, "value": null,'
            '"default_value": "features", '
            '"allowed_values": null, "sample_value": null, '
            '"real_name": "other_col", '
            '"need_interprete": true'
            '}]}}, "feature_colunms_changeable": true}'
        )
        algorithm_version.execute_config = ""
        algorithm_version.properties = ""
        algorithm_version.created_by = "xxxx"
        algorithm_version.created_at = datetime.now()
        algorithm_version.updated_by = "xxxx"
        algorithm_version.updated_at = datetime.now()
        algorithm_version.description = "description"
        return algorithm_version

    AlgorithmVersionHandler.get_alorithm_by_name = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_proc_batch_info_by_proc_id():
    def mock_func(processing_id):
        batch_info = ProcessingBatchInfo()
        batch_info.processing_id = "591_model_result"
        processor_logic = {
            "logic": {"processor": {"type": ProcessorType.TRAINED_RUN.value}},
            "model": {"name": "591_rfc_model_release_1"},
        }
        if processing_id == "1":
            processor_logic["logic"]["task_type"] = PARSED_TASK_TYPE.SUB_QUERY.value
        elif processing_id == "2":
            processor_logic["operate_type"] = "train"
            processor_logic["logic"]["task_type"] = PARSED_TASK_TYPE.MLSQL_QUERY.value
        elif processing_id == "3":
            processor_logic["logic"]["task_type"] = PARSED_TASK_TYPE.MLSQL_QUERY.value
            processor_logic["operate_type"] = "insert"
        elif processing_id == "4":
            processor_logic["logic"]["task_type"] = PARSED_TASK_TYPE.MLSQL_QUERY.value
            processor_logic["operate_type"] = "create"
        batch_info.processor_logic = json.dumps(processor_logic)
        batch_info.submit_args = {
            "node": {
                "deployed_model_info": {
                    "model_params": {"input_cols": "a,b,c", "max_iter": 10},
                    "input_model": "591_input_model_4",
                    "model_version": "3",
                    "enable_auto_update": False,
                },
                "output": {
                    "591_model_result": {
                        "data_set_id": "591_model_result",
                        "data_set_type": "result_table",
                        "fields": [{"origin": "", "field": "log1", "type": "string", "description": "log1"}],
                        "table_name_ailas": "model_test",
                    }
                },
                "extra_info": {"cluster": "xxxx", "static_data": [], "batch_type": "default"},
                "schedule_info": {
                    "count_freq": 1,
                    "data_end": -1,
                    "schedule_period": "hour",
                    "data_start": -1,
                    "delay": 600,
                    "advanced": {
                        "self_dependency": True,
                        "start_time": 1584414000000,
                        "recovery_enable": False,
                        "self_dependency_config": {},
                        "recovery_interval": "5m",
                        "recovery_times": 1,
                    },
                },
            },
            "dependence": {
                "591_modeling_query_set": {
                    "window_type": "accumulate",
                    "window_size": 1,
                    "window_delay": 0,
                    "window_size_period": "hour",
                    "is_managed": 1,
                    "dependency_rule": "all_finished",
                    "type": "clean",
                }
            },
        }
        return batch_info

    ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id = staticmethod(mock_func)
