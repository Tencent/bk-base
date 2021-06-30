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

import uuid

import pytest
from rest_framework.test import APITestCase

import dataflow
from dataflow.batch.api.api_helper import BksqlHelper
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.exceptions.comp_exceptions import (
    CreateModelProcessingException,
    EntityAlreadyExistsError,
    ModelCreateError,
    RTCreateError,
    SQLForbiddenError,
    SqlParseError,
    TableNotQuerySetError,
)
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.handler.model_instance import ModelInstanceHandler
from dataflow.modeling.models import Algorithm
from dataflow.modeling.processing.bulk_params import DataflowProcessingBulkParams, DatalabProcessingBulkParams
from dataflow.modeling.processing.modeling_processing import DataflowTableOperator, DatalabTableOperator, TableOperator
from dataflow.modeling.processing.tensorflow.tensorflow_batch_operator import DataflowTensorFlowOperator
from dataflow.modeling.settings import PARSED_TASK_TYPE
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.udf.functions import function_driver


def get_hdfs_cluster_group_mock(cluster_type, cluster_name):
    def mock_func(*args, **kwargs):
        return {"id": 24, "connection": {"hdfs_url": "hdfs://xxxx"}}

    return mock_func


def create_model_info_mock():
    def mock_func(*args, **kwargs):
        pass

    return mock_func


def save_or_update_processings_mock():
    def mock_func(*args, **kwargs):
        pass

    return mock_func


@pytest.fixture(scope="function")
def mock_create_model_info():
    dataflow.modeling.processing.modeling_processing.DatalabTableOperator.create_model_info = staticmethod(
        create_model_info_mock()
    )


@pytest.fixture(scope="function")
def mock_save_or_update_processings():
    dataflow.modeling.processing.modeling_processing.DatalabTableOperator.save_or_update_processings = (
        save_or_update_processings_mock()
    )


@pytest.fixture(scope="function")
def mock_get_hdfs_cluster_group():
    dataflow.shared.storekit.storekit_helper.StorekitHelper.get_storage_cluster_config = staticmethod(
        get_hdfs_cluster_group_mock("hdfs", "xxxx")
    )


@pytest.fixture(scope="function")
def mock_function_driver_parse_sql():
    def mock_func(*args, **kwargs):
        data = [
            {
                "name": "udf_string_length",
                "version": "1.0",
                "language": "python",
                "type": "udf",
                "hdfs_path": "hdfs://xxxx/udf/udf_string_length.py",
                "local_path": "/test/udf/udf_string_length.py",
            }
        ]
        return data

    function_driver.parse_sql = mock_func


@pytest.fixture(scope="function")
def mock_bksql_get_table_names():
    def mock_func(*args, **kwargs):
        return ["591_new_modeling_query_set"]

    ModelingApiHelper.get_table_names_by_mlsql_parser = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_bksql_spark_sql():
    def mock_func(*args, **kwargs):
        fields = [{"field_name": "field1", "field_type": "string", "field_alias": "alias", "field_index": 0}]
        sql = "demo sql"
        return [{"fields": fields, "sql": sql}]

    BksqlHelper.spark_sql = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_columns_by_mlsql_parser():
    def mock_func(*args, **kwargs):
        return {"sql": "demo sql", "columns": ["field1", "field2"]}

    ModelingApiHelper.get_columns_by_mlsql_parser = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_get_mlsql_query_source_parser():
    def mock_func(*args, **kwargs):
        return "20210505_20210506"

    ModelingApiHelper.get_mlsql_query_source_parser = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_dataflow_save_processings():
    def mock_func(operator, processing_info, data_processing_info, is_update):
        # assert actual_processing_info == processing_info
        # assert actual_data_processing == data_processing_info
        pass

    DataflowTableOperator.save_or_update_processings = mock_func


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
def mock_model_instance_save():
    def mock_func(*args, **kwargs):
        pass

    ModelInstanceHandler.save = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_is_proc_batch_exists():
    def mock_func(proc_id):
        if proc_id == "591_tensorflow_example_1":
            return False
        else:
            return True

    ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_dp_set_dp():
    def mock_func(proc_args):
        pass

    DataProcessingHelper.set_data_processing = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_storekit_create_physical():
    def mock_func(*args, **kwargs):
        pass

    StorekitHelper.create_physical_table = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_storekit_prepare():
    def mock_func(*args, **kwargs):
        pass

    StorekitHelper.prepare = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_create_batch_info_v2():
    def mock_func(*args, **kwargs):
        pass

    ProcessingBatchInfoHandler.save_proc_batch_info_v2 = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_list_result_tables():
    result_obj = {"result": True, "content": {"read": {}, "write": {}}}

    def mock_func(params):
        sql = params["sql"]
        if "1" == sql:
            result_obj["content"]["read"]["result_table"] = ["read_table_1"]
            result_obj["content"]["read"]["model"] = ["read_model_0"]
            result_obj["content"]["write"]["result_table"] = []
            result_obj["content"]["write"]["model"] = ["write_model_1"]
        elif "2" == sql:
            result_obj["content"]["read"]["result_table"] = ["read_table_1"]
            result_obj["content"]["read"]["model"] = ["write_model_1"]
            result_obj["content"]["write"]["result_table"] = ["result_table_2"]
            result_obj["content"]["write"]["model"] = []
        elif "3" == sql:
            result_obj["content"]["read"]["result_table"] = ["result_table_2"]
            result_obj["content"]["read"]["model"] = []
            result_obj["content"]["write"]["result_table"] = []
            result_obj["content"]["write"]["model"] = ["write_model_3"]
        elif "4" == sql:
            result_obj["content"]["read"]["result_table"] = ["result_table_2"]
            result_obj["content"]["read"]["model"] = ["write_model_3"]
            result_obj["content"]["write"]["result_table"] = ["result_table_4"]
            result_obj["content"]["write"]["model"] = ["write_model_4"]
        elif "5" == sql:
            result_obj["content"]["type"] = "drop_model"
            result_obj["content"]["data"] = "drop_model_1"
        elif "6" == sql:
            result_obj["content"]["type"] = "drop_table"
            result_obj["content"]["data"] = "drop_table_2"
        else:
            result_obj["content"]["type"] = "truncate"
            result_obj["content"]["data"] = "truncate_table_1"
        return result_obj

    ModelingApiHelper.list_result_tables_by_mlsql_parser = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_check_table_exists():
    def mock_func(table_name):
        if table_name.find("_table_") != -1:
            return True
        else:
            return False

    TableOperator.check_table_exists = staticmethod(mock_func)


@pytest.fixture(scope="function")
def mock_fetch_model_by_name():
    def mock_fun(model_name):
        return model_name

    MLSqlModelInfoHandler.fetch_model_by_name = staticmethod(mock_fun)


class TestModelingProcessing(APITestCase):
    databases = "__all__"

    service_list = []

    def setUp(self):
        algorithm = Algorithm.objects.filter(algorithm_name="pyspark_classify_evaluation")
        if not algorithm.exists():
            Algorithm.objects.create(
                algorithm_name="pyspark_classify_evaluation",
                algorithm_alias="【官方】分类评估",
                description="对分类模型的预测结果进行评估",
                algorithm_original_name="pyspark_classify_evaluation",
                algorithm_type="evaluation",
                generate_type="system",
                sensitivity="public",
                project_id=3481,
                run_env="spark_cluster",
                framework="pyspark_mllib",
                created_by="admin",
                updated_by="admin",
            )

    def test_refine_evaluate_info(self):
        evaluate_map = {"predict_label": "prediction", "evaluate_type": "classify"}
        algorithm_name = "random_forest_classifier"
        target_fields = [
            {"field": "prediction", "type": "double", "origins": [""]},
            {"field": "label", "type": "double", "origins": ["double_1"]},
            {"field": "double_2", "type": "double", "origins": ["double_2"]},
            {"field": "indexed", "type": "double", "origins": ["indexed"]},
        ]
        expected_evalaute_map = {
            "evaluate_function": "pyspark_classify_evaluation",
            "evaluate_label": "label",
            "evaluate_type": "classify",
            "predict_label": "prediction",
        }
        expected_result = True
        result = DatalabTableOperator.refine_evaluate_info(evaluate_map, algorithm_name, target_fields)
        assert result == expected_result
        assert len(evaluate_map) == len(expected_evalaute_map)
        assert evaluate_map["evaluate_function"] == expected_evalaute_map["evaluate_function"]
        assert evaluate_map["evaluate_label"] == expected_evalaute_map["evaluate_label"]
        assert evaluate_map["evaluate_type"] == expected_evalaute_map["evaluate_type"]

    @pytest.mark.usefixtures("mock_get_hdfs_cluster_group")
    @pytest.mark.usefixtures("mock_create_model_info")
    @pytest.mark.usefixtures("mock_save_or_update_processings")
    def test_parse_mlsql_result(self):
        params = {
            "bk_biz_id": 591,
            "project_id": 13105,
            "use_scenario": "datalab",
            "tags": ["inland"],
            "sql_list": [
                "create table 591_rfc_result_315 as run 591_rfc_model_314(features_col=<double_1,double_2>) "
                "as prediction,double_1 as label,double_2,indexed  from 591_string_indexer_result_315"
            ],
            "notebook_id": 883,
            "cell_id": "abcde",
            "bk_username": "xxxx",
        }
        bulk_params = DatalabProcessingBulkParams(params)
        table_operator = DatalabTableOperator(bulk_params)
        table_name = "591_rfc_result_{}".format(uuid.uuid4().hex[0:8])
        parsed_task = {
            "evaluate_map": {
                "predict_label": "prediction",
                "evaluate_type": "classify",
            },
            "table_name": table_name,
            "name": "591_rfc_model_314_run",
            "model_info": {
                "algorithm_name": "random_forest_classifier",
                "modelStorageId": {"path": "hdfs://xxxx/app/mlsql/models/591_rfc_model_314"},
                "model_storage": {"path": "hdfs://xxxx/app/mlsql/models/591_rfc_model_314"},
                "model_name": "591_rfc_model_314",
                "model_alias": "591_rfc_model_314",
            },
            "fields": [
                {"field": "prediction", "type": "double", "origins": [""]},
                {"field": "label", "type": "double", "origins": ["double_1"]},
                {"field": "double_2", "type": "double", "origins": ["double_2"]},
                {"field": "indexed", "type": "double", "origins": ["indexed"]},
            ],
            "processor": {
                "args": {
                    "features_col": "features_col",
                    "prediction_col": "prediction",
                },
                "type": "trained-run",
                "name": "random_forest_classifier",
            },
            "table_need_create": True,
            "parents": ["591_string_indexer_result_315"],
            "task_type": "mlsql_query",
            "interpreter": {
                "features_col": {
                    "implement": "Vectors",
                    "value": ["double_1", "double_2"],
                }
            },
            "write_mode": "overwrite",
            "type": "data",
            "id": "591_rfc_model_314_run",
            "description": "591_rfc_model_314_run",
        }
        success = True
        try:
            table_operator.parse_mlsql_result(parsed_task, params["sql_list"][0], 0)
        except (
            EntityAlreadyExistsError,
            SqlParseError,
            ModelCreateError,
            RTCreateError,
            SQLForbiddenError,
            TableNotQuerySetError,
            CreateModelProcessingException,
        ):
            success = False

        assert success

        assert "evaluate_map" in parsed_task
        evaluate_map = parsed_task["evaluate_map"]
        expected_evalaute_map = {
            "evaluate_function": "pyspark_classify_evaluation",
            "evaluate_label": "label",
            "evaluate_type": "classify",
            "predict_label": "prediction",
        }
        assert len(evaluate_map) == len(expected_evalaute_map)
        assert evaluate_map["evaluate_function"] == expected_evalaute_map["evaluate_function"]
        assert evaluate_map["evaluate_label"] == expected_evalaute_map["evaluate_label"]
        assert evaluate_map["evaluate_type"] == expected_evalaute_map["evaluate_type"]

    def test_refine_evaluate_info_no_need(self):
        target_fields = [{"field": "abc"}]
        evaluate_map = {"evaluate_type": "classify"}
        algorithm_name = "random_forest_classifier"
        need_evalaute = DatalabTableOperator.refine_evaluate_info(evaluate_map, algorithm_name, target_fields)
        assert not need_evalaute

    @pytest.mark.usefixtures("mock_save_or_update_processings")
    @pytest.mark.usefixtures("mock_function_driver_parse_sql")
    @pytest.mark.usefixtures("mock_bksql_get_table_names")
    @pytest.mark.usefixtures("mock_bksql_spark_sql")
    @pytest.mark.usefixtures("mock_get_result_table_fields")
    @pytest.mark.usefixtures("mock_get_columns_by_mlsql_parser")
    @pytest.mark.usefixtures("mock_get_mlsql_query_source_parser")
    def test_parse_subquery_result(self):
        sql = (
            "create table 591_string_indexer_result_306 "
            "as run 591_string_indexer_287("
            "input_col=char_1,handle_invalid='keep') as indexed_new, double_1,double_2,str_len "
            "from ("
            "select double_1, double_2,char_1, udf_string_length(string_1) as str_len "
            "from "
            "591_new_modeling_query_set "
            "where int_2>0)"
        )

        sub_sql = (
            "select double_1, double_2,char_1, udf_string_length(string_1) as str_len "
            "from 591_new_modeling_query_set "
            "where int_2>0"
        )
        operate_type = "create"
        target_entity_name = "591_string_indexer_result_306"
        geog_area_code = "defaule"
        sub_query_has_join = False
        sql_index = 0
        params = {
            "bk_biz_id": 591,
            "project_id": 13105,
            "use_scenario": "datalab",
            "tags": ["inland"],
            "sql_list": [],
            "notebook_id": 883,
            "cell_id": "abcde",
            "bk_username": "xxxx",
        }
        bulk_params = DatalabProcessingBulkParams(params)
        table_operator = DatalabTableOperator(bulk_params)
        parse_result = table_operator.parse_subquery_result(
            sql, sub_sql, operate_type, target_entity_name, geog_area_code, sub_query_has_join, sql_index
        )

        assert parse_result["processing_id"].startswith(target_entity_name)
        assert parse_result["table_name"].startswith(target_entity_name)
        assert len(table_operator.processing_ids) == 1
        assert table_operator.processing_ids[0].startswith(target_entity_name)
        assert len(parse_result["fields"]) == 1 and parse_result["fields"][0]["field"] == "field1"
        assert parse_result["parents"][0] == "591_new_modeling_query_set"
        assert parse_result["processor"]["name"] == "tmp_processor"
        assert len(parse_result["processor"]["args"]["column"]) == 1
        assert parse_result["processor"]["args"]["column"][0] == "field1"
        assert len(parse_result["udfs"]) == 1
        assert parse_result["task_type"] == PARSED_TASK_TYPE.SUB_QUERY.value

    @pytest.mark.usefixtures("mock_get_result_table_fields")
    @pytest.mark.usefixtures("mock_get_result_table_storage")
    @pytest.mark.usefixtures("mock_get_proc_batch_info_by_proc_id")
    @pytest.mark.usefixtures("mock_dataflow_save_processings")
    @pytest.mark.usefixtures("mock_get_result_table")
    @pytest.mark.usefixtures("mock_release_handler_where")
    @pytest.mark.usefixtures("mock_model_instance_save")
    def test_dataflow_deal_with_processings(self):
        params = {
            "model_config": {
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
                        "input": {
                            "path": "hdfs://xxxx/591_string_indexer_1",
                            "type": "hdfs",
                            "format": "string_indexer",
                        },
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
                    "591_rfc_result_1_abcde": {
                        "description": "591_rfc_result_1_abcde",
                        "task_type": "sub_query",
                        "fields": [
                            {"origin": "", "field": "category1", "type": "double"},
                            {"origin": "double_1", "field": "double_1", "type": "double"},
                            {"origin": "double_2", "field": "double_2", "type": "double"},
                        ],
                        "processor": {
                            "args": {
                                "features_col": "features_col",
                                "prediction_col": "category1",
                                "format_sql": "SELECT double_1, double_2, char_1,"
                                "udf_string_length(string_1) AS str_len "
                                "FROM __DATA_SOURCE_INPUT_SQL__ WHERE int_2 > 0",
                            },
                            "type": "untrained-run",
                            "name": "random_forest_classifier",
                        },
                        "parents": ["__DATA_SOURCE_ID__"],
                        "interpreter": {"features_col": {"implement": "Vectors", "value": ["double_1", "double_2"]}},
                        "type": "data",
                        "id": "__TRANSFORM_ID__abcde",
                        "name": "__TRANSFORM_ID__abcde",
                    },
                    "591_rfc_result_1_fghij": {
                        "description": "591_rfc_result_1_fghij",
                        "task_type": "mlsql_query",
                        "fields": [
                            {"origin": "", "field": "category1", "type": "double"},
                            {"origin": "double_1", "field": "double_1", "type": "double"},
                            {"origin": "double_2", "field": "double_2", "type": "double"},
                        ],
                        "processor": {
                            "args": {"features_col": "features_col", "prediction_col": "category1"},
                            "type": "trained-run",
                            "name": "random_forest_classifier",
                        },
                        "parents": ["591_rfc_model_1", "__TRANSFORM_ID__abcde"],
                        "interpreter": {"features_col": {"implement": "Vectors", "value": ["double_1", "double_2"]}},
                        "type": "data",
                        "id": "__TRANSFORM_ID__fghij",
                        "name": "__TRANSFORM_ID__fghij",
                    },
                    "591_string_indexer_result_1_klmno": {
                        "description": "591_string_indexer_result_1_klmno",
                        "task_type": "sub_query",
                        "fields": [
                            {"origin": "", "field": "indexed", "type": "double"},
                            {"origin": "doble_1", "field": "double_1", "type": "double"},
                            {"origin": "double_2", "field": "double_2", "type": "double"},
                        ],
                        "processor": {
                            "args": {
                                "handle_invalid": "keep",
                                "input_col": "char_1",
                                "output_col": "indexed",
                                "format_sql": "SELECT double_1, double_2, char_1,"
                                "udf_string_length(string_1) AS str_len "
                                "FROM __DATA_SOURCE_INPUT_SQL__ WHERE int_2 > 0",
                            },
                            "type": "untrained-run",
                            "name": "string_indexer",
                        },
                        "parents": ["__TRANSFORM_ID__fghij"],
                        "interpreter": {},
                        "type": "data",
                        "id": "__TRANSFORM_ID__klmno",
                        "name": "__TRANSFORM_ID__klmno",
                    },
                    "591_string_indexer_result_1": {
                        "description": "591_string_indexer_result_1",
                        "fields": [
                            {"origin": "", "field": "indexed", "type": "double"},
                            {"origin": "double_1", "field": "double_1", "type": "double"},
                            {"origin": "double_2", "field": "double_2", "type": "double"},
                        ],
                        "processor": {
                            "args": {"handle_invalid": "keep", "input_col": "char_1", "output_col": "indexed"},
                            "type": "trained-run",
                            "name": "string_indexer",
                        },
                        "parents": ["591_string_indexer_1", "__TRANSFORM_ID__klmno"],
                        "interpreter": {},
                        "type": "data",
                        "id": "__TRANSFORM_ID__",
                        "name": "__TRANSFORM_ID__",
                    },
                },
            },
            "submit_args": {
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
                "node": {
                    "schedule_info": {
                        "count_freq": 1,
                        "schedule_period": "hour",
                        "delay": 600,
                        "data_start": -1,
                        "data_end": -1,
                        "advanced": {},
                    },
                    "deployed_model_info": {
                        "model_params": {"input_cols": "a,b,c", "max_iter": 10},
                        "input_model": "591_input_model_4",
                        "model_version": "4",
                        "enable_auto_update": False,
                    },
                    "output": {
                        "591_model_result": {
                            "data_set_id": "591_model_result",
                            "data_set_type": "result_table",
                            "table_alias": "591_model_result",
                            "fields": [
                                {"field_type": "string", "field_alias": "log", "field_name": "log", "origin": ""}
                            ],
                        }
                    },
                    "extra_info": {"static_data": [], "cluster": "xxxx", "batch_type": "default"},
                },
            },
            "use_scenario": "dataflow",
            "project_id": 13105,
            "bk_biz_id": 591,
            "tags": ["inland"],
            "bk_username": "xxxx",
        }
        bulk_params = DataflowProcessingBulkParams(params)
        operator = DataflowTableOperator(bulk_params)
        result = operator.deal_with_processing(False)
        assert result["processing_id"] == "591_model_result"
        assert result["result_table_ids"][0] == "591_model_result"

    @pytest.mark.usefixtures("mock_is_proc_batch_exists")
    @pytest.mark.usefixtures("mock_get_result_table_storage")
    @pytest.mark.usefixtures("mock_dp_set_dp")
    @pytest.mark.usefixtures("mock_storekit_prepare")
    @pytest.mark.usefixtures("mock_storekit_create_physical")
    @pytest.mark.usefixtures("mock_create_batch_info_v2")
    def test_tf_create_processing(self):
        params = {
            "api_version": "v2",
            "bk_username": "xxxx",
            "project_id": 13105,
            "tags": ["inland"],
            "name": "tensorflow_example_1",
            "bk_biz_id": 591,
            "processor_logic": {
                "user_args": "optimizer=xxxx epochs=5 batch_size=5",
                "user_main_class": "tensorflow_example_1.py",
                "user_package": {"path": "hdfs://xxxx/example.zip", "id": "123"},
            },
            "dedicated_config": {
                "batch_type": "tensorflow",
                "self_dependence": {"self_dependency": False, "self_dependency_config": {}},
                "schedule_config": {"count_freq": 1, "schedule_period": "hour", "start_time": "2021-03-11 00:00:00"},
                "recovery_config": {"recovery_enable": False, "recovery_times": "1", "recovery_interval": "60m"},
                "input_config": {
                    "models": [{"name": "591_tensorflow_example_1"}],
                    "tables": [{"result_table_id": "591_tensorflow_example_input_1", "label_shape": 0}],
                },
                "output_config": {
                    "tables": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "591_tensorflow_example_result_1",
                            "table_alias": "591_tensorflow_example_result_1",
                            "enable_customize_output": False,
                            "need_create_storage": True,
                            "fields": [
                                {
                                    "is_dimension": False,
                                    "field_name": "field1",
                                    "field_type": "double",
                                    "field_alias": "field1",
                                    "field_index": 0,
                                },
                                {
                                    "is_dimension": False,
                                    "field_name": "field2",
                                    "field_type": "double",
                                    "field_alias": "field2",
                                    "field_index": 1,
                                },
                            ],
                        }
                    ],
                    "models": [
                        {"name": "tensorflow_output_model", "alias": "tensorflow_output_model", "bk_biz_id": 591}
                    ],
                },
            },
            "window_info": [
                {
                    "result_table_id": "591_tensorflow_example_input_1",
                    "window_type": "scroll",
                    "window_offset": "0",
                    "window_offset_unit": "hour",
                    "dependency_rule": "at_least_one_finished",
                    "window_size": "2",
                    "window_size_unit": "hour",
                    "window_start_offset": "1",
                    "window_start_offset_unit": "hour",
                    "window_end_offset": "2",
                    "window_end_offset_unit": "hour",
                    "accumulate_start_time": "2021-03-11 00:00:00",
                }
            ],
        }
        operator = DataflowTensorFlowOperator(params)
        result = operator.create_processing()
        assert result["processing_id"] == "591_tensorflow_example_1"

    @pytest.mark.usefixtures("mock_list_result_tables")
    @pytest.mark.usefixtures("mock_check_table_exists")
    def test_parse_sql_table(self):
        read_table, write_table, read_model, write_model = TableOperator.parse_sql_table(["1", "2", "3", "4"])
        assert len(read_table) == 1 and "read_table_1" in read_table
        assert len(write_table) == 1 and "result_table_4" in write_table
        assert len(read_model) == 1 and "read_model_0" in read_model
        assert len(write_model) == 1 and "write_model_4" in write_model

    @pytest.mark.usefixtures("mock_list_result_tables")
    @pytest.mark.usefixtures("mock_fetch_model_by_name")
    def test_parse_ddl_table(self):
        drop_table, drop_model, write_table = TableOperator.parse_ddl_table(["5", "6", "7"])
        assert len(drop_table) == 1 and "drop_table_2" in drop_table
        assert len(drop_model) == 1 and "drop_model_1" in drop_model
        assert len(write_table) == 1 and "truncate_table_1" in write_table
