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

from datetime import datetime

import pytest
from rest_framework.test import APITestCase

from dataflow.modeling.algorithm.algorithm_controller import AlgorithmController
from dataflow.modeling.model.platform_model import PlatformModel
from dataflow.modeling.models import Algorithm, AlgorithmVersion
from dataflow.shared.log import modeling_logger as logger


def init_algrithm_list():
    def mock_func(*args, **kwargs):
        algorithm = Algorithm()
        algorithm.algorithm_name = "spark_decision_tree_classifier"
        algorithm.algorithm_alias = "decision_tree_classifier"
        algorithm.description = "description"
        algorithm.algorithm_original_name = "decision_tree_classifier"
        algorithm.algorithm_type = "classification"
        algorithm.generate_type = "system"
        algorithm.sensitivity = "public"
        algorithm.project_id = 0
        algorithm.run_env = "spark_cluster"
        algorithm.framework = "spark_mllib"
        algorithm.created_by = "xxxx"
        algorithm.created_at = datetime.now()
        algorithm.updated_by = "xxxx"
        algorithm.updated_at = datetime.now()
        return [algorithm]

    return mock_func


def init_algorithm_detail():
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
            '"single": []}}, "feature_colunms_changeable": true}'
        )
        algorithm_version.execute_config = ""
        algorithm_version.properties = ""
        algorithm_version.created_by = "xxxx"
        algorithm_version.created_at = datetime.now()
        algorithm_version.updated_by = "xxxx"
        algorithm_version.updated_at = datetime.now()
        algorithm_version.description = "description"
        return algorithm_version

    return mock_func


@pytest.fixture(scope="function")
def mock_get_user_algorithm_list():
    PlatformModel.get_algrithm_by_user = init_algrithm_list()


@pytest.fixture(scope="function")
def mock_get_algorithm_by_name():
    PlatformModel.get_algorithm_by_name = init_algorithm_detail()


class TestAlgorithmController(APITestCase):
    def setUp(self):
        pass

    @pytest.mark.usefixtures("mock_get_user_algorithm_list")
    @pytest.mark.usefixtures("mock_get_algorithm_by_name")
    def test_get_algorithm_list(self):
        controller = AlgorithmController()
        result = controller.get_algorithm_list("spark_mllib")
        assert "spark_mllib" in result
        assert len(result["spark_mllib"]) == 1
        group = result["spark_mllib"][0]
        assert len(group["alg_info"]) == 1
        algorithm_item = group["alg_info"][0]
        sql = algorithm_item["sql"]
        logger.info("result sql:" + sql)
        expected_sql = "train model 模型名称 \n options(algorithm='{algorithm_name}', {param_list})\n from 查询结果集名称".format(
            algorithm_name="decision_tree_classifier",
            param_list="features_col=<字段1,字段2,字段3>, label_col=字段名, cache_node_ids='false'",
        )
        logger.info("expected sql:" + expected_sql)
        assert expected_sql == sql
