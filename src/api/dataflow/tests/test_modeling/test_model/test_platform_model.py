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

import mock
from rest_framework.test import APITestCase

from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.modeling.exceptions.comp_exceptions import ModelReleaseInspectionError
from dataflow.modeling.model.platform_model import PlatformModel


class TestPlatformModel(APITestCase):
    databases = "__all__"

    @mock.patch("dataflow.modeling.api.api_helper.ModelingApiHelper.list_result_tables_by_mlsql_parser")
    def test_inspection_before_release(self, list_result_tables_by_mlsql_parser):
        # 正常逻辑验证
        sql = """
            %%msql
            --进行预测
            create table 591_string_indexer_result_release_1
            as run 591_string_indexer_release_1(
                input_col=char_1,
                handle_invalid='keep') as indexed,double_1,double_2
            from 591_xxx_mock_offline_10;

            --生成新的随机森林算法
            train model 591_rfc_model_release_1
            options(
                algorithm='random_forest_classifier',
                features_col=<double_1,double_2>,
                label_col=indexed)
            from 591_string_indexer_result_release_1;

            --继续进行预测
            create table 591_rfc_result_release_1
            as run 591_rfc_model_release_1(
                features_col=<double_1,double_2>) as category1,double_1,double_2
            from 591_string_indexer_result_release_1;
        """
        list_result_tables_by_mlsql_parser.return_value = {
            "result": True,
            "content": {
                "read": {
                    "result_table": ["591_string_indexer_result_release_1"],
                    "model": ["591_rfc_model_release_1"],
                },
                "write": {"result_table": ["591_rfc_result_release_1"], "model": []},
            },
            "code": "1586200",
        }
        ProcessingBatchInfo.objects.create(
            processing_id="591_rfc_result_release_1",
            batch_id="591_rfc_result_release_1",
            processor_type="sql",
            processor_logic='{"logic":{"processor":{"type":"trained-run"}}, '
            '"model":{"name":"591_rfc_model_release_1"}}',
            component_type="spark_mllib",
            count_freq=0,
            delay=0,
        )
        inspect_result = PlatformModel.inspection_before_release(sql)
        last_sql = inspect_result["last_sql"]
        model_name = inspect_result["model_name"]
        assert (
            last_sql
            == """--继续进行预测
            create table 591_rfc_result_release_1
            as run 591_rfc_model_release_1(
                features_col=<double_1,double_2>) as category1,double_1,double_2
            from 591_string_indexer_result_release_1"""
        )

        assert model_name == "591_rfc_model_release_1"

        # 异常逻辑验证
        sql1 = """
            %%msql
            --进行预测
            create table 591_string_indexer_result_release_1
            as run 591_string_indexer_release_1(
                input_col=char_1,
                handle_invalid='keep') as indexed,double_1,double_2
            from 591_xxx_mock_offline_10;

            --生成新的随机森林算法
            train model 591_rfc_model_release_1
            options(
                algorithm='random_forest_classifier',
                features_col=<double_1,double_2>,
                label_col=indexed)
            from 591_string_indexer_result_release_1;
        """
        list_result_tables_by_mlsql_parser.return_value = {
            "result": True,
            "content": {
                "read": {
                    "result_table": ["591_string_indexer_result_release_1"],
                    "model": [],
                },
                "write": {"result_table": [], "model": ["591_rfc_model_release_1"]},
            },
            "code": "1586200",
        }

        ProcessingBatchInfo.objects.create(
            processing_id="591_rfc_model_release_1",
            batch_id="591_rfc_model_release_1",
            processor_type="sql",
            processor_logic='{"logic":{"processor":{"type":"train"}}}',
            component_type="spark_mllib",
            count_freq=0,
            delay=0,
        )

        with self.assertRaises(ModelReleaseInspectionError):
            PlatformModel.inspection_before_release(sql1)
