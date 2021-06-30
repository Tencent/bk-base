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
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.tasks import ReleaseModelTask
from dataflow.stream.api.api_helper import MetaApiHelper


class TestReleaseModelTask(APITestCase):
    databases = "__all__"

    def setUp(self):
        MLSqlExecuteLog.objects.create(id=1, context="", created_by="admin")

    def test_set_processing_id_mlsql_link(self):
        ProcessingBatchInfo.objects.create(
            processing_id="591_rfc_result_release_1",
            batch_id="591_rfc_result_release_1",
            processor_type="sql",
            processor_logic='{"sql": "create table 591_rfc_result_release_1 as '
            "run 591_rfc_model_release_1(features_col=<double_1,double_2>) as category1"
            ",double_1,double_2 "
            'from 591_string_indexer_result_release_1;"}',
            component_type="spark_mllib",
            count_freq=0,
            delay=0,
        )
        ProcessingBatchInfo.objects.create(
            processing_id="591_string_indexer_result_release_1",
            batch_id="591_string_indexer_result_release_1",
            processor_type="sql",
            processor_logic='{"sql": "create table 591_string_indexer_result_release_1 as '
            "run 591_string_indexer_release_1(input_col=char_1,handle_invalid='keep') as indexed"
            ',double_1,double_2 from 591_source_data;"}',
            component_type="spark_mllib",
            count_freq=0,
            delay=0,
        )
        data_processing_values = {
            "591_rfc_result_release_1": {"inputs": [{"data_set_id": "591_string_indexer_result_release_1"}]},
            "591_string_indexer_result_release_1": {"inputs": [{"data_set_id": "591_source_data"}]},
            "591_source_data": {"inputs": [{"data_set_id": "591_clean_data"}]},
        }
        MetaApiHelper.get_data_processing = mock.Mock(side_effect=lambda p_id: data_processing_values[p_id])

        task = ReleaseModelTask(1)
        actual_mlsql_link = []
        actual_processing_ids = []
        task._ReleaseModelTask__set_processing_id_mlsql_link(
            "591_rfc_result_release_1",
            [
                "591_rfc_result_release_1",
                "591_rfc_model_release_1",
                "591_string_indexer_result_release_1",
                "591_string_indexer_release_1",
            ],
            actual_mlsql_link,
            actual_processing_ids,
        )
        expected_mlsql_link = [
            "create table 591_string_indexer_result_release_1 as "
            "run 591_string_indexer_release_1(input_col=char_1,handle_invalid='keep') as indexed"
            ",double_1,double_2 from 591_source_data;",
            "create table 591_rfc_result_release_1 as "
            "run 591_rfc_model_release_1(features_col=<double_1,double_2>) as category1"
            ",double_1,double_2 from 591_string_indexer_result_release_1;",
        ]
        expected_processing_ids = [
            "591_string_indexer_result_release_1",
            "591_rfc_result_release_1",
        ]
        assert len(actual_mlsql_link) == len(expected_mlsql_link)
        assert all([a == b for a, b in zip(actual_mlsql_link, expected_mlsql_link)])
        assert len(expected_processing_ids) == len(actual_processing_ids)
        assert all([a == b for a, b in zip(actual_processing_ids, expected_processing_ids)])
