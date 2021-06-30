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

import pytest
from django.test import TestCase
from rest_framework.reverse import reverse

from tests.utils import UnittestClient
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.tests.datamodel.conftest import MODEL_ID, PROJECT_ID, BK_BIZ_ID, FLOW_ID


CREATE_PARAMS = {
    'model_id': MODEL_ID,
    'project_id': PROJECT_ID,
    'bk_biz_id': BK_BIZ_ID,
    'input': {'main_table': '591_source_table1', 'dimension_tables': ['591_source_dim_table1']},
    'main_table': {
        'table_name': 'my_test_main_table',
        'table_alias': '我的模型表',
        'storages': [
            {'cluster_type': 'hdfs', 'cluster_name': 'hdfs-test', 'expires': 7},
            {
                'cluster_type': 'ignite',
                'cluster_name': 'default',
                'expires': 7,
                'specific_params': {
                    "indexed_fields": ["province"],
                    "max_records": 1000000,
                    "storage_keys": ["province"],
                    "storage_type": "join",
                },
            },
        ],
    },
    'default_indicators_storages': [{'cluster_type': 'tspider', 'cluster_name': 'tspider-test', 'expires': 7}],
}


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceCreateTest(TestCase):
    """
    数据模型应用实例创建相关单元测试
    """

    databases = "__all__"

    def setUp(self):
        self.client = UnittestClient()
        self.url = reverse('application_model_instance-dataflow')

    @pytest.mark.usefixtures('init_complete_model', 'patch_flow_create', 'patch_flow_nodes_create', 'patch_rt_fields')
    def test_create_model_dataflow_task(self):
        create_params = copy.deepcopy(CREATE_PARAMS)
        response = self.client.post(self.url, create_params)
        assert response.is_success() is True
        assert response.data.get('flow_id') == FLOW_ID

    @pytest.mark.usefixtures('init_complete_model', 'patch_flow_create', 'patch_flow_nodes_create')
    def test_create_df_with_custom_inds(self):
        create_params = copy.deepcopy(CREATE_PARAMS)
        create_params['indicators'] = [
            {
                'indicator_name': 'max_price_1d',
                'storages': [{'cluster_type': 'tspider', 'cluster_name': 'tspider-test', 'expires': 7}],
            }
        ]

        response = self.client.post(self.url, create_params)
        assert response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'patch_flow_create', 'patch_flow_nodes_create')
    def test_create_df_without_hdfs(self):
        create_params = copy.deepcopy(CREATE_PARAMS)
        del create_params['main_table']['storages']

        response = self.client.post(self.url, create_params)
        assert response.is_success() is False
        assert response.code == dm_pro_errors.BatchChannelIsNecessaryError().code

    @pytest.mark.usefixtures('init_complete_model', 'patch_flow_create', 'patch_flow_nodes_create')
    def test_create_df_with_unknown_inds(self):
        create_params1 = copy.deepcopy(CREATE_PARAMS)
        create_params1['indicators'] = [{'indicator_name': 'unknown'}]

        response1 = self.client.post(self.url, create_params1)
        assert response1.is_success() is False
        assert response1.code == dm_pro_errors.IndicatorNotExistError().code

        create_params2 = copy.deepcopy(CREATE_PARAMS)
        del create_params2['main_table']['storages']
        create_params2['indicators'] = [{'indicator_name': 'unknown'}]

        response2 = self.client.post(self.url, create_params2)
        assert response2.is_success() is False
        assert response2.code == dm_pro_errors.IndicatorNotExistError().code
