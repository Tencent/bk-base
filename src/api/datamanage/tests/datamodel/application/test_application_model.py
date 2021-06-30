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
import json

import mock
import pytest
from django.test import TestCase
from rest_framework.reverse import reverse

from common.exceptions import ValidationError

from tests.utils import UnittestClient
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.tests.datamodel.conftest import MODEL_ID, VERSION_ID, INSTANCE_ID, PROJECT_ID, MAIN_TABLE_ID


CREATE_PARAMS = {
    'model_id': MODEL_ID,
    'version_id': VERSION_ID,
    'project_id': 1,
    'bk_biz_id': 591,
    'result_table_id': '591_table2',
    'flow_id': 2,
    'flow_node_id': 1000,
    'from_result_tables': [
        {
            'result_table_id': '591_source_table1',
            'node_type': 'stream_source',
        },
        {
            'result_table_id': '591_source_dim_table1',
            'node_type': 'kv_source',
        },
    ],
    'fields': [
        {
            'field_name': 'price',
            'input_result_table_id': '591_source_table1',
            'input_field_name': 'source_field1',
            'application_clean_content': {'clean_option': 'SQL', 'clean_content': 'source_field1 * 100 as price'},
        },
        {
            'field_name': 'channel_id',
            'input_result_table_id': '591_source_table1',
            'input_field_name': 'channel_id',
            'application_clean_content': {},
            'relation': {
                'related_model_id': 2,
                'input_result_table_id': '591_source_dim_table1',
                'input_field_name': 'channel_id',
            },
        },
        {
            'field_name': 'channel_name',
            'input_result_table_id': '591_source_dim_table1',
            'input_field_name': 'channel_name',
            'application_clean_content': {},
        },
    ],
}

UPDATE_PARAMS = {
    'model_id': MODEL_ID,
    'version_id': VERSION_ID,
    'upgrade_version': True,
    'from_result_tables': [
        {
            'result_table_id': '591_source_table1',
            'node_type': 'stream_source',
        },
        {
            'result_table_id': '591_source_dim_table1',
            'node_type': 'kv_source',
        },
    ],
    'fields': [
        {
            'field_name': 'price',
            'input_result_table_id': '591_source_table1',
            'input_field_name': 'source_field1',
            'application_clean_content': {'clean_option': 'SQL', 'clean_content': 'source_field1 * 1000 as price'},
        },
        {
            'field_name': 'channel_id',
            'input_result_table_id': '591_source_table1',
            'input_field_name': 'channel_id',
            'application_clean_content': {},
            'relation': {
                'related_model_id': 2,
                'input_result_table_id': '591_source_dim_table1',
                'input_field_name': 'channel_id',
            },
        },
        {
            'field_name': 'channel_name',
            'input_result_table_id': '591_source_dim_table1',
            'input_field_name': 'channel_name',
            'application_clean_content': {},
        },
    ],
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
        pass

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_create_model_instance(self):
        create_params = copy.deepcopy(CREATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_model_instance-list')
        response = client.post(url, create_params)
        print('====>', response.message)
        assert response.is_success() is True
        assert response.data.get('data_model_sql') != ''

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_crt_model_ver_not_latest(self):
        create_params = copy.deepcopy(CREATE_PARAMS)
        create_params['version_id'] = 'version0'

        client = UnittestClient()
        url = reverse('application_model_instance-list')
        response = client.post(url, create_params)
        assert response.is_success() is False
        assert response.code == dm_pro_errors.ModelVersionNotLatestError().code

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_create_model_with_exist_rt(self):
        create_params = copy.deepcopy(CREATE_PARAMS)
        create_params['result_table_id'] = '591_main_table'

        client = UnittestClient()
        url = reverse('application_model_instance-list')
        response = client.post(url, create_params)
        assert response.is_success() is False
        assert response.code == dm_pro_errors.ModelInstanceTableBeenUsedError().code


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceValidateTest(TestCase):
    """
    数据模型应用实例校验逻辑单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_validate_serializer(self):
        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])

        update_params1 = copy.deepcopy(UPDATE_PARAMS)
        del update_params1['version_id']
        response1 = client.put(url, update_params1)
        assert response1.code == ValidationError().code

        update_params2 = copy.deepcopy(UPDATE_PARAMS)
        del update_params2['model_id']
        del update_params2['version_id']
        response2 = client.put(url, update_params2)
        assert response2.code == ValidationError().code

        update_params3 = copy.deepcopy(UPDATE_PARAMS)
        del update_params3['model_id']
        del update_params3['fields']
        response3 = client.put(url, update_params3)
        assert response3.code == ValidationError().code

        update_params4 = copy.deepcopy(UPDATE_PARAMS)
        update_params4['fields'][0]['model_id'] = MODEL_ID + 1
        response4 = client.put(url, update_params4)
        assert response4.code == dm_pro_errors.ModelNotTheSameError().code

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_validate_model_structure(self):
        client = UnittestClient()
        url = reverse('application_model_instance-list')

        create_params1 = copy.deepcopy(CREATE_PARAMS)
        create_params1['fields'][0]['field_name'] = 'other_field'
        response1 = client.post(url, create_params1)
        assert response1.code == dm_pro_errors.ModelInstanceStructureNotSameError().code

        create_params2 = copy.deepcopy(CREATE_PARAMS)
        create_params2['fields'][0]['field_name'] = 'channel_id'
        create_params2['fields'][1]['field_name'] = 'price'
        response2 = client.post(url, create_params2)
        assert response2.code == dm_pro_errors.ModelInstanceStructureNotSameError().code

        create_params3 = copy.deepcopy(CREATE_PARAMS)
        create_params3['fields'][1]['relation']['related_model_id'] = 3
        response3 = client.post(url, create_params3)
        assert response3.code == dm_pro_errors.ModelInstanceStructureNotSameError().code

        create_params4 = copy.deepcopy(CREATE_PARAMS)
        del create_params4['fields'][1]['relation']
        response4 = client.post(url, create_params4)
        assert response4.code == dm_pro_errors.ModelInstanceStructureNotSameError().code

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_validate_input_table(self):
        client = UnittestClient()
        url = reverse('application_model_instance-list')

        create_params1 = copy.deepcopy(CREATE_PARAMS)
        create_params1['fields'][1]['relation']['input_result_table_id'] = '591_source_dim_table2'
        response1 = client.post(url, create_params1)
        assert response1.code == dm_pro_errors.DimFieldMustFromDimTableError().code

        create_params2 = copy.deepcopy(CREATE_PARAMS)
        create_params2['fields'][0]['input_result_table_id'] = '591_source_dim_table1'
        response2 = client.post(url, create_params2)
        assert response2.code == dm_pro_errors.MainFieldMustFromMainTableError().code

    @pytest.mark.usefixtures('init_complete_model')
    @mock.patch('datamanage.pro.datamodel.dmm.model_instance_manager.get_sql_source_columns_mapping')
    def test_validate_instance_table_fields(self, patch_source_columns):
        client = UnittestClient()
        url = reverse('application_model_instance-list')

        patch_source_columns.return_value = {'price': ['source_field1']}
        create_params1 = copy.deepcopy(CREATE_PARAMS)
        create_params1['fields'][0]['application_clean_content']['clean_content'] = 'source_field1 * 100 as price1'
        response1 = client.post(url, create_params1)
        assert response1.code == dm_pro_errors.AliasNotMappingFieldError().code

        patch_source_columns.return_value = {'fields': {'price': ['source_field1', 'source_field2']}}
        create_params2 = copy.deepcopy(CREATE_PARAMS)
        response2 = client.post(url, create_params2)
        assert response2.code == dm_pro_errors.OtherFieldsNotPermittedError().code

        patch_source_columns.return_value = {'fields': {'price': ['source_field2']}}
        create_params3 = copy.deepcopy(CREATE_PARAMS)
        response3 = client.post(url, create_params3)
        assert response3.code == dm_pro_errors.OtherFieldsNotPermittedError().code


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceUpdateTest(TestCase):
    """
    数据模型应用实例单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_update_model_instance(self):
        update_params = copy.deepcopy(UPDATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.put(url, update_params)
        assert response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_upd_inst_without_full_params(self):
        update_params = copy.deepcopy(UPDATE_PARAMS)
        update_params['upgrade_version'] = False
        del update_params['from_result_tables']
        del update_params['fields']

        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.put(url, update_params)
        assert response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_update_model_instance_sources(self):
        update_params = {
            'model_id': MODEL_ID,
            'version_id': VERSION_ID,
            'upgrade_version': True,
            'from_result_tables': [
                {
                    'result_table_id': '591_source_dim_table1',
                    'node_type': 'stream_source',
                },
                {
                    'result_table_id': '591_source_dim_table2',
                    'node_type': 'kv_source',
                },
            ],
            'fields': [
                {
                    'field_name': 'price',
                    'input_result_table_id': '591_source_dim_table1',
                    'input_field_name': 'source_field1',
                    'application_clean_content': {
                        'clean_option': 'SQL',
                        'clean_content': 'source_field1 * 1000 as price',
                    },
                },
                {
                    'field_name': 'channel_id',
                    'input_result_table_id': '591_source_dim_table1',
                    'input_field_name': 'channel_id',
                    'application_clean_content': {},
                    'relation': {
                        'related_model_id': 2,
                        'input_result_table_id': '591_source_dim_table2',
                        'input_field_name': 'channel_id',
                    },
                },
                {
                    'field_name': 'channel_name',
                    'input_result_table_id': '591_source_dim_table1',
                    'input_field_name': 'channel_name',
                    'application_clean_content': {},
                },
            ],
        }

        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.put(url, update_params)
        assert response.is_success() is True


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceRetrieveTest(TestCase):
    """
    数据模型应用实例单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_retrieve_model_instance(self):
        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.get(url)
        assert response.is_success() is True
        assert response.data.get('model_instance_id') == INSTANCE_ID


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceDeleteTest(TestCase):
    """
    数据模型应用实例单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.run(order=0)
    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_delete_model_inst_ind_error(self):
        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.delete(url)
        assert response.is_success() is False
        assert response.code == dm_pro_errors.InstanceStillHasIndicatorsError().code

    @pytest.mark.run(order=1)
    @pytest.mark.usefixtures(
        'init_complete_model', 'init_complete_model_instance', 'patch_source_columns', 'delete_model_instance_indicator'
    )
    def test_delete_model_instance(self):
        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.delete(url)
        assert response.is_success() is True
        assert response.data.get('model_instance_id') == INSTANCE_ID


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceRollbackTest(TestCase):
    """
    模型应用实例增删改回滚
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_rollback_create(self):
        create_params = copy.deepcopy(CREATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_model_instance-list')
        response = client.post(url, create_params)
        assert response.is_success() is True

        rollback_url = reverse('application_model_instance-rollback', [response.data.get('model_instance_id')])
        rollback_response = client.post(rollback_url, {'rollback_id': response.data.get('rollback_id')})
        assert rollback_response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_source_columns')
    def test_rollback_update(self):
        update_params = copy.deepcopy(UPDATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.put(url, update_params)

        rollback_url = reverse('application_model_instance-rollback', [INSTANCE_ID])
        rollback_response = client.post(rollback_url, {'rollback_id': response.data.get('rollback_id')})
        assert rollback_response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'patch_source_columns')
    def test_rollback_delete(self):
        client = UnittestClient()
        url = reverse('application_model_instance-detail', [INSTANCE_ID])
        response = client.delete(url)

        rollback_url = reverse('application_model_instance-rollback', [INSTANCE_ID])
        rollback_response = client.post(rollback_url, {'rollback_id': response.data.get('rollback_id')})
        assert rollback_response.is_success() is True


@pytest.mark.usefixtures('django_db_setup')
class ApplicationModelInstanceOtherTest(TestCase):
    """
    模型应用实例增删改回滚
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model')
    def test_released_models_api(self):
        client = UnittestClient()
        url = reverse('application_model_instance-released-models')
        response = client.get(url, {'project_id': PROJECT_ID})
        assert response.is_success() is True
        assert len(response.data) == 2

    @pytest.mark.usefixtures('init_complete_model', 'patch_rt_fields')
    def test_fields_mappings_api(self):
        client = UnittestClient()
        url = reverse('application_model_instance-fields-mappings')
        response = client.get(
            url,
            {
                'model_id': MODEL_ID,
                'version_id': VERSION_ID,
                'from_result_tables': json.dumps(
                    [
                        {
                            'result_table_id': '591_source_table1',
                            'node_type': 'stream_source',
                        },
                        {
                            'result_table_id': '591_source_dim_table1',
                            'node_type': 'kv_source',
                        },
                    ]
                ),
            },
        )
        assert response.is_success() is True
        assert len(response.data) == 3

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_rt')
    def test_instance_by_table_api(self):
        client = UnittestClient()
        url = reverse('application_model_instance-by-table')
        response = client.get(url, {'result_table_id': MAIN_TABLE_ID})
        assert response.is_success() is True


@pytest.mark.usefixtures('django_db_setup')
class ApplicationInstanceIndicatorInitTest(TestCase):
    """
    数据模型应用实例默认指标节点单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_instance_init_nodes(self):
        client = UnittestClient()
        url = reverse('application_model_instance-init')
        response = client.get(url, {'model_instance_id': INSTANCE_ID})
        assert response.is_success() is True
