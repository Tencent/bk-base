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
from datamanage.tests.datamodel.conftest import INSTANCE_ID, INDICATOR_RESULT_TABLE_ID, CALCULATION_ATOM_NAME


CREATE_PARAMS = {
    'bk_biz_id': 591,
    'result_table_id': '591_indicator_table2',
    'parent_result_table_id': '591_table1',
    'flow_node_id': 1,
    'calculation_atom_name': CALCULATION_ATOM_NAME,
    'aggregation_fields': ['dim1', 'dim2'],
    'filter_formula': 'dim3 = \'haha\'',
    'scheduling_type': 'stream',
    'scheduling_content': {
        'window_type': 'slide',  # 窗口类型
        'window_time': 0,  # 窗口长度
        'waiting_time': 0,  # 延迟时间
        'count_freq': 30,  # 统计频率
        'window_lateness': {
            'allowed_lateness': True,  # 是否计算延迟数据
            'lateness_time': 1,  # 延迟时间
            'lateness_count_freq': 60,  # 延迟统计频率
        },
        'session_gap': None,  # 会话窗口时间
        'expired_time': 60,  # 会话过期时间
    },
}

UPDATE_PARAMS = {
    'parent_result_table_id': '591_table1',
    'calculation_atom_name': CALCULATION_ATOM_NAME,
    'aggregation_fields': ['dim1', 'dim2'],
    'filter_formula': 'dim3 = \'haha\'',
    'scheduling_type': 'stream',
    'scheduling_content': {
        'window_type': 'slide',  # 窗口类型
        'window_time': 0,  # 窗口长度
        'waiting_time': 0,  # 延迟时间
        'count_freq': 30,  # 统计频率
        'window_lateness': {
            'allowed_lateness': True,  # 是否计算延迟数据
            'lateness_time': 1,  # 延迟时间
            'lateness_count_freq': 60,  # 延迟统计频率
        },
        'session_gap': None,  # 会话窗口时间
        'expired_time': 60,  # 会话过期时间
    },
}


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationInstanceIndicatorCreateTest(TestCase):
    """
    数据模型应用实例指标创建单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_create_instance_indicator(self):
        create_params = copy.deepcopy(CREATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_instance_indicator-list', [INSTANCE_ID])
        response = client.post(url, create_params)
        self.assertTrue(response.is_success())
        assert response.data.get('data_model_sql') != ''


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationInstanceIndicatorUpdateTest(TestCase):
    """
    数据模型应用实例指标更新单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_update_instance_indicator(self):
        update_params = copy.deepcopy(UPDATE_PARAMS)

        client = UnittestClient()
        url = reverse('application_instance_indicator-detail', [INSTANCE_ID, INDICATOR_RESULT_TABLE_ID])
        response = client.put(url, update_params)
        assert response.is_success() is True


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationInstanceIndicatorRetrieveTest(TestCase):
    """
    数据模型应用实例指标获取单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_retrieve_instance_indicator(self):
        client = UnittestClient()
        url = reverse('application_instance_indicator-detail', [INSTANCE_ID, INDICATOR_RESULT_TABLE_ID])
        response = client.get(url)
        assert response.is_success() is True
        assert response.data.get('model_instance_id') == INSTANCE_ID


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationInstanceIndicatorDeleteTest(TestCase):
    """
    数据模型应用实例指标删除单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_delete_instance_indicator(self):
        client = UnittestClient()
        url = reverse('application_instance_indicator-detail', [INSTANCE_ID, INDICATOR_RESULT_TABLE_ID])
        response = client.delete(url)
        assert response.is_success() is True
        assert response.data.get('model_instance_id') == INSTANCE_ID
        assert response.data.get('result_table_id') == INDICATOR_RESULT_TABLE_ID


@pytest.mark.usefixtures('django_db_setup')
class ApplicationInstanceIndicatorOtherTest(TestCase):
    """
    模型应用实例增删改回滚
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_ind_calatoms_api(self):
        client = UnittestClient()
        url = reverse('application_instance_indicator-calculation-atoms', [INSTANCE_ID])
        response = client.get(url)
        assert response.is_success() is True

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance', 'patch_rt_fields')
    def test_indicator_optional_fields_api(self):
        client = UnittestClient()
        url = reverse('application_instance_indicator-optional-fields', [INSTANCE_ID])
        response = client.get(url, {'parent_result_table_id': '591_source_table1', 'field_category': 'dimension'})
        assert response.is_success() is True
