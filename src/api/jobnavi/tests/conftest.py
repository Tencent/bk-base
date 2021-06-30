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
import os
import pytest
from django.conf import settings

from conf import dataapi_settings
from common import transaction

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


def pytest_configure(config):
    # 自行添加django配置
    settings.DATABASES.update({
        'bkdata_flow': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'bkdata_flow',
            'USER': getattr(dataapi_settings, 'CONFIG_DB_USER', 'root'),
            'PASSWORD': getattr(dataapi_settings, 'CONFIG_DB_PASSWORD', 'xxxx'),
            'HOST': getattr(dataapi_settings, 'CONFIG_DB_HOST', '127.0.0.1'),
            'PORT': getattr(dataapi_settings, 'CONFIG_DB_PORT', 3306),
        }
    })


@pytest.fixture(autouse=True, scope='session')
def patch_meta_sync():
    transaction.sync_model_data = lambda x: None


@pytest.fixture()
def jobnavi_cluster_config_fixture():
    from jobnavi.models import bkdata_flow
    from jobnavi.tests.utils.mock.cluster_config_mocker import DataflowJobNaviClusterConfigMocker
    bkdata_flow.DataflowJobNaviClusterConfig = DataflowJobNaviClusterConfigMocker
    from jobnavi.handlers import dataflow_jobnavi_cluster_config
    dataflow_jobnavi_cluster_config.save = DataflowJobNaviClusterConfigMocker.save
    dataflow_jobnavi_cluster_config.filter = DataflowJobNaviClusterConfigMocker.filter
    dataflow_jobnavi_cluster_config.update = DataflowJobNaviClusterConfigMocker.update
    dataflow_jobnavi_cluster_config.delete = DataflowJobNaviClusterConfigMocker.delete


@pytest.fixture
def jobnavi_api_fixture(jobnavi_cluster_config_fixture):
    from jobnavi.config import jobnavi_config
    from jobnavi.tests.utils.mock.jobnaviapi_mocker import get_jobnavi_config_mock
    jobnavi_config.get_jobnavi_config = get_jobnavi_config_mock
    from jobnavi.api import jobnavi_api
    from jobnavi.tests.utils.mock.jobnaviapi_mocker import JobNaviApiMocker
    jobnavi_api.JobNaviApi = JobNaviApiMocker
