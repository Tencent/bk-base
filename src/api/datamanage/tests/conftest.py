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
from mock import Mock

from conf import dataapi_settings
from common.api.base import DataResponse

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


def pytest_configure(config):
    # 自行添加django配置
    settings.DATABASES.update(
        {
            'bkdata_basic': {
                'ENGINE': 'django.db.backends.mysql',
                'NAME': 'bkdata_test',
                'USER': getattr(dataapi_settings, 'CONFIG_DB_USER', 'root'),
                'PASSWORD': getattr(dataapi_settings, 'CONFIG_DB_PASSWORD', ''),
                'HOST': getattr(dataapi_settings, 'CONFIG_DB_HOST', '127.0.0.1'),
                'PORT': getattr(dataapi_settings, 'CONFIG_DB_PORT', 3306),
                'TEST': {
                    'NAME': 'bkdata_test',
                },
            },
        }
    )

    settings.CACHES = {
        'redis': {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://{}:{}@{}:{}/0".format(
                getattr(dataapi_settings, 'REDIS_NAME'),
                getattr(dataapi_settings, 'REDIS_PASS'),
                getattr(dataapi_settings, 'REDIS_HOST'),
                getattr(dataapi_settings, 'REDIS_PORT'),  # redis的地址
            ),
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "CONNECTION_POOL_KWARGS": {"max_connections": 10},  # 池的个数
            },
        }
    }
    settings.CACHES['default'] = settings.CACHES['redis']


# 公共fixtures
@pytest.fixture(scope='class')
def patch_auth_check():
    """
    单元测试时Patch所有权限校验
    """
    from common import auth

    def skip(*args, **kwargs):
        pass

    auth.check_perm = skip


@pytest.fixture(scope='class')
def patch_meta_sync():
    """
    单元测试时Patch所有元数据同步
    """
    from common.api import MetaApi

    MetaApi.sync_hook = Mock()

    MetaApi.sync_hook.return_value = DataResponse(
        {
            'data': {},
            'result': True,
            'code': '1500000',
            'message': 'ok',
            'errors': {},
        }
    )
