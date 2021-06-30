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

import functools
import os

import pytest
from common import local
from conf import dataapi_settings
from django.conf import settings
from rest_framework.reverse import reverse

from meta.exceptions import MetaDataCallError
from tests.utils import UnittestClient

# from meta.tag.views.tag import TagViewSet

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


def pytest_configure(config):
    # 自行添加django配置
    settings.DATABASES.update(
        {
            "bkdata_basic": {
                "ENGINE": "django.db.backends.mysql",
                "NAME": "bkdata_test",
                "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
                "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
                "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
                "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
                "TEST": {
                    "NAME": "bkdata_test",
                },
            },
            "bkdata_log": {
                "ENGINE": "django.db.backends.mysql",
                "NAME": "bkdata_test",
                "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
                "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
                "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
                "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
                "TEST": {
                    "NAME": "bkdata_test",
                },
            },
        }
    )

    # 对一些底层引用提前进行patch
    early_patch()
    patch_rpc_call()

    # print("TEST UTIL DB:\n{}".format(settings.DATABASES))
    # print("TEST UTIL CACHE:\n{}".format(settings.CACHES))

    # 初始化需要的数据
    # init_test_data()


def early_patch():
    def patch_user_name():
        return "test_admin"

    local.get_request_username = patch_user_name


@pytest.fixture(scope="session")
def patch_meta_sync():
    from common.transaction import meta_sync

    def skip(*args, **kwargs):
        pass

    meta_sync.sync_model_data = skip


@pytest.fixture(scope="session")
def patch_auth_check():
    from common import auth

    def skip(*args, **kwargs):
        pass

    auth.check_perm = skip


@pytest.fixture(scope="session")
def patch_auth_update():
    from common.api.modules.auth import AuthApi

    def skip(*args, **kwargs):
        pass

    AuthApi.update_project_role_users = skip


def patch_entity_complex_search(obj, statement, **kwargs):
    backend_type = kwargs.get("backend_type", "mysql")
    kwargs["backend_type"] = backend_type

    class Response(object):
        def __init__(self):
            self.result = {"data": {}} if backend_type == "dgraph" else []
            self.error = "patched_error"
            self.message = "patched_response"
            self.unique_id = None
            self.input = {"args": [statement], "kwargs": kwargs}

    response = Response()
    # print("{}\n".format(response.input))
    return response


def patch_entity_query_via_erp(obj, retrieve_args, **kwargs):
    backend_type = kwargs.get("backend_type", "dgraph")
    kwargs["backend_type"] = backend_type

    class Response(object):
        def __init__(self):
            self.result = {"data": {}} if backend_type == "dgraph" else []
            self.error = "patched_error"
            self.message = "patched_response"
            self.unique_id = None
            self.input = {"args": [retrieve_args], "kwargs": kwargs}

    response = Response()
    # print("{}\n".format(response.input))
    return response


def patch_entity_query_lineage(obj, **kwargs):
    backend_type = kwargs.get("backend_type", "dgraph")

    class Response(object):
        def __init__(self):
            self.result = {"data": {}} if backend_type == "dgraph" else []
            self.error = "patched_error"
            self.message = "patched_response"
            self.unique_id = None
            self.input = {"args": [], "kwargs": kwargs}

    response = Response()
    # print("{}\n".format(response.input))
    return response


def patch_bridge_sync(obj, db_operations_list, **kwargs):
    # print("sync db_operations_list: {}".format(db_operations_list))
    # print("sync kwargs: {}".format(kwargs))
    return True


def patch_rpc_communicate(*o_args, **o_kwargs):
    def __func(func):
        @functools.wraps(func)
        def _func(*args, **kwargs):
            data = func(*args, **kwargs)
            if data:
                return data[0]
            else:
                raise MetaDataCallError(message_kv={"inner_error": "RPC返回结果为空."})

        return _func

    return __func


def patch_get_lz_task_info(obj, lz_id):
    return "3", "D"


def patch_rpc_call():
    if getattr(dataapi_settings, "LOCAL_TEST", True):
        from meta.basic import common

        common.RPCMixIn.entity_complex_search = patch_entity_complex_search
        common.RPCMixIn.entity_query_via_erp = patch_entity_query_via_erp
        common.RPCMixIn.entity_query_lineage = patch_entity_query_lineage
        common.RPCMixIn.bridge_sync = patch_bridge_sync
        common.rpc_communicate = patch_rpc_communicate


@pytest.fixture(scope="function")
def init_tag_data():
    init_tag_conf = {"geog_area": 111111, "test": 222222, "haha": 333333}
    for tag_name, tid in init_tag_conf.items():
        tag_param = {
            "id": tid,
            "code": tag_name,
            "alias": "test",
            "parent_id": 0,
            "tag_type": "manage",
            "kpath": 1,
            "icon": "xxxxxx",
            "description": "test",
            "ret_detail": False,
        }

        client = UnittestClient()
        url = reverse("tag_tags-list")
        post_ret = client.post(url, tag_param)
        assert post_ret.is_success()


class FakeRequest(object):
    data = None

    def __init__(self, query_params=None, data=None, method="POST"):
        self.data = data
        self.query_params = query_params
        self.POST = data
        self.GET = query_params
        self.method = method

    @staticmethod
    def is_ajax():
        return False
