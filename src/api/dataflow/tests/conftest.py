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
from common import transaction

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


def pytest_configure(config):
    pass


@pytest.fixture(autouse=True, scope="session")
def patch_meta_sync():
    transaction.sync_model_data = lambda x: None


# 权限控制mock
@pytest.fixture(autouse=True, scope="session")
def patch_perm_check():
    from common import auth

    auth.check_perm = lambda *args, **kwargs: True

    from dataflow.shared import permission

    permission.perm_check_data = lambda *args, **kwargs: True
    permission.perm_check_cluster_group = lambda *args, **kwargs: True


# 基于配置的国际化mock
@pytest.fixture(autouse=True, scope="session")
def patch_language_translate():
    from dataflow.shared import language

    language.translate_based_on_language_config = lambda *args, **kwargs: "translate-mock"
    from common import fields

    fields.bktranslates = lambda *args, **kwargs: None
