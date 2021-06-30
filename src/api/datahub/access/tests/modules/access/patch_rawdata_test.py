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

import httpretty
import pytest
from datahub.access.tests import utils
from datahub.access.tests.fixture import conftest
from datahub.access.tests.utils import patch

PARAMS = {
    "raw_data_name": "dataname",
    "raw_data_alias": {
        "description": u"数据标识",
        "valid": "hello",
        "invalid": ["", None, utils.CHARS_50, utils.NoField],
    },
    "bk_biz_id": 2,
    "data_scenario": "log",
    "data_source": "server",
    "sensitivity": "private",
    "data_encoding": {
        "valid": "UTF8",
        "invalid": utils.INVALID_STR_ENUM,
    },
    "bk_app_code": "bk_log_search",
    "bk_username": "admin",
    "description": "服务日志",
}


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.skip
def test_dataid_not_found():
    httpretty.reset()
    httpretty.enable()
    conftest.mock_get_meta_lang_conf()
    conftest.mock_user_perm("admin")
    conftest.mock_get_applist()

    res = patch("/v3/access/rawdata/99999999/?bk_username=admin", utils.valid_args(PARAMS))

    assert res["code"] == "1576203"
