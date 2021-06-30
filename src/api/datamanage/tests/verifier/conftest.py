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
import json

import pytest

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope='session')
def set_up():
    with open("{}/data.json".format(os.path.abspath(os.path.dirname(__file__))), "r") as load_f:
        param_dict = json.load(load_f)
    return param_dict


@pytest.fixture(scope='class')
def patch_sql_verify_api(set_up):
    param_dict = set_up

    class MockResponse(object):
        code = '00'
        message = ''
        result = True
        data = None

        def __init__(self, data):
            self.data = data

        def is_success(self):
            return self.result

    def skip_verify_check(*args, **kwargs):
        verify_res_data = [
            {
                "id": param_dict['bk_biz_id'],
                "name": param_dict['return_table_name'],
                "fields": param_dict['require_schema'][param_dict['return_table_name']],
            }
        ]
        return MockResponse(verify_res_data)

    return skip_verify_check
