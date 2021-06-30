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
import pytest

from common.api.base import DataAPI
from datamanage.tests.dataapi_settings import BKSQL_HOST_PARSE, BKSQL_PORT_PARSE


@pytest.fixture(scope='session')
def patch_sql_parse_api():
    # 自行添加django配置
    BKSQL_V1_API_ROOT = 'http://{host}:{port}/v3/bksql/api/v1'.format(
        host=BKSQL_HOST_PARSE,
        port=BKSQL_PORT_PARSE,
    )
    print(BKSQL_V1_API_ROOT)
    sql_parse = DataAPI(
        method='POST', url=BKSQL_V1_API_ROOT + '/convert/common', module='sqlparse', description='sqlparse'
    )
    return sql_parse
