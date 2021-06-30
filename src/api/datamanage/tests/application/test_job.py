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
import json

from datamanage.utils.api.dataquery import DataqueryApi
from datamanage.pro.datamodel.application.jobs.console import Console


@pytest.mark.django_db(transaction=True)
def test_job_gen_sql(patch_sql_parse_api):
    """
    模型应用生成sql任务测试
    """

    DataqueryApi.sql_parse = patch_sql_parse_api

    print("\n================ test_job_gen_sql ================\n")
    with open("{}/data.json".format(os.path.abspath(os.path.dirname(__file__))), "r") as load_f:
        data_scope = json.load(load_f)

    print('[dimension_table]'.center(100, '='))
    dim_job = Console(data_scope['dimension_table'])
    dim_sql = dim_job.build(engine='flink_sql')
    if isinstance(dim_sql, str):
        dim_sql = dim_sql.encode('utf-8')
    print(dim_sql)
    response = DataqueryApi.sql_parse({'sql': dim_sql})
    assert response.is_success()

    print('[fact_table]'.center(100, '='))
    fact_job = Console(data_scope['fact_table'])
    fact_sql = fact_job.build(engine='flink_sql')
    if isinstance(fact_sql, str):
        fact_sql = fact_sql.encode('utf-8')
    print(fact_sql)
    response = DataqueryApi.sql_parse({'sql': fact_sql})
    assert response.is_success()

    print('[indicator]'.center(100, '='))
    ind_job = Console(data_scope['indicator'])
    ind_sql = ind_job.build(engine='flink_sql')
    if isinstance(ind_sql, str):
        ind_sql = ind_sql.encode('utf-8')
    print(ind_sql)
    response = DataqueryApi.sql_parse({'sql': ind_sql})
    assert response.is_success()
