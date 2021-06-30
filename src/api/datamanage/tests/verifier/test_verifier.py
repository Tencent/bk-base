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
import json
import sqlparse

from django.core.cache import cache

from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.utils import get_sql_fields_alias_mapping


@pytest.mark.django_db(transaction=True)
def test_sql_verify(set_up, patch_sql_verify_api):
    """
    清洗加工sql生成
    :return:
    """
    print("\n================ test_sql_verify ================")
    param_dict = set_up
    SQLVerifier.requests_clients['flink_sql'] = patch_sql_verify_api
    SQLVerifier.requests_clients['spark_sql'] = patch_sql_verify_api
    verifier = SQLVerifier(
        bk_biz_id=param_dict['bk_biz_id'],
        sql=None,
        sql_parts=param_dict['sql_parts'],
        scopes=param_dict['scopes'],
        auto_format=False,
    )
    require_schema = param_dict['require_schema']
    flink_sql_exp = verifier.gen_sql_expression(require_schema, 'flink_sql')
    print('flink_sql: {}'.format(flink_sql_exp).encode('utf-8'))
    spark_sql_exp = verifier.gen_sql_expression(require_schema, 'spark_sql')
    print('spark_sql: {}'.format(spark_sql_exp).encode('utf-8'))
    matched, ret_info = verifier.check_res_schema(require_schema, ['flink_sql', 'spark_sql'])

    # print('error_trace is {}'.format(json.dumps(verifier.get_err_trace())))
    if not matched:
        if isinstance(ret_info, str):
            ret_info = ret_info.encode('utf-8')
        print(ret_info)
    assert matched


def test_check_cache_in_redis(set_up):
    """
    检查redis中设置缓存内容是否成功
    :return:
    """
    param_dict = set_up
    print("\n================ test_check_cache_in_redis ================")
    cache_key = 'data_manage:data_model:verifier:scope:{}'.format(list(param_dict['scopes'].keys())[0])
    # print('[cache_key_get]: {}'.format(cache_key))
    cache.set(cache_key, json.dumps(param_dict['scopes']))
    cache_value = cache.get(cache_key)
    if cache_value:
        print((json.loads(cache_value)))
    else:
        print('cache is empty')
    assert cache_value


def test_check_sql_parse(set_up):
    print("\n================ test_check_sql_parse ================")
    sql = set_up['sql_parse_statement']
    sql = sqlparse.format(sql, strip_comments=True, strip_whitespace=True).strip()
    if isinstance(sql, str):
        sql = sql.encode('utf-8')
    # print('[check sql]{}'.format(sql.decode('utf-8')).encode('utf-8'))
    expr_dict = dict(
        a="aaa",
        b="case c_ase when 1 then '一' when 2 then '二' else '三' end",
        c="cast(case table1.a when 1 then table2.b when 2 then '二' else c end as int) as    a",
        d="count(uid) as bb",
        e="udf_123(concat(cc, '', s)) as udf",
        f="AGE",
        g="if(a>0, 1, a+1) as count",
        h="result",
        i="UID count",
    )
    alias_expr_mapping = get_sql_fields_alias_mapping(expr_dict)
    # source_column_mapping = get_sql_source_columns_mapping(sql)
    # print('alias_expr_mapping: \n{}'.format(json.dumps(alias_expr_mapping)).encode('utf-8'))
    # print('source_column_mapping: \n{}'.format(json.dumps(source_column_mapping)))
    assert alias_expr_mapping and isinstance(alias_expr_mapping, dict)
