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

import json
import os
from collections import defaultdict

import pytest
from attr import fields_dict

from metadata.backend.dgraph.backend import Uid
from metadata.backend.dgraph.erp_v2.service import ERPQuery
from metadata.backend.erp.functions import AuthCheckParams
from metadata.backend.erp.utils import combine_filters, pretty_statement
from metadata.db_models.meta_service.log_models import DbOperateLog
from metadata.interactor.core import Operation
from metadata.runtime import rt_g
from metadata.util.orm import model_to_dict
from metadata_biz.interactor.mappings import db_model_md_name_mappings

TEST_ROOT = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope='function')
def clear_context():
    setattr(rt_g, 'test_gen_rdf', [])


def patch_build_operate_records(db_operations_lst):
    db_operate_logs = [DbOperateLog(**item) for item in db_operations_lst]
    operate_records = []
    for log in db_operate_logs:
        kwargs = {k: v for k, v in list(model_to_dict(log).items())}
        kwargs['identifier_value'] = kwargs['primary_key_value']
        if kwargs['table_name'] not in db_model_md_name_mappings:
            print('The table name {} is not watched in bridge.'.format(kwargs['table_name']))
        kwargs['type_name'] = db_model_md_name_mappings[kwargs['table_name']]
        kwargs['extra_'] = dict(mysql_table_name=kwargs['table_name'], updated_by=kwargs['updated_by'])
        o_r = Operation(**{k: v for k, v in list(kwargs.items()) if k in fields_dict(Operation)})
        operate_records.append(o_r)
    return operate_records


def patch_common_operate(obj, modify_content_lst, action, data_format, commit_now, statement='', if_cond=''):
    response = {
        'rdfs': modify_content_lst,
        'action': action,
        'format': data_format,
        'commit_now': commit_now,
        'statement': statement,
        'if_cond': if_cond,
    }
    rt_g.test_gen_rdf.append(response)


def patch_get_existed_uids(obj, query_contents):
    """
    查询已存在节点的uid值

    :param: dict query_contents 待查询的节点信息
    :return: list uid列表
    """

    uid_luncher = 0
    ret = {"data": defaultdict(list)}
    for predicate_name, identifier_values in list(query_contents.items()):
        for identifier_value in identifier_values:
            uid_luncher += 1
            ret['data'][predicate_name].append({'uid': "0x{}".format(uid_luncher), "id_v": identifier_value})
    return ret['data'] if ret['data'] else []


def patch_none_node(obj=None):
    return Uid('0x00')


def patch_query_referred_nodes(obj, md_obj):
    ret = {
        'data': {
            "dp": [{"uid": "0x{}_dp".format(md_obj.processing_id)}],
            "ds": [{"uid": "0x{}_ds".format(md_obj.data_set_id)}],
        }
    }
    dp = ret['data']['dp'][0]['uid']
    ds = ret['data']['ds'][0]['uid']
    return dp, ds


def patch_query_dgraph_via_erp(obj, erp_tree):
    statement = pretty_statement(
        obj.query_template.render(erp_tree=erp_tree, combine_filters=combine_filters, get_list_length=len),
    )
    print('[{}]statement: {}'.format(obj.__class__, json.dumps([statement])))
    return dict(statement=[statement], ret=dict(patched_dgraph_ret=None))


def patch_query_mysql_via_erp(obj, erp_tree):
    statement_list = list()
    for ee_tree in erp_tree.expressions:
        statement = pretty_statement(
            obj.query_template.render(ee_tree=ee_tree, combine_filters=combine_filters, get_list_length=len)
        )
        statement_list.append(statement)
    print('[{}]statement: {}'.format(obj.__class__, json.dumps(statement_list)))
    return dict(statement=statement_list, ret=dict(patched_mysql_ret=None))


def patch_execute_old_erp(obj, search_params, query_func=None):
    search_params = obj.compatible_with_version_one(search_params)
    for sub_item_dict in search_params:
        if '?:backend' in sub_item_dict:
            sub_item_dict.pop('?:backend')
        if '?:auth_check' in sub_item_dict:
            sub_item_dict.pop('?:auth_check')
    q = ERPQuery(service=obj)
    q.parse(search_params)
    q.compile()
    statement = q.render()
    return statement


def patch_auth_check(obj, auth_check_params_lst):
    check_param_tuple = (
        ('action_id', 'algorithm.retrieve'),
        ('metadata_type', 'algorithm'),
        ('user_id', 'admin'),
        ('variable_name', 'auth_scope_test'),
    )
    return {
        check_param_tuple: 'auth_scope_test as var(func: has(Algorithm.typed)) @filter(     '
        ' ( eq(Algorithm.project_id, [3481, 13123, 3, 13105, 13141, 12966, 292, 211])  )      OR    '
        '  ( eq(Algorithm.sensitivity, ["public"])  )       )',
    }
