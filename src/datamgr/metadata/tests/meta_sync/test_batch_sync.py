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
import logging
import os
import random

import pytest
from mock import patch

from metadata.backend.dgraph.backend import (
    DgraphBackend,
    DgraphOperator,
    DgraphSessionHub,
)
from metadata.interactor.operators import (
    CommonDgraphOperator,
    CommonMySQLReplicaOperator,
)
from metadata.interactor.orchestrate import (
    BatchDGraphOrchestrator,
    BatchMySQLReplicaOrchestrator,
)
from metadata.runtime import rt_context, rt_g
from metadata.state.state import State, StateMode
from metadata.util.context import load_common_resource
from metadata_biz.interactor.operators import (
    JsonFieldMySQLReplicaOperator,
    LineageDgraphOperator,
    TagTargetDgraphOperator,
)
from metadata_contents.config.conf import default_configs_collection
from tests.conftest import (
    patch_build_operate_records,
    patch_common_operate,
    patch_get_existed_uids,
    patch_none_node,
    patch_query_referred_nodes,
)

rt_context.g.env = os.environ.get(str('RUN_MODE'), 'test')

load_common_resource(rt_g, default_configs_collection)
logging.getLogger('lazy_import').setLevel(logging.ERROR)
rt_g.language = 'en_US.UTF-8'
rt_g.md_type_registry = []
rt_g.test_gen_rdf = []
rt_g.system_state = State('/metadata', StateMode.LOCAL)

# early patch
DgraphOperator.common_operate = patch_common_operate
DgraphOperator._get_existed_uids = patch_get_existed_uids
LineageDgraphOperator.query_referred_nodes = patch_query_referred_nodes

# init env
mysql_session_url = 'mysql+pymysql://root:example@127.0.0.1/bkdata_meta?charset=utf8'
mysql_backend_session = None

dgraph_session_pool = DgraphSessionHub(["http://localhost:80"], 50, 100)
dgraph_backend = DgraphBackend(dgraph_session_pool, ver_='1.2.3')
dgraph_backend.gen_types(list(rt_context.md_types_registry.values()))
dgraph_backend_session = dgraph_backend.operate_session()
rt_g.dgraph_backend = dgraph_backend
rt_g.dgraph_session_now = dgraph_backend_session
rt_g.config_collection.normal_config.AVAILABLE_BACKENDS = {'dgraph': True}

dgraph_action_operators = [LineageDgraphOperator, TagTargetDgraphOperator, CommonDgraphOperator]
replica_mysql_action_operators = [JsonFieldMySQLReplicaOperator, CommonMySQLReplicaOperator]


@patch.object(DgraphBackend, 'none_node', patch_none_node())
def dgraph_batch_orchestrator_operator(input_data):

    # 构造batch_actions
    records = patch_build_operate_records(input_data)
    orchestrator = BatchDGraphOrchestrator(None)
    orchestrator.logger.setLevel(logging.ERROR)
    orchestrator.construct_actions(records)
    batch_actions_lst = orchestrator.batch_actions_lst
    # 比对record分类分布和batch_action分类分布的数目是否一致
    records_counter = dict()
    split_key = None
    split_idx = 0
    for record in records:
        key = (record.method, record.type_name)
        # 计算操作应该被拆分成的堆数
        if split_key is not None and split_key != key:
            split_idx += 1
        split_key = key
        static_key = (split_idx, split_key)
        # 统计各类操作总数
        if static_key not in records_counter:
            records_counter[static_key] = 0
        records_counter[static_key] += 1
    records_metric_list = [(key_tuple, records_counter[key_tuple]) for key_tuple in sorted(records_counter)]
    # 获取batch_action_lst信息，校验
    batch_collector = dict()
    batch_idx_list = list()
    batch_index = 0

    for batch_action in batch_actions_lst:
        key = (batch_index, (batch_action.method, batch_action.md_type_name.replace('Referred', '')))
        if key not in batch_collector:
            batch_collector[key] = 0
        batch_collector[key] += len(batch_action.items)
        batch_idx_list.append(key)
        batch_index += 1
    batch_action_metric_list = [(batch_idx_key, batch_collector[batch_idx_key]) for batch_idx_key in batch_idx_list]
    # 保证batch action的顺序和record表达的数量及顺序都保持一致
    assert records_metric_list == batch_action_metric_list
    assert split_idx + 1 == len(batch_actions_lst)

    # 判断batch_action每次的operator选择是否正确
    batch_counter = 0
    # print('batch cnt {}'.format(len(batch_actions_lst)))
    for batch_action in batch_actions_lst:
        for dgraph_operator_cls in dgraph_action_operators:
            operator = dgraph_operator_cls(batch_action, dgraph_backend_session, batch=True)
            if operator.in_scope and operator.batch_support:
                selected_operator = type(operator).__name__
                # 额外生成血缘
                if batch_action.md_type_name in ('DataProcessingRelation', 'ReferredDataProcessingRelation'):
                    batch_counter += 1
                    if batch_action.method in ('CREATE', 'DELETE'):
                        batch_counter += 1
                    assert selected_operator == 'LineageDgraphOperator'
                # 额外创建标签目标集合
                elif batch_action.md_type_name in ('TagTarget', 'ReferredTagTarget'):
                    # dgraph只记录code和来源code一致的标签关联
                    target_code_set = {
                        action.tag_code for action in batch_action.items if action.tag_code == action.source_tag_code
                    }
                    if target_code_set:
                        batch_counter += 1
                        if batch_action.method in ('CREATE', 'DELETE'):
                            batch_counter += 1
                    assert selected_operator == 'TagTargetDgraphOperator'
                else:
                    batch_counter += 1
                    assert selected_operator == 'CommonDgraphOperator'

                # meta类型系统转化为dgraph类型和RDF文档
                operator.execute()
                break
    # 生成rdf文档和batch操作的数目是否一致
    assert len(rt_context.test_gen_rdf) == batch_counter


def mysql_batch_orchestrator_operator(input_data):

    # 构造batch_actions
    records = patch_build_operate_records(input_data)
    orchestrator = BatchMySQLReplicaOrchestrator(None)
    orchestrator.logger.setLevel(logging.ERROR)
    orchestrator.construct_actions(records)
    batch_actions_lst = orchestrator.batch_actions_lst
    # 比对record分类分布和batch_action分类分布的数目是否一致
    records_counter = dict()
    split_key = None
    split_idx = 0
    for record in records:
        key = (record.method, 'Replica' + record.type_name)
        # 计算操作应该被拆分成的堆数
        if split_key is not None and split_key != key:
            split_idx += 1
        split_key = key
        static_key = (split_idx, split_key)
        # 统计各类操作总数
        if static_key not in records_counter:
            records_counter[static_key] = 0
        records_counter[static_key] += 1
    records_metric_list = [(key_tuple, records_counter[key_tuple]) for key_tuple in sorted(records_counter)]
    # 获取batch_action_lst信息，校验
    batch_collector = dict()
    batch_idx_list = list()
    batch_index = 0
    for batch_action in batch_actions_lst:
        key = (batch_index, (batch_action.method, batch_action.md_type_name))
        if key not in batch_collector:
            batch_collector[key] = 0
        batch_collector[key] += len(batch_action.items)
        batch_idx_list.append(key)
        batch_index += 1
    batch_action_metric_list = [(batch_idx_key, batch_collector[batch_idx_key]) for batch_idx_key in batch_idx_list]
    # 保证batch action的顺序和record表达的数量及顺序都保持一致
    assert records_metric_list == batch_action_metric_list
    assert split_idx + 1 == len(batch_actions_lst)

    # 判断batch_action每次的operator选择是否正确
    for batch_action in batch_actions_lst:
        for mysql_operator_cls in replica_mysql_action_operators:
            operator = mysql_operator_cls(batch_action, mysql_backend_session, batch=True)
            if operator.in_scope and operator.batch_support:
                selected_operator = type(operator).__name__
                if batch_action.md_type_name in ('ReplicaTdwTable', 'ReplicaTdwColumn', 'ReplicaResultTableField'):
                    assert selected_operator == 'CommonMySQLReplicaOperator'
                else:
                    print(batch_action.md_type_name)
                    assert selected_operator == 'CommonMySQLReplicaOperator'


@pytest.fixture(scope='module')
def set_up():
    with open("{}/data.json".format(os.path.abspath(os.path.dirname(__file__))), "r") as load_f:
        param_dict = json.load(load_f)
    return param_dict


# Dgraph test
@pytest.mark.usefixtures('clear_context')
def test_dgraph_batch_create(set_up):
    input_data = set_up['bulk_sync']['create']
    dgraph_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_dgraph_batch_update(set_up):
    input_data = set_up['bulk_sync']['update']
    dgraph_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_dgraph_batch_delete(set_up):
    input_data = set_up['bulk_sync']['delete']
    dgraph_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_dgraph_batch_mixed(set_up):
    mixed_data = set_up['bulk_sync']['create']
    mixed_data.extend(set_up['bulk_sync']['update'])
    mixed_data.extend(set_up['bulk_sync']['delete'])
    random.shuffle(mixed_data)
    dgraph_batch_orchestrator_operator(mixed_data)


# MySQL test
@pytest.mark.usefixtures('clear_context')
def test_mysql_batch_create(set_up):
    input_data = set_up['bulk_sync']['create']
    mysql_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_batch_update(set_up):
    input_data = set_up['bulk_sync']['update']
    mysql_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_batch_delete(set_up):
    input_data = set_up['bulk_sync']['delete']
    mysql_batch_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_batch_mixed(set_up):
    mixed_data = set_up['bulk_sync']['create']
    mixed_data.extend(set_up['bulk_sync']['update'])
    mixed_data.extend(set_up['bulk_sync']['delete'])
    random.shuffle(mixed_data)
    mysql_batch_orchestrator_operator(mixed_data)
