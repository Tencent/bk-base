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

import cProfile
import json
import logging
import os

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
    MySQLReplicaOrchestrator,
    SingleDGraphOrchestrator,
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
def dgraph_orchestrator_operator(input_data):

    # pr = cProfile.Profile()
    # pr.enable()

    # 构造actions
    records = patch_build_operate_records(input_data)
    orchestrator = SingleDGraphOrchestrator(None)
    orchestrator.logger.setLevel(logging.ERROR)
    orchestrator.construct_actions(records)
    action_lst = orchestrator.actions_lst
    records_counter = dict()
    for record in records:
        key = (record.method, record.type_name)
        if key not in records_counter:
            records_counter[key] = 0
        records_counter[key] += 1
    records_metric_tuple = {(key, cnt) for key, cnt in list(records_counter.items())}
    collector = dict()
    for action in action_lst:
        key = (action.method, action.linked_record.type_name.replace('Referred', ''))
        if key not in collector:
            collector[key] = 0
        collector[key] += 1
    action_metric_tuple = {(key, cnt) for key, cnt in list(collector.items())}
    assert records_metric_tuple == action_metric_tuple

    # operator选择
    counter = 0
    for action in action_lst:
        for dgraph_operator_cls in dgraph_action_operators:
            operator = dgraph_operator_cls(action, dgraph_backend_session, batch=False)
            if operator.in_scope:
                selected_operator = type(operator).__name__
                if action.linked_record.type_name in ('DataProcessingRelation', 'ReferredDataProcessingRelation'):
                    counter += 1
                    if action.method in ('CREATE', 'DELETE'):
                        counter += 1
                    assert selected_operator == 'LineageDgraphOperator'
                elif action.linked_record.type_name in ('TagTarget', 'ReferredTagTarget'):
                    # dgraph只记录code和来源code一致的标签关联
                    if action.item.tag_code == action.item.source_tag_code:
                        counter += 1
                        if action.method in ('CREATE', 'DELETE'):
                            counter += 1
                    assert selected_operator == 'TagTargetDgraphOperator'
                else:
                    counter += 1
                    assert selected_operator == 'CommonDgraphOperator'

                # meta类型系统转化为dgraph类型和RDF文件
                operator.execute()
                break
    # print(json.dumps(rt_context.test_gen_rdf))
    assert len(rt_context.test_gen_rdf) == counter

    # pr.disable()
    # pr.print_stats(sort=2)


def mysql_orchestrator_operator(input_data):

    # pr = cProfile.Profile()
    # pr.enable()

    # 构造actions
    records = patch_build_operate_records(input_data)
    orchestrator = MySQLReplicaOrchestrator(None)
    orchestrator.logger.setLevel(logging.ERROR)
    orchestrator.construct_actions(records)
    action_lst = orchestrator.actions_lst
    records_counter = dict()
    for record in records:
        key = (record.method, 'Replica' + record.type_name)
        if key not in records_counter:
            records_counter[key] = 0
        records_counter[key] += 1
    records_metric_tuple = {(key, cnt) for key, cnt in list(records_counter.items())}
    collector = dict()
    for action in action_lst:
        key = (action.method, action.linked_record.type_name)
        if key not in collector:
            collector[key] = 0
        collector[key] += 1
    action_metric_tuple = {(key, cnt) for key, cnt in list(collector.items())}
    assert records_metric_tuple == action_metric_tuple

    # operator选择
    for action in action_lst:
        for mysql_operator_cls in replica_mysql_action_operators:
            operator = mysql_operator_cls(action, mysql_backend_session, batch=False)
            if operator.in_scope:
                selected_operator = type(operator).__name__
                if action.linked_record.type_name in ('ReplicaTdwTable', 'ReplicaTdwColumn', 'ReplicaResultTableField'):
                    assert selected_operator == 'JsonFieldMySQLReplicaOperator'
                else:
                    assert selected_operator == 'CommonMySQLReplicaOperator'


@pytest.fixture(scope='module')
def set_up():
    with open("{}/data.json".format(os.path.abspath(os.path.dirname(__file__))), "r") as load_f:
        param_dict = json.load(load_f)
    return param_dict


@pytest.mark.usefixtures('clear_context')
def test_dgraph_create(set_up):
    input_data = set_up['upsert_sync']['create']
    dgraph_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_dgraph_update(set_up):
    input_data = set_up['upsert_sync']['update']
    dgraph_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_dgraph_delete(set_up):
    input_data = set_up['upsert_sync']['delete']
    dgraph_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_create(set_up):
    input_data = set_up['upsert_sync']['create']
    dgraph_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_update(set_up):
    input_data = set_up['upsert_sync']['update']
    dgraph_orchestrator_operator(input_data)


@pytest.mark.usefixtures('clear_context')
def test_mysql_delete(set_up):
    input_data = set_up['upsert_sync']['delete']
    dgraph_orchestrator_operator(input_data)
