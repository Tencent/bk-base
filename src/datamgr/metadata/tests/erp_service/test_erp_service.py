# coding=utf-8
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

import attr
import pytest
from jinja2 import Environment, FileSystemLoader

from metadata.backend.dgraph.backend import DgraphBackend
from metadata.backend.dgraph.erp_v2.service import DgraphErpService
from metadata.backend.erp import functions
from metadata.backend.erp.service import ErpService
from metadata.backend.interface import BackendType
from metadata.backend.mysql.backend import MySQLBackend
from metadata.runtime import rt_context, rt_g
from metadata_biz.interactor.mappings import (
    md_db_model_mappings,
    original_replica_db_model_mappings,
)
from tests.conftest import (
    patch_auth_check,
    patch_execute_old_erp,
    patch_query_dgraph_via_erp,
    patch_query_mysql_via_erp,
)

normal_conf = rt_g.config_collection.normal_config

module_logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def erp_service():
    DgraphBackend.query_via_erp = patch_query_dgraph_via_erp
    d = DgraphBackend(None, None)
    d.gen_types(rt_context.md_types_registry.values())
    MySQLBackend.query_via_erp = patch_query_mysql_via_erp
    m = MySQLBackend(None)
    m.gen_types(
        rt_context.md_types_registry,
        replica_mappings=original_replica_db_model_mappings,
        mappings={k: v for k, v in md_db_model_mappings},
    )
    backend_groups = {
        BackendType.DGRAPH: d,
        BackendType.MYSQL: m,
    }

    @attr.s(frozen=True)
    class PatchAuthCheckParams(object):
        user_id = attr.ib(type=str)
        action_id = attr.ib(type=str)
        metadata_type = attr.ib(type=str)
        variable_name = attr.ib(type=str, factory=lambda: 'auth_scope_test')

    functions.AuthCheckParams = PatchAuthCheckParams
    functions.AuthCheck.check = patch_auth_check

    return ErpService(backend_groups)


@pytest.fixture(scope='module')
def erp_old_service():
    d = DgraphBackend(None, None)
    d.jinja_env = Environment(loader=FileSystemLoader(searchpath='metadata/backend/dgraph/erp_v2/'))
    d.gen_types(rt_context.md_types_registry.values())
    DgraphErpService.query = patch_execute_old_erp
    return DgraphErpService(d)


def test_multi_erp(erp_service, erp_old_service):

    backend = 'mysql'

    old_srv = erp_old_service
    new_srv = erp_service

    params = {
        'retrieve_args': [
            {
                '?:typed': 'ResultTable',
                '?:cascade': True,
                '?:backend': backend,
                '?:start_filter': 'processing_type="stream"',
                '?:filter': 'bk_biz_id=639 or generate_type="user"',
                '?:paging': {'limit': 10},
                '?:order_by': {'order': 'asc', 'by': 'created_by'},
                # "?:auth_check": {"user_id": "one", "action_id": "result_table.query_data"},
                '*': True,
                '?:count': True,
                '~DataProcessingRelation.data_set': {
                    '*': True,
                    'processing': {'processing_type': True},
                },
            },
            {
                "?:typed": "SampleSet",
                "*": True,
                '?:backend': backend,
                "?:paging": {"limit": 10, "offset": 0},
                "~SampleSetStoreDataset.sample_set": {
                    "*": True,
                    "data_set": {"_:~StorageResultTable.result_table": {"*": True, "storage_cluster": True}},
                },
            },
            {
                '?:backend': backend,
                "~ModelInstance.data_processing": {"serving_mode": True},
                "*": True,
                "?:filter": "processing_id=\"591_batch_model2099\"",
                "?:typed": "DataProcessing",
            },
            {
                "?:typed": "ModelInfo",
                '?:backend': backend,
                "?:start_filter": "model_id in [\"datamodeling_model_02ac7818\"]",
                "sample_set": {"sample_latest_time": True},
                "~ModelExperiment.model": {
                    "experiment_id": True,
                    "sample_latest_time": True,
                    "?:filter": "experiment_id >= 3",
                },
            },
            {
                "?:cascade": True,
                '?:backend': backend,
                "?:typed": "Algorithm",
                "?:auth_check": {"user_id": "admin", "action_id": "algorithm.retrieve"},
                "?:paging": {"limit": 10000, "offset": 0},
                "~AlgorithmVersion.algorithm": {"version": True, "config": True},
                "algorithm_name": True,
                "algorithm_alias": True,
                "algorithm_original_name": True,
                "description": True,
                "algorithm_type": True,
                "category_type": True,
                "generate_type": True,
                "sensitivity": True,
                "project_id": True,
                "run_env": True,
                "scene_name": True,
                "framework": True,
                "updated_by": True,
                "updated_at": True,
                "created_by": True,
                "created_at": True,
                "?:filter": "protocol_version in [\"1.1\"] and category_type=\"optimization\" and "
                "scene_name=\"timeseries_anomaly_detect\"",
            },
            {
                '?:typed': 'SampleSet',
                '?:auth_check': {'user_id': 'admin', 'action_id': 'sample_set.retrieve'},
                '?:paging': {'limit': 20, 'offset': 0},
                '?:order_by': {'order': 'desc', 'by': 'created_at'},
                '~TimeseriesSampleConfig.sample_set': {'ts_freq': True, 'ts_depend': True},
                '~ModelInfo.sample_set': {'model_id': True, 'model_name': True},
                'scene_info': {'scene_alias': True},
                'project': {'project_name': True},
                'id': True,
                'sample_set_name': True,
                'sample_set_alias': True,
                'scene_name': True,
                'sample_type': True,
                'sensitivity': True,
                'project_id': True,
                'description': True,
                'active': True,
                'properties': True,
                'sample_size': True,
                'sample_latest_time': True,
                'sample_feedback': True,
                'modeling_type': True,
                'sample_feedback_config': True,
                'commit_status': True,
                'updated_at': True,
                'updated_by': True,
                'created_at': True,
                'created_by': True,
                '~SampleResultTable.sample_set': {
                    'status': True,
                    'result_table_id': True,
                    'result_table': {'~Tag.targets': {'code': True, 'alias': True, 'description': True}},
                },
                '~SampleFeatures.sample_set': {
                    'status': True,
                    'field_name': True,
                    'attr_type': True,
                    'field_type': True,
                    'origin': True,
                    'generate_type': True,
                },
                '?:filter': "project_id=3481 and sensitivity='private' and scene_name='timeseries_anomaly_detect' "
                "and sample_set_alias*='\u5f02\u5e38\u68c0\u6d4b' or sensitivity='public' "
                "and scene_name='timeseries_anomaly_detect' "
                "and sample_set_alias*='\u5f02\u5e38\u68c0\u6d4b' or project_id=3481 "
                "and sensitivity='private' and scene_name='timeseries_anomaly_detect' "
                "and description*='\u5f02\u5e38\u68c0\u6d4b' or sensitivity='public' "
                "and scene_name='timeseries_anomaly_detect' and description*='\u5f02\u5e38\u68c0\u6d4b'",
            },
        ],
        'version': 2,
    }
    # new
    print("\n[new erp statements]:\n")
    new_srv.query(params['retrieve_args'])

    # old
    ret = old_srv.query(params['retrieve_args'], None)
    print("\n[old erp statements]:\n" + json.dumps(ret))


# def test_entity_query_lineage_with_erp(rpc_client):
#     # params = {
#     #     'entity_type': 'RawData',
#     #     'attr_value': '1',
#     #     'backend_type': 'dgraph',
#     #     'direction': 'OUTPUT',type_system_search_target
#     #     'depth': 10
#     # }
#     params = {
#         'entity_type': 'ResultTable',
#         'attr_value': '591_f866_algor_08',
#         'backend_type': 'dgraph',
#         'direction': 'OUTPUT',
#         'depth': "2",
#         'extra_retrieve': {'DataProcessing': {'processing_id': True},
#                            'ResultTable': True}
#     }
#     response = rpc_client.call('entity_query_lineage', [], params)
#     module_logger.info('[test_entity_query_lineage] dgraph response is {}'.format(response))
#
#
# def test_erp(rpc_client):
#     params = {
#         'retrieve_args': {
#             "ResultTable": {"expression": {
#                 "*": True,
#                 "~DataProcessingRelation.data_set": {
#                     "processing_id": True,
#                     "data_directing": True,
#                     "processing": {
#                         "processing_type": True
#                     },
#                 },
#             }, "starts": ["591_durant1115"]}
#         }
#     }
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2(rpc_client):
#     params = {'retrieve_args': [{
#         '?:typed': 'ResultTable',
#         '?:paging': {'limit': 10},
#         '?:order_by': {'order': 'asc', 'by': 'created_by'},
#         "?:start_filter": "processing_type=\"stream\"",
#         "?:filter": "bk_biz_id=639 or generate_type=\"user\"",
#         "?:auth_check": {"user_id": "one", "action_id": "result_table.query_data"},
#         "*": True,
#         "~DataProcessingRelation.data_set": {
#             "*": True,
#             "processing": {
#                 "processing_type": True
#             },
#         }}], 'version': 2}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_2(rpc_client):
#     params = {'retrieve_args': [{
#         '?:typed': 'ResultTable',
#         '?:paging': {'limit': 20},
#         "*": True,
#         "?:count": True
#     }], 'version': 2}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_3(rpc_client):
#     params = {
#         "retrieve_args": [
#             {
#                 "?:start_filter": "code=\"inland\"",
#                 "?:count": True,
#                 "?:typed": "Tag",
#                 "?:paging": {"limit": 10},
#                 "?:cascade": True,
#                 "targets": {
#                     "_:ResultTable.result_table_id": True,
#                     "_:ResultTable.project": True,
#                     "_:~ResultTableField.result_table": {'field_name':True}
#                 }
#             }], 'version': 2}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_filter(rpc_client):
#     params = {'retrieve_args': [{
#         '?:paging': {'limit': 10},
#         '?:order_by': {'order': 'asc', 'by': 'result_table_id'},
#         '?:count': True,
#         '?:typed': 'ResultTable',
#         '?:start_filter': "result_table_id in [\"591_durant1115\"]",
#         "*": True,
#         "~DataProcessingRelation.data_set": {
#             '?:paging': {'limit': 10},
#             '?:order_by': {'order': 'asc', 'by': 'id'},
#             "*": True,
#             '?:count': True,
#             "processing": {
#                 "processing_type": True
#             },
#         }}], 'version': 2, 'backend_type': 'dgraph_backup'}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_common(rpc_client):
#     params = {'retrieve_args': [{
#         '?:paging': {'limit': 10},
#         '?:order_by': {'order': 'asc', 'by': 'result_table_id'},
#         '?:count': True,
#         '?:typed': 'ResultTable',
#         '?:start_filter': "result_table_id in [\"591_durant1115\"]",
#         "*": True,
#         "~StorageResultTable.result_table": {"physical_table_name": True, "active": True,
#                                              "storage_cluster": {"cluster_type": True},
#                                              "storage_channel": {"cluster_type": True}},
#         "~DataProcessingRelation.data_set": {
#             '?:paging': {'limit': 10},
#             '?:order_by': {'order': 'asc', 'by': 'id'},
#             "*": True,
#             '?:count': True,
#             "processing": {
#                 "processing_type": True
#             },
#         }}], 'version': 2}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_compatibility(rpc_client):
#     params = {'retrieve_args': {'ResultTable': {"expression": {
#         '?:paging': {'limit': 10},
#         '?:order_by': {'order': 'asc', 'by': 'result_table_id'},
#         '?:count': True,
#         '?:typed': '',
#         '?:start_filter': "result_table_id in [\"591_durant1115\"]",
#         "*": True,
#         "~StorageResultTable.result_table": {"physical_table_name": True, "active": True,
#                                              "storage_cluster": {"cluster_type": True},
#                                              "storage_channel": {"cluster_type": True}},
#         "~DataProcessingRelation.data_set": {
#             '?:paging': {'limit': 10},
#             '?:order_by': {'order': 'asc', 'by': 'id'},
#             "*": True,
#             '?:count': True,
#             "processing": {
#                 "processing_type": True
#             },
#         }}, "starts": ["pp"]}, }, 'version': 2}
#     response = rpc_client.call('entity_query_via_erp', [], params)
#     module_logger.info('[entity_query_via_erp] response is {}'.format(response))
#
#
# def test_erp_v2_parse_performance(rpc_client, erp_service, benchmark):
#     info = [{
#         '?:paging': {'limit': 10},
#         '?:order_by': {'order': 'asc', 'by': 'result_table_id'},
#         '?:count': True,
#         '?:typed': 'ResultTable',
#         '?:start_filter': "result_table_id in [\"591_durant1115\"]",
#         "*": True,
#         "~StorageResultTable.result_table": {"physical_table_name": True, "active": True,
#                                              "storage_cluster": {"cluster_type": True},
#                                              "storage_channel": {"cluster_type": True}},
#         "~DataProcessingRelation.data_set": {
#             '?:paging': {'limit': 10},
#             '?:order_by': {'order': 'asc', 'by': 'id'},
#             "*": True,
#             '?:count': True,
#             "processing": {
#                 "processing_type": True
#             },
#         }}]
#
#     def parse_and_compile():
#         q = ErpQuery(service=erp_service)
#         q.parse(info)
#         q.compile()
#         statement = q.render()
#
#     benchmark.pedantic(parse_and_compile, rounds=10000)
#
#
# def test_complex_search_with_erp(rpc_client):
#     params = {
#         'statement': '''{
# rt_content(func:has(ResultTable.result_table_id),first:100)
#       {all as uid}
# ResultTable(func:uid(all)) {ResultTable.result_table_id}
# }''',
#         'retrieve_args': {
#             "ResultTable": {"expression": {
#                 "*": True,
#                 "~DataProcessingRelation.data_set": {
#                     "processing_id": True,
#                     "data_directing": True,
#                     "processing": {
#                         "processing_type": True
#                     },
#                 },
#             }}
#         },
#         'start_rules': {'ResultTable': 'ResultTable.result_table_id'}
#     }
#     response = rpc_client.call('entity_complex_search_with_erp', [], params)
#     module_logger.info('[entity_complex_search_with_erp-dgraph] response is {}'.format(response))
#     params = {
#         'statement': '''select * from result_table limit 100''',
#         'retrieve_args': {
#             "ResultTable": {"expression": {
#                 "*": True,
#                 "~DataProcessingRelation.data_set": {
#                     "processing_id": True,
#                     "data_directing": True,
#                     "processing": {
#                         "processing_type": True
#                     },
#                 },
#             }}
#         },
#         'start_rules': {'ResultTable': 'result_table_id'},
#         'backend_type': 'mysql'
#     }
#     response = rpc_client.call('entity_complex_search_with_erp', [], params)
#     module_logger.info('[entity_complex_search_with_erp-mysql] response is {}'.format(response))
#
#
# def test_analyse_tag_propagate(rpc_client):
#     ret = rpc_client.call('analyse_tag_propagate', [], {'result_table_id': '581_ja_os_online'})
#     module_logger.info('Ret is {}'.format(ret))
#     return ret
#
#
# def test_result_table_similarity(rpc_client):
#     ret = rpc_client.call('analyse_result_table_similarity', [],
#                          {'result_table_ids': ['591_durant1115', '591_durant1115_cal'], 'analyse_mode': 'up_to_date'})
#     module_logger.info('Ret is {}'.format(ret))
#     ret = rpc_client.call('analyse_result_table_similarity', [],
#                           {'result_table_ids': ['591_durant1115', ],
#                            'reference_result_table_ids': ['591_durant1115_cal', ], 'compare_mode': 'two_way',
#                            'analyse_mode': 'up_to_date'})
#     module_logger.info('Ret is {}'.format(ret))
#     ret = rpc_client.call('analyse_result_table_similarity', [],
#                           {'result_table_ids': ['100115_test_ungzip', '100120_b_users_active']})
#     module_logger.info('Ret is {}'.format(ret))
#     ret = rpc_client.call('analyse_result_table_similarity', [],
#                           {'result_table_ids': [u'0100133_docker_inode', ], 'reference_max_n': 10,
#                            'reference_result_table_ids': ['*', ], 'compare_mode': 'two_way', })
#     module_logger.info('Ret is {}'.format(ret))
#
#     return ret
#
#
# def test_result_table_lineage_similarity(rpc_client):
#     ret = rpc_client.call('analyse_result_table_lineage_similarity', [],
#                           {'result_table_ids': ['591_durant1115', ],
#                            'reference_result_table_ids': ['591_durant1115', ], })
#     module_logger.info('Ret is {}'.format(ret))
#
#
# def test_suggest_fields(rpc_client):
#     ret = rpc_client.call('analyse_suggest_fields', [],
#                           {'fields': [['path', 'report_time', 'log', 'ip']]})
#     module_logger.info('Ret is {}'.format(ret))
#     return ret
#
#
# def test_suggest_field_alias(rpc_client):
#     ret = rpc_client.call('analyse_suggest_field_alias', [],
#                           {'fields': [['path', 'report_time', 'log', 'ip']]})
#     module_logger.info('Ret is {}'.format(ret))
#     return ret
