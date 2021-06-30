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
import sys
import time
from codecs import open
from copy import deepcopy
from datetime import datetime

import arrow
import click
import dateutil
import jsonext
from attr import fields_dict
from gevent.pool import Pool
from gipc import start_process
from tinyrpc import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

from metadata.backend.dgraph.backend import DgraphBackend
from metadata.backend.mysql.backend import MySQLBackend, MySQLSessionHub
from metadata.db_models.meta_service.log_models import DbOperateLog
from metadata.db_models.meta_service.replica_models import ReplicaBase, Tag
from metadata.exc import InteractorExecuteError
from metadata.interactor.core import Operation
from metadata.interactor.orchestrate import SingleDGraphOrchestrator
from metadata.runtime import rt_context, rt_g
from metadata.support import quick_support
from metadata.util.orm import get_pk, model_to_dict
from metadata.util.os import dir_create, get_host_ip
from metadata_biz.interactor.mappings import db_model_md_name_mappings
from metadata_biz.type_system.converters import tag_related_md_types

sync_content_format = sync_info = {
    'method': 'CREATE',
    'db_name': 'bkdata_meta',
    'table_name': '',
    'primary_key_value': '',
    'changed_data': '',
    'change_time': '',
}

module_logger = logging.getLogger(__name__)

statement = """
{
  result_table(func:has(ResultTable.typed))
  {v:ResultTable.result_table_id
   uid}
   data_processing(func:has(DataProcessing.typed))
  {v:DataProcessing.processing_id
   uid}
  raw_data(func:has(AccessRawData.typed))
  {v:AccessRawData.id
   uid}
  tag(func:has(Tag.typed))
  {v:Tag.code
   uid}
  rtft(func:has(FieldTypeConfig.typed))
  {v:FieldTypeConfig.field_type
   uid}
}
"""

type_category_type_map = {}
for k, v in tag_related_md_types.items():
    if isinstance(v['target_type'], set):
        for i in v['target_type']:
            type_category_type_map[i] = k
    else:
        type_category_type_map[v['target_type']] = k


@click.command()
def get_replica_models_to_sync_content():
    db_conf = rt_context.config_collection.db_config
    task_conf = rt_context.config_collection.task_config
    models_to_get = task_conf.TABLE_NAMES_TO_GET
    tdw_table_ids = []
    mb = MySQLBackend(
        session_hub=MySQLSessionHub(
            getattr(
                db_conf,
                getattr(
                    task_conf,
                    'ORGI_DB_URL_KEY',
                    'BKDATA_META_DB_URL',
                ),
            ),
            pool_size=db_conf.DB_POOL_SIZE,
            pool_maxsize=db_conf.DB_POOL_SIZE + 10,
            db_conf=db_conf,
        )
    )
    dir_create('replica')
    if models_to_get:
        models = {
            k: v
            for k, v in ReplicaBase._decl_class_registry.items()
            if (not k.startswith('_')) and v.__tablename__ in models_to_get
        }
    else:
        models = {k: v for k, v in ReplicaBase._decl_class_registry.items() if not k.startswith('_')}
    for model in models.values():
        model_sync_contents = []
        with mb.operate_session().session as se:
            if model.__tablename__ == 'tdw_table':
                ret = se.query(model).filter(model.result_table_id is not None).all()
                tdw_table_ids = [item.table_id for item in ret]
            elif model.__tablename__ == 'tdw_column':
                ret = se.query(model).filter(model.tdw_table_id.in_(tdw_table_ids)).all()
            else:
                ret = se.query(model).yield_per(1000).all()
            pk_name = get_pk(model)
            for per_ret in ret:
                sync_content = deepcopy(sync_content_format)
                change_content = model_to_dict(per_ret)
                if model.__tablename__ == 'tdw_table':
                    change_content['associated_lz_id'] = json.loads(change_content['associated_lz_id'])
                elif model.__tablename__ == 'result_table_field':
                    try:
                        change_content['roles'] = json.loads(change_content['roles'])
                    except Exception:
                        pass
                sync_content['changed_data'] = jsonext.dumps(change_content)
                sync_content['change_time'] = datetime.now()
                sync_content['primary_key_value'] = change_content[pk_name]
                sync_content['table_name'] = model.__tablename__
                model_sync_contents.append(sync_content)
        if model_sync_contents:
            with open('replica/replica_db_{}.txt'.format(model.__tablename__), 'w', encoding='utf8') as fw:
                contents = jsonext.dumps(model_sync_contents)
                fw.write(contents)
    with mb.operate_session().session as se:
        all_tag = se.query(Tag).all()
        tag_id_code_map = {item.code: item.id for item in all_tag}
        with open(os.path.join('replica/tag_id_code_map.json'), 'w') as fw:
            json.dump(tag_id_code_map, fw)


@click.command()
def manual_meta_sync():
    normal_conf = rt_context.config_collection.normal_config
    task_conf = rt_context.config_collection.task_config
    rpc_client = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport('http://{}:{}/jsonrpc/2.0/'.format(get_host_ip(), normal_conf.ACCESS_RPC_SERVER_PORT)),
    )
    pool = Pool(1)

    try:
        with open('synced', 'r') as fr:
            synced_table_names = json.load(fr)
    except Exception:
        synced_table_names = []
    for table_name in task_conf.TABLE_NAMES_TO_META_SYNC:
        if not os.path.isfile('replica/replica_db_{}.txt'.format(table_name)):
            continue
        if table_name in synced_table_names:
            continue
        module_logger.info('Syncing {}'.format(table_name))
        with open(os.path.join('replica/replica_db_{}.txt'.format(table_name))) as fr:
            try:
                sync_contents = json.load(fr)
                for sync_content in sync_contents:
                    sync_content['change_time'] = arrow.get(sync_content['change_time']).datetime
                    record_id_lst = db_log_prepare([sync_content])
                    pool.spawn(_bridge_sync, rpc_client, record_id_lst, sync_content)
                pool.join()
            except Exception:
                module_logger.exception('Fail to sync table {}'.format(table_name))
        synced_table_names.append(table_name)
        with open('synced', 'w') as fw:
            json.dump(synced_table_names, fw)
        module_logger.info('Synced {}'.format(table_name))


@click.command()
def gen_common_rdfs():
    rt_g.dgraph_backend = DgraphBackend(
        rt_context.m_resource.dgraph_session,
    )
    rt_g.dgraph_backend.gen_types(list(rt_context.md_types_registry.values()))
    task_conf = rt_context.config_collection.task_config
    rt_g.skip_auto_gen_reffed_attr = True

    p_list = []
    for table_name in task_conf.TO_COMMON_RDFS_TABLE_NAMES:
        p = start_process(per_gen_common_rdf, (table_name,), daemon=True)
        p_list.append(p)
    for p in p_list:
        p.join()
    with open(
        'replica/rdfs_common_extra.txt',
        'w',
        encoding='utf8',
    ) as fw:
        fw.writelines(['_:Internal.as_none <Internal.role> "as_none" .\n'])


operate_record_fields = fields_dict(Operation)


def build_operate_records(db_operations_lst):
    operate_records = []

    db_operate_logs = [DbOperateLog(**item) for item in db_operations_lst]
    for log in db_operate_logs:
        kwargs = {k: v for k, v in model_to_dict(log).items()}
        kwargs['identifier_value'] = kwargs['primary_key_value']
        if kwargs['table_name'] not in db_model_md_name_mappings:
            raise InteractorExecuteError('The table name {} is not watched in bridge.'.format(kwargs['table_name']))
        kwargs['type_name'] = db_model_md_name_mappings[kwargs['table_name']]
        o_r = Operation(**{k: v for k, v in kwargs.items() if k in operate_record_fields})
        operate_records.append(o_r)
    return db_operate_logs, operate_records


def per_gen_common_rdf(table_name):
    se = None
    if not os.path.isfile('replica/replica_db_{}.txt'.format(table_name)):
        return
    module_logger.warning('Syncing {}'.format(table_name))
    with open(os.path.join('replica/replica_db_{}.txt'.format(table_name))) as fr, open(
        os.path.join('replica/rdfs_common_{}.txt'.format(table_name)),
        'w',
        encoding='utf8',
    ) as fw, open(
        os.path.join('replica/rdfs_common_{}.error.txt'.format(table_name)),
        'w',
        encoding='utf8',
    ) as fw_2:
        rdfs = []
        for line in fr:
            sync_contents = json.loads(line.strip())
            for sync_content in sync_contents:
                try:
                    sync_content['change_time'] = arrow.get(sync_content['change_time']).datetime
                    db = SingleDGraphOrchestrator(se)
                    db.construct_actions(build_operate_records([sync_content])[1])
                    rdfs.extend(db.gen_create_action_rdfs())
                except Exception:
                    module_logger.exception('Fail to gen rdf for sync content {}'.format(sync_content))
                    fw_2.write(repr(sys.exc_info()[1]) + '\t' + jsonext.dumps(sync_content) + '\n')
            for line in rdfs:
                fw.write(line + '\n')
            rdfs = []


@click.command()
def gen_custom_rdfs():
    task_conf = rt_context.config_collection.task_config
    p_list = []
    for table_name in task_conf.TO_CUSTOM_RDFS_TABLE_NAMES:
        p = start_process(gen_per_custom_rdfs, (table_name,), daemon=True)
        p_list.append(p)
    for p in p_list:
        p.join()


def gen_per_custom_rdfs(table_name):
    normal_conf = rt_context.config_collection.normal_config
    module_logger.warning('Syncing {}'.format(table_name))
    with open(os.path.join('replica/tag_id_code_map.json')) as fr:
        tag_id_code_map = json.load(fr)
    tag_id_code_map.update({0: 'blank'})
    if not os.path.isfile('replica/replica_db_{}.txt'.format(table_name)):
        return
    module_logger.info('Gen {}'.format(table_name))
    with open(os.path.join('replica/replica_db_{}.txt'.format(table_name))) as fr, open(
        os.path.join('replica/rdfs_custom_{}.txt'.format(table_name)),
        'w',
        encoding='utf8',
    ) as fw, open(
        os.path.join('replica/rdfs_custom_{}.error.txt'.format(table_name)),
        'w',
        encoding='utf8',
    ) as fw_2:
        if table_name == 'tag':
            fw.writelines(['_:Tag.blank <Tag.code> "blank" .\n', '_:Tag.blank <Tag.id> "0" .\n'])
        sync_contents = json.load(fr)
        for sync_content in sync_contents:
            try:
                sync_content['changed_data'] = json.loads(sync_content['changed_data'])
                if table_name == 'data_processing_relation':
                    if sync_content['changed_data']['data_directing'] == 'output':
                        fw.write(
                            ' '.join(
                                [
                                    '_:DataProcessing.{}'.format(sync_content['changed_data']['processing_id']),
                                    '<lineage.descend>',
                                    '_:{}.{}'.format(
                                        type_category_type_map[sync_content['changed_data']['data_set_type']],
                                        sync_content['changed_data']['data_set_id'],
                                    ),
                                    '.',
                                ]
                            )
                            + '\n'
                        )
                    else:
                        fw.write(
                            ' '.join(
                                [
                                    '_:{}.{}'.format(
                                        type_category_type_map[sync_content['changed_data']['data_set_type']],
                                        sync_content['changed_data']['data_set_id'],
                                    ),
                                    '<lineage.descend>',
                                    '_:DataProcessing.{}'.format(sync_content['changed_data']['processing_id']),
                                    '.',
                                ]
                            )
                            + '\n'
                        )
                elif table_name == 'tag_target':
                    target_actual_type = type_category_type_map.get(sync_content['changed_data']['target_type'])
                    if (
                        target_actual_type
                        and sync_content['changed_data']['tag_code'] == sync_content['changed_data']['source_tag_code']
                    ):
                        fw.write(
                            ' '.join(
                                [
                                    '_:Tag.{}'.format(sync_content['changed_data']['tag_code']),
                                    '<Tag.targets>',
                                    '_:{}.{}'.format(target_actual_type, sync_content['changed_data']['target_id']),
                                    '.',
                                ]
                            )
                            + '\n'
                        )
                elif table_name == 'result_table_field':
                    all_rdfs = []
                    for k, v in list(sync_content['changed_data'].items()):
                        v = '' if v is None else v
                        if k in ('created_at', 'updated_at'):
                            v = (
                                arrow.get(v).replace(tzinfo=dateutil.tz.gettz(normal_conf.TIME_ZONE)).isoformat()
                                if v
                                else arrow.get(0).isoformat()
                            )
                            v = json.dumps(str(v))
                            all_rdfs.append(
                                ' '.join(
                                    [
                                        '_:ResultTableField.{}'.format(sync_content['primary_key_value']),
                                        '<{}>'.format(k),
                                        '''{}'''.format(v),
                                        '.',
                                    ]
                                )
                                + '\n'
                            )
                        v = json.dumps(str(v))
                        all_rdfs.append(
                            ' '.join(
                                [
                                    '_:ResultTableField.{}'.format(sync_content['primary_key_value']),
                                    '<ResultTableField.{}>'.format(k),
                                    '''{}'''.format(v),
                                    '.',
                                ]
                            )
                            + '\n'
                        )
                    all_rdfs.append(
                        ' '.join(
                            [
                                '_:ResultTableField.{}'.format(sync_content['primary_key_value']),
                                '<ResultTableField.field_type_obj>',
                                '_:FieldTypeConfig.{}'.format(sync_content['changed_data']['field_type']),
                                '.',
                            ]
                        )
                        + '\n'
                    )
                    all_rdfs.append(
                        ' '.join(
                            [
                                '_:ResultTableField.{}'.format(sync_content['primary_key_value']),
                                '<ResultTableField.result_table>',
                                '_:ResultTable.{}'.format(sync_content['changed_data']['result_table_id']),
                                '.',
                            ]
                        )
                        + '\n'
                    )
                    fw.writelines(all_rdfs)
                elif table_name == 'tag':
                    all_rdfs = []
                    for k, v in list(sync_content['changed_data'].items()):
                        v = '' if v is None else v
                        if k in ('created_at', 'updated_at'):
                            v = (
                                arrow.get(v).replace(tzinfo=dateutil.tz.gettz(normal_conf.TIME_ZONE)).isoformat()
                                if v
                                else arrow.get(0).isoformat()
                            )
                            v = json.dumps(str(v))
                            all_rdfs.append(
                                ' '.join(
                                    [
                                        '_:ResultTableField.{}'.format(sync_content['primary_key_value']),
                                        '<{}>'.format(k),
                                        '''{}'''.format(v),
                                        '.',
                                    ]
                                )
                                + '\n'
                            )
                        v = json.dumps(str(v))
                        all_rdfs.append(
                            ' '.join(
                                [
                                    '_:Tag.{}'.format(tag_id_code_map[int(sync_content['primary_key_value'])]),
                                    '<Tag.{}>'.format(k),
                                    '''{}'''.format(v),
                                    '.',
                                ]
                            )
                            + '\n'
                        )
                    all_rdfs.append(
                        ' '.join(
                            [
                                '_:Tag.{}'.format(tag_id_code_map[int(sync_content['primary_key_value'])]),
                                '<Tag.parent_tag>',
                                '_:Tag.{}'.format(tag_id_code_map[sync_content['changed_data']['parent_id']]),
                                '.',
                            ]
                        )
                        + '\n'
                    )
                    fw.writelines(all_rdfs)
            except Exception:
                module_logger.exception('Fail to gen {}'.format(sync_content))
                fw_2.write(repr(sys.exc_info()[1]) + '\t' + jsonext.dumps(sync_content) + '\n')


@click.command()
def gen_dgraph_schema():
    db = DgraphBackend(rt_context.m_resource.dgraph_session)
    db.gen_types(list(rt_context.md_types_registry.values()))
    content = db.declare_types(dry=True)
    print(content, file=sys.stdout)


@click.command()
@click.pass_context
def migrate_to_dgraph(
    ctx,
):
    ctx.forward(get_replica_models_to_sync_content)
    ctx.forward(gen_common_rdfs)
    ctx.forward(gen_custom_rdfs)
    os.system('cat replica/rdfs_*.txt > rdfs_to_migrate.rdf')


def _bridge_sync(rpc_client, record_id_lst, sync_content):
    for i in range(3):
        try:
            rpc_client.call('bridge_sync', [], {'operate_records_lst': record_id_lst})
            break
        except Exception:
            if i == 2:
                module_logger.exception('Fail to sync {}'.format(sync_content))
                raise
        time.sleep(float(random.randint(1, 3)) / 2)


def db_log_prepare(sync_contents):
    with quick_support.bkdata_log_session() as session:
        operations = [DbOperateLog(**operation) for operation in sync_contents]
        for operation in operations:
            session.add(operation)
        session.commit()
        return [m.id for m in operations]


def _get_batch_info():
    db = DgraphBackend(
        rt_context.m_resource.dgraph_session,
    )
    ret = db.query(statement)
    data = ret['data']
    for k, v in list(data.items()):
        v_dct = {item['v']: item['uid'] for item in v}
        data[k] = v_dct
    return data
