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
from collections import defaultdict
from copy import deepcopy

import attr
from attrdict import AttrDict

from metadata.backend.dgraph.backend import DgraphInstanceConverter
from metadata.exc import InteractorError, InteractorExecuteError
from metadata.interactor.core import (
    Action,
    BatchAction,
    BatchOperationOrchestrator,
    SingleOperationOrchestrator,
)
from metadata.interactor.operators import (
    CommonDgraphOperator,
    CommonMySQLReplicaOperator,
)
from metadata.runtime import rt_context
from metadata.type_system.converter import CommonInstanceConverter
from metadata.util.i18n import lazy_selfish as _
from metadata_biz.interactor.mappings import (
    db_model_md_name_mappings,
    dgraph_action_operators,
    original_replica_db_model_mappings,
    replica_mysql_action_operators,
)
from metadata_biz.type_system.mappings import md_converters as md_converters
from metadata_biz.types import default_registry


class DGraphOrchestratorMixIn(object):
    """
    Dgraph编配混入类
    """

    action_operators = []
    action_operators.extend(dgraph_action_operators)
    action_operators.append(CommonDgraphOperator)
    type_name_info_mappings = {
        name: (name, cls, md_converters.get(cls, CommonInstanceConverter)) for name, cls in default_registry.items()
    }

    def construct_per_action(self, info, record):
        type_name, md_type, converter = info
        partial = True if record.method in ('UPDATE', 'DELETE') else False
        changed_info = json.loads(record.changed_data)
        # TODO：暂时兼容下Tag数据库主键和标示键不一致的问题。
        if type_name != 'Tag':
            changed_info[md_type.metadata['identifier'].name] = record.identifier_value
        cov = converter(AttrDict(**changed_info), md_type, partial)
        action = Action(method=record.method, item=cov.output, linked_record=record)
        self.logger.info('Action {} has been built.'.format(action))
        return action, cov

    def parse_record(self, record):
        type_name = record.type_name
        info = self.type_name_info_mappings.get(type_name)
        return info


class SingleDGraphOrchestrator(DGraphOrchestratorMixIn, SingleOperationOrchestrator):
    """
    Dgraph遍历编排类
    """

    erp_expression_obj = {'?:typed': None, '?:filter': None, '?:paging': {'limit': 200}, '?:count': True}

    def construct_actions(self, records):
        for record in records:
            split_records = []
            info = self.parse_record(record)
            if info:
                if record.method.startswith('CONDITIONAL_'):
                    split_records.extend(self.generate_records_with_filter(record))
                else:
                    split_records.append(record)
                for per_record in split_records:
                    action, cov = self.construct_per_action(info, per_record)
                    self.actions_lst.append(action)
            else:
                raise InteractorError(
                    _(
                        'Type name {} is not registered in SingleDGraphOrchestrator. The operate record is {}'.format(
                            record.type_name, record.operate_id if record.operate_id else record
                        )
                    )
                )

    def generate_records_with_filter(self, record):
        erp_expression_obj = deepcopy(self.erp_expression_obj)
        erp_expression_obj['?:typed'] = record.type_name
        erp_expression_obj['?:filter'] = record.conditional_filter
        ret = rt_context.dgraph_backend.erp_v2.query([erp_expression_obj], rt_context.dgraph_session_now.query)['data']
        count, pks, full_records = 0, [], []
        for item in ret[record.type_name]:
            if 'count' in item:
                count = item['count']
            else:
                pks.append(item['identifier_value'])
        if count > 150:
            raise InteractorError('The item length to modify is larger than 150.')
        for k in pks:
            r = attr.evolve(record, identifier_value=k, method=record.method.split('CONDITIONAL_')[1])
            full_records.append(r)
        return full_records

    def gen_create_action_rdfs(self):
        rdfs = []
        for action in self.actions_lst:
            if action.method == 'CREATE':
                conv = DgraphInstanceConverter(rt_context.dgraph_backend.mappings)
                conv.input = action.item
                for item in conv.output:
                    rdfs.append(item.serialize())
        return rdfs


class BatchDGraphOrchestrator(DGraphOrchestratorMixIn, BatchOperationOrchestrator):
    """
    Dgraph批量Batch编排实例

    按用户提交顺序，尽可能聚合同类batch操作，但不打乱顺序
    """

    def construct_actions(self, records):
        split_actions = defaultdict(list)
        current_action_key = None
        current_bulk_idx = 0
        for record in records:
            if record.method.startswith('CONDITIONAL_'):
                continue
            info = self.parse_record(record)
            if info:
                action, cov = self.construct_per_action(info, record)
                split_key = (record.method, type(action.item).__name__)
                if current_action_key is not None and split_key != current_action_key:
                    current_bulk_idx += 1
                current_action_key = split_key
                split_actions[(current_bulk_idx, current_action_key)].append(action)
            else:
                raise InteractorError(
                    _(
                        'Type name {} is not registered in BatchDGraphOrchestrator. The operate record is {}'.format(
                            record.type_name,
                            record.operate_id if record.operate_id else record,
                        )
                    )
                )
        for action_key in sorted(split_actions):
            bulk_idx, (method_name, md_type_name) = action_key
            action_group = split_actions[action_key]
            batch_action = BatchAction(
                method=method_name,
                md_type_name=md_type_name,
                items=[simple_action.item for simple_action in action_group],
                linked_records=[simple_action.linked_record for simple_action in action_group],
            )
            self.batch_actions_lst.append(batch_action)


class MySQLReplicaOrchestratorMixIn(object):
    """
    MySQLReplica编排混入类
    """

    action_operators = []
    action_operators.extend(replica_mysql_action_operators)
    action_operators.append(CommonMySQLReplicaOperator)
    type_name_info_mappings = {
        db_model_md_name_mappings[original.__tablename__]: [original, replica]
        for original, replica in original_replica_db_model_mappings.items()
        if original.__tablename__ in db_model_md_name_mappings
    }

    def construct_per_action(self, info, record):
        original, replica = info
        action = Action(method=record.method, item=replica(**json.loads(record.changed_data)), linked_record=record)
        self.logger.info('Action {} has been built.'.format(action))
        return action


class MySQLReplicaOrchestrator(MySQLReplicaOrchestratorMixIn, SingleOperationOrchestrator):
    """
    MySQL遍历编排实例
    """

    def construct_actions(self, records):
        for record in records:
            info = self.type_name_info_mappings.get(record.type_name)
            if info:
                action = self.construct_per_action(info, record)
                self.actions_lst.append(action)
            else:
                raise InteractorExecuteError(
                    _(
                        'The type name {} is not watched in MySQReplicaInteractor. The record info is {}'.format(
                            record.type_name, record.operate_id if record.operate_id else record
                        )
                    )
                )


class BatchMySQLReplicaOrchestrator(MySQLReplicaOrchestratorMixIn, BatchOperationOrchestrator):
    """
    MySQL批量Batch编排实例

    按用户提交顺序，尽可能聚合同类batch操作，但不打乱顺序
    """

    def construct_actions(self, records):
        split_actions = defaultdict(list)
        current_action_key = None
        current_bulk_idx = 0
        for record in records:
            if record.method.startswith('CONDITIONAL_'):
                continue
            info = self.type_name_info_mappings.get(record.type_name)
            if info:
                action = self.construct_per_action(info, record)
                split_key = (record.method, type(action.item).__name__)
                if current_action_key is not None and split_key != current_action_key:
                    current_bulk_idx += 1
                current_action_key = split_key
                split_actions[(current_bulk_idx, current_action_key)].append(action)
            else:
                raise InteractorExecuteError(
                    _(
                        'Type name {} is not watched in BatchMySQLReplicaOrchestrator. The record info is {}'.format(
                            record.type_name,
                            record.operate_id if record.operate_id else record,
                        )
                    )
                )
        for action_key in sorted(split_actions):
            bulk_idx, (method_name, md_type_name) = action_key
            action_group = split_actions[action_key]
            batch_action = BatchAction(
                method=method_name,
                md_type_name=md_type_name,
                items=[simple_action.item for simple_action in action_group],
                linked_records=[simple_action.linked_record for simple_action in action_group],
            )
            self.batch_actions_lst.append(batch_action)
