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

from enum import Enum

import attr
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session, sessionmaker

from metadata.backend.erp.utils import combine_filters, pretty_statement
from metadata.backend.interface import LineageDirection
from metadata.db_models.meta_service.replica_models import DataProcessingRelation
from metadata.runtime import rt_local
from metadata_biz.types.entities.data_set import AccessRawData, ResultTable

from ..interface import Backend, BackendOperator, BackendSessionHub


class MySQLSession(Session):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.expunge_all()
        if exc_type:
            self.rollback()
        self.close()


class MySQLSessionHub(BackendSessionHub):
    """
    mysql后端事务池。
    """

    def __init__(self, uri, pool_size, pool_maxsize, db_conf, **kwargs):
        super(MySQLSessionHub, self).__init__(uri, pool_size, pool_maxsize, **kwargs)

        self.engine = create_engine(
            uri,
            pool_recycle=db_conf.DB_POOL_RECYCLE,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=int(pool_maxsize - pool_size),
        )
        self.session_factory = sessionmaker(bind=self.engine, class_=MySQLSession)

    def session(self, in_local=None):
        if in_local:
            if not getattr(rt_local, in_local, None):
                setattr(rt_local, in_local, self.session_factory())
            session = getattr(rt_local, in_local)
        else:
            session = self.session_factory()

        return session


class EntityTypeSupportLineage(Enum):
    RAW_DATA = 'raw_data'
    RESULT_TABLE = 'result_table'


@attr.s(frozen=True)
class DataNode(object):
    data_set_type = attr.ib()
    data_set_id = attr.ib()
    qualified_name = attr.ib(init=False)
    type = attr.ib(init=False)

    def __attrs_post_init__(self):
        object.__setattr__(self, str("qualified_name"), self.data_set_id)
        object.__setattr__(self, str("type"), self.data_set_type)


@attr.s(frozen=True)
class ProcessNode(object):
    processing_id = attr.ib()
    qualified_name = attr.ib(init=False)
    type = attr.ib(default='data_processing')

    def __attrs_post_init__(self):
        object.__setattr__(self, str("qualified_name"), self.processing_id)


@attr.s(frozen=True)
class Edge(object):
    from_ = attr.ib(default=None)
    to = attr.ib(default=None)


class MySQLUniversalOperator(BackendOperator):
    """
    mysql后端通用处理器。
    """

    def __init__(self, session, replica_mappings, mappings, **kwargs):
        super(MySQLUniversalOperator, self).__init__(session, **kwargs)
        self.session_hub = self.session
        self.replica_mappings = replica_mappings
        self.mappings = mappings

    def query(self, *args, **kwargs):
        raise NotImplementedError

    def create(self, *args, **kwargs):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        raise NotImplementedError

    def update(self, *args, **kwargs):
        raise NotImplementedError

    def operate(self, sql_sentence, orderly=False, query_only=True):
        """
        mysql直接操作。直接执行sql语句。
        :param query_only: 是否只支持查询。
        :param sql_sentence: sql语句。
        :param orderly: 返回(k,v)列表还是字典。
        :return:
        """
        ts = self.session
        raw_factory = list if orderly else dict
        ret = [raw_factory(list(item.items())) for item in ts.execute(sql_sentence).fetchall()]
        if query_only:
            ts.rollback()
        return ret

    def __enter__(self):
        self.session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)


class MySQLBackend(Backend):
    """
    mysql后端。
    """

    def __init__(self, session_hub, **kwargs):
        super(MySQLBackend, self).__init__(session_hub, **kwargs)
        self.jinja_env = Environment(loader=FileSystemLoader(searchpath='metadata/backend/mysql/jinja/'))
        self.query_template = self.jinja_env.get_template('erp.jinja2')
        self.md_types = None
        self.replica_mappings = None
        self.mappings = None

    def gen_types(self, md_type, replica_mappings, mappings):
        self.md_types = md_type
        self.replica_mappings = replica_mappings
        self.mappings = mappings

    def operate_session(self, in_local=None):
        return MySQLUniversalOperator(self.session_hub.session(in_local), self.replica_mappings, self.mappings)

    def get_models_by_md_type(self, md_type):
        if md_type == 'RawData':
            md_type = AccessRawData
        elif md_type == 'ResultTable':
            md_type = ResultTable
        return self.replica_mappings[self.mappings[md_type]], self.mappings[md_type]

    def query_lineage(
        self,
        entity_type,
        data_set_id,
        depth=3,
        raw_direction=LineageDirection.BOTH,
        extra_retrieve=None,
        only_user_entity=None,
    ):
        depth = int(depth)
        nodes = set()
        relations = set()
        model = self.get_models_by_md_type(entity_type)[0]
        directions = (
            [raw_direction]
            if raw_direction is not LineageDirection.BOTH
            else [LineageDirection.OUTPUT, LineageDirection.INPUT]
        )
        data_set_type = 'raw_data' if model.__tablename__ == 'access_raw_data' else model.__tablename__
        _ = EntityTypeSupportLineage(data_set_type)
        true_max_depth = 0
        for direction in directions:
            start_nodes = {DataNode(data_set_type=data_set_type, data_set_id=data_set_id)}
            for i in range(depth):
                edges_lst = self.access_edges_once(start_nodes, direction)
                start_nodes = set()
                if edges_lst:
                    self.process_edges(edges_lst, nodes, relations, direction, start_nodes)
                if not start_nodes:
                    break
                if i > true_max_depth:
                    true_max_depth = i

        return {
            'criteria': {
                'entity_type': entity_type,
                'data_set_id': data_set_id,
                'depth': true_max_depth + 1,
                'direction': raw_direction.value,
            },
            'nodes': [attr.asdict(node) for node in nodes],
            'relations': [self.purify_relation(attr.asdict(relation)) for relation in relations],
        }

    def query_genealogy(self, entity_type, data_set_id, depth=3):
        nodes = set()
        relations = set()
        model = self.get_models_by_md_type(entity_type)[0]
        directions = [LineageDirection.OUTPUT, LineageDirection.INPUT]
        data_set_type = 'raw_data' if model.__tablename__ == 'access_raw_data' else model.__tablename__
        _ = EntityTypeSupportLineage(data_set_type)
        true_max_depth = 0
        start_nodes = {DataNode(data_set_type=data_set_type, data_set_id=data_set_id)}
        last_nodes = start_nodes
        for i in range(depth):
            next_start_nodes = set()
            last_nodes.update(start_nodes)
            for n, direction in enumerate(directions):
                edges_lst = self.access_edges_once(start_nodes, direction)
                if edges_lst:
                    self.process_edges(edges_lst, nodes, relations, direction, next_start_nodes)
            start_nodes = next_start_nodes
            if not (start_nodes - last_nodes):
                break
            if i > true_max_depth:
                true_max_depth = i

        return {
            'criteria': {'entity_type': entity_type, 'data_set_id': data_set_id, 'depth': true_max_depth + 1},
            'nodes': [attr.asdict(node) for node in nodes],
            'relations': [self.purify_relation(attr.asdict(relation)) for relation in relations],
        }

    @staticmethod
    def process_edges(edges_lst, nodes, relations, direction, start_nodes):
        if edges_lst:
            for edge in edges_lst:
                nodes.add(edge.from_)
                nodes.add(edge.to)
                relations.add(edge)
                if direction is LineageDirection.INPUT and isinstance(edge.from_, DataNode):
                    start_nodes.add(edge.from_)
                if direction is LineageDirection.OUTPUT and isinstance(edge.to, DataNode):
                    start_nodes.add(edge.to)

    @staticmethod
    def purify_relation(relation):
        relation['from'] = relation.pop('from_')
        relation['from'] = {k: v for k, v in relation['from'].items() if k in ('type', 'qualified_name')}
        relation['to'] = {k: v for k, v in relation['to'].items() if k in ('type', 'qualified_name')}
        return relation

    def access_edges_once(self, from_data_nodes, direction=LineageDirection.OUTPUT):
        from_direction, to_direction = (
            ('input', 'output') if direction == LineageDirection.OUTPUT else ('output', 'input')
        )

        edges_set = set()
        with self.operate_session().session as ts:
            for from_node in from_data_nodes:
                lst = (
                    ts.query(DataProcessingRelation)
                    .filter(
                        DataProcessingRelation.data_set_type == from_node.data_set_type,
                        DataProcessingRelation.data_set_id == from_node.data_set_id,
                        DataProcessingRelation.data_directing == from_direction,
                    )
                    .all()
                )
                dp_id_lst = [ret.processing_id for ret in lst]
                for dp_id in dp_id_lst:
                    lst = (
                        ts.query(DataProcessingRelation)
                        .filter(
                            DataProcessingRelation.processing_id == dp_id,
                            DataProcessingRelation.data_directing == to_direction,
                        )
                        .all()
                    )
                    for ret in lst:
                        to_node = DataNode(ret.data_set_type, ret.data_set_id)
                        if from_direction == 'input':
                            edges_set.add(Edge(from_=from_node, to=ProcessNode(processing_id=dp_id)))
                            edges_set.add(Edge(from_=ProcessNode(processing_id=dp_id), to=to_node))
                        else:
                            edges_set.add(Edge(from_=ProcessNode(processing_id=dp_id), to=from_node))
                            edges_set.add(Edge(from_=to_node, to=ProcessNode(processing_id=dp_id)))
        return edges_set

    def query_via_erp(self, erp_tree):
        """
        基于erp协议的模板查询

        :param erp_tree: erp语法解析树
        :return: dict 结果
        """
        result_mapping = dict()
        statement_list = list()
        with self.operate_session() as ops:
            for ee_tree in erp_tree.expressions:
                statement = pretty_statement(
                    self.query_template.render(ee_tree=ee_tree, combine_filters=combine_filters, get_list_length=len)
                )
                statement_list.append(statement)
                result_mapping[ee_tree.expression_name] = ops.operate(statement)
        return {'statement': statement_list, 'ret': result_mapping}
