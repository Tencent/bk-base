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
from enum import Enum

import attr

from metadata.exc import ERPError
from metadata.runtime import rt_context, rt_local
from metadata.type_system.core import MetaData, parse_list_type
from metadata.util.i18n import lazy_selfish as _

module_logger = logging.getLogger(__name__)


class LineageDirection(Enum):
    BOTH = 'BOTH'
    INPUT = 'INPUT'
    OUTPUT = 'OUTPUT'


@attr.s(frozen=True)
class LineageDataNode(object):
    uid = attr.ib()
    data_set_type = attr.ib()
    data_set_id = attr.ib(converter=str)
    qualified_name = attr.ib(init=False)
    type = attr.ib(init=False)
    generate_type = attr.ib(default='user')
    extra = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        object.__setattr__(self, str("qualified_name"), self.data_set_id)
        object.__setattr__(self, str("type"), self.data_set_type)


@attr.s(frozen=True)
class LineageProcessNode(object):
    uid = attr.ib()
    processing_id = attr.ib()
    qualified_name = attr.ib(init=False)
    type = attr.ib(default='data_processing')
    generate_type = attr.ib(default='user')
    extra = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        object.__setattr__(self, str("qualified_name"), self.processing_id)


@attr.s(frozen=True)
class LineageEdge(object):
    start = attr.ib(default=None)
    end = attr.ib(default=None)


class DgraphLineageMixin(object):
    """
    Dgraph 关联计算功能
    """

    def __init__(self, *args, **kwargs):
        super(DgraphLineageMixin, self).__init__(*args, **kwargs)
        self.lineage_with_extra_template = self.jinja_env.get_template('lineage_with_extra.jinja2')
        self.lineage_template = self.jinja_env.get_template('lineage.jinja2')
        self.lineage_check_template = self.jinja_env.get_template('check_lineage.jinja2')

    def query_lineage(
        self,
        entity_type,
        data_set_id,
        depth=3,
        raw_direction=LineageDirection.BOTH,
        extra_retrieve=None,
        only_user_entity=False,
    ):
        """
        查询血缘接口，目前专指数据源的血缘，仅支持从 result_table， raw_data 为搜索根节点

        :param raw_direction: 检索方向
        :param entity_type  数据集类型，目前仅支持 ResultTable / RawData
        :param data_set_id  数据集ID
        :param depth  血缘深度，以数据链路的深度为标准，以下举例说明
        :param extra_retrieve: ERP检索表达式
        :param only_user_entity: 是否只检索用户生成的实体

            A(DataNode) -> B^(ProcessNode) -> B(DataNode) -> C^(ProcessNode) -> C(DataNode)

            以 A 为起点，按照以上血缘关系，则

            depth(A)  = 0
            depth(B^) = 1
            depth(B)  = 1
            depth(C^) = 2
            depth(C)  = 2

        :param {LineageDirection} raw_direction  血缘追溯方向，OUTPUT 是正向 INPUT 是反向

        TODO:用户模式需增加起点实体校验，必须为用户创建的实体。
        """
        m_dgraph_entity_field = {'RawData': 'AccessRawData.id', 'ResultTable': 'ResultTable.result_table_id'}

        lineage_nodes = []
        lineage_edges = []
        user_lineage_nodes = []
        user_lineage_edges = []

        # 血缘深度定义与 Dgraph 有关深度的定义不一致，需要转换
        dgraph_depth = (depth + 1) * 2 - 1

        m_direction_query_template_args = {
            LineageDirection.OUTPUT.value: [
                dict(
                    relation='children',
                    relation_predicate='lineage.descend',
                    dgraph_entity_id_field=m_dgraph_entity_field[entity_type],
                    dgraph_entity_id=data_set_id,
                    depth=dgraph_depth,
                )
            ],
            LineageDirection.INPUT.value: [
                dict(
                    relation='parents',
                    relation_predicate='~lineage.descend',
                    dgraph_entity_id_field=m_dgraph_entity_field[entity_type],
                    dgraph_entity_id=data_set_id,
                    depth=dgraph_depth,
                )
            ],
            LineageDirection.BOTH.value: [
                dict(
                    relation='children',
                    relation_predicate='lineage.descend',
                    dgraph_entity_id_field=m_dgraph_entity_field[entity_type],
                    dgraph_entity_id=data_set_id,
                    depth=dgraph_depth,
                ),
                dict(
                    relation='parents',
                    relation_predicate='~lineage.descend',
                    dgraph_entity_id_field=m_dgraph_entity_field[entity_type],
                    dgraph_entity_id=data_set_id,
                    depth=dgraph_depth,
                ),
            ],
        }

        max_depth = 0
        for template_args in m_direction_query_template_args[raw_direction.value]:
            if extra_retrieve:
                # 进行额外的ERP协议查询
                parsed_retrieve_args = {}
                for k, per_retrieve_args in extra_retrieve.items():
                    per_parsed_retrieve_args = {}
                    md_type = rt_context.md_types_registry.get(k, None)
                    if not md_type:
                        raise ERPError(_('Fail to get md type in extra retrieve protocol content.'))
                    self._parse_per_retrieve_expression(per_retrieve_args, per_parsed_retrieve_args, md_type)
                    parsed_retrieve_args[k] = per_parsed_retrieve_args

                parsed_retrieve_args.update(template_args)
                for md_type_name in ['ResultTable', 'AccessRawData', 'DataProcessing']:
                    if md_type_name not in parsed_retrieve_args:
                        parsed_retrieve_args[md_type_name] = {}
                statement = self.lineage_with_extra_template.render(
                    is_dict=lambda x: isinstance(x, dict), **parsed_retrieve_args
                )
            else:
                statement = self.lineage_template.render(template_args)

            module_logger.info('[Dgraph Lineage] statement={}'.format(statement))
            response = self.query(statement)

            if extra_retrieve:
                nodes_extra = {
                    'ResultTable': {item['identifier_value']: item for item in response['data']['ResultTable']},
                    'AccessRawData': {item['identifier_value']: item for item in response['data']['AccessRawData']},
                    'DataProcessing': {item['identifier_value']: item for item in response['data']['DataProcessing']},
                }
            else:
                nodes_extra = None
            dgraph_nodes = response['data']['target']

            _max_depth = self._traverse_nodes(
                None,
                None,
                dgraph_nodes,
                lineage_nodes,
                user_lineage_nodes,
                lineage_edges,
                user_lineage_edges,
                nodes_extra=nodes_extra,
                only_user_entity=only_user_entity,
            )

            if _max_depth > max_depth:
                max_depth = _max_depth

        # 观察 dgraph 返回数据结构，存在以下场景
        #          A -> B -> C -> D
        #                    ^
        #                    |
        #                    E
        # 当 B.children 里有 (B -> C, C->D) 的关系时，E.children 中存在两种情况 (E->C, C->D) 或者 (E->C)
        # 所以对于最终遍历出来的结果来看，需要去重操作，保证结果的唯一性
        lineage_nodes = self._remove_dup_nodes(lineage_nodes)
        lineage_edges = self._remove_dup_edges(lineage_edges)

        # RT -> DP -> RT -> DP -> RT 虽然有三个 RT，但是深度只能算 2
        max_depth = max_depth - 1

        if only_user_entity:
            return_lineage_nodes = user_lineage_nodes
            return_lineage_edges = user_lineage_edges
        else:
            return_lineage_nodes = lineage_nodes
            return_lineage_edges = lineage_edges

        return {
            'criteria': {
                'entity_type': entity_type,
                'data_set_id': data_set_id,
                'depth': max_depth,
                'direction': raw_direction.value,
            },
            'nodes': [attr.asdict(node) for node in return_lineage_nodes],
            'relations': [self._purify_relation(attr.asdict(relation)) for relation in return_lineage_edges],
        }

    def check_lineage_integrity(self, check_list, session_ins=None):
        """
        检查data_processing_relation关联的数据流程血缘的完整性

        :param check_list: list 携带用于查询目标rt主键信息的数据列表
        :param session_ins: obj session实例，若没有session环境，则使用dgraph_backend代替
        :return: boolean 完整(True) or 不完整(False)
        """
        if check_list is None:
            return False
        if session_ins is None:
            session_ins = self
        lineage_nodes = []
        lineage_edges = []
        user_lineage_nodes = []
        user_lineage_edges = []
        identifier_set = set()
        for check_data in check_list:
            if check_data.get('method', None) != 'CREATE':
                continue
            changed_dict = json.loads(check_data['changed_data'])
            if (
                changed_dict.get('data_directing', None) == 'input'
                and changed_dict.get('data_set_type', None) == 'raw_data'
            ):
                continue
            identifier_val = changed_dict.get('data_set_id', None)
            if identifier_val:
                identifier_set.add(str(identifier_val))
        if not identifier_set:
            return True
        identifier_list_str = '["{}"]'.format('", "'.join(identifier_set))
        statement = self.lineage_check_template.render(
            dict(dgraph_entity_id_field='ResultTable.result_table_id', dgraph_entity_id=identifier_list_str, depth=201)
        )
        response = session_ins.query(statement)
        dgraph_nodes = response['data']['target']
        # DPR和DS的关系是一对一的，检查查询结果是否完备
        if len(identifier_set) != len(dgraph_nodes):
            return False
        # 获取豁免的节点
        check_exclude_nodes = self._get_check_exclude_nodes(response)
        # 只对platform为bkdata的dt进行检查，且排除豁免标签的dt检查
        need_check_nodes = [
            item
            for item in dgraph_nodes
            if item['ResultTable.platform'] == 'bkdata' and item['uid'] not in check_exclude_nodes
        ]
        if not need_check_nodes:
            return True
        # 遍历所有查到的data_set的血缘根源，若血缘根源不是raw_data，说明血缘断层
        for check_node in need_check_nodes:
            self._traverse_nodes(
                None, None, [check_node], lineage_nodes, user_lineage_nodes, lineage_edges, user_lineage_edges
            )
            lineage_nodes_list = [attr.asdict(node) for node in self._remove_dup_nodes(lineage_nodes)]
            if not lineage_nodes_list:
                return False
            root_node = lineage_nodes_list.pop()
            if root_node.get('type', None) != 'raw_data':
                return False
        return True

    @staticmethod
    def _get_check_exclude_nodes(check_data):
        # 检查数据结果是否有豁免血缘检查的标签
        check_exclude_nodes = set()
        check_tags = check_data.get('data', {}).get('check_tags', [])
        for check_tag in check_tags:
            for tag_item in check_tag.get('tags', []):
                if tag_item['Tag.code'] == 'lineage_check_exclude':
                    check_exclude_nodes.add(str(check_tag['uid']))
                    break
        return check_exclude_nodes

    def _traverse_nodes(
        self,
        from_lineage_node,
        direction,
        dgraph_nodes,
        lineage_nodes,
        user_lineage_nodes,
        lineage_edges,
        user_lineage_edges,
        nodes_extra=None,
        only_user_entity=False,
    ):
        """
        遍历 dgraph 引擎查询结果，提取 -> 转换 -> 存放血缘结果

        :param {LineageNode} 遍历起点，可为 None
        :param {LineageDirection} direction 起点与子节点的方向
        :param {DgraphNode[]} dgraph_nodes dgraph 引擎查询出来的结果
        :param {LineageNode[]} lineage_nodes 血缘节点列表，用于存放 dgraph 遍历过程中存在的节点
        :param {LineageEdge[]} lineage_edges 血缘边缘列表，用于存放 dgraph 遍历过程中存在的节点关系
        :paramExample dgraph_nodes 样例
            [
              {
                "AccessRawData.id": 1,
                "AccessRawData.raw_data_nane": "rawdata1",
                "children": [
                  {
                    "DataProcessing.processing_id": "591_rt1",
                    "DataProcessing.processing_alias": "591_rt1"
                  }
                ]
              }
            ]
        :return {Int} 当前 dgraph_nodes 所有节点往下遍历的最大深度
        :note 注意确保 dgraph_nodes 数据不会同时存在 children 和 parents
        """
        if nodes_extra is None:
            nodes_extra = {}

        # 当前深度
        max_depth = 0

        for _node in dgraph_nodes:

            _lineage_node = generate_lineage_node(_node, nodes_extra)
            if getattr(rt_local, 'last_user_status', None) is None:
                rt_local.last_user_status = {}

            # 遇到非预期的血缘节点，则跳过
            if _lineage_node is None:
                continue

            # 添加节点关系
            if from_lineage_node is not None and direction is not None:
                edge = None
                if direction == LineageDirection.OUTPUT:
                    edge = LineageEdge(from_lineage_node, _lineage_node)
                if direction == LineageDirection.INPUT:
                    edge = LineageEdge(_lineage_node, from_lineage_node)
                lineage_edges.append(edge)
                if only_user_entity:
                    edge_into_user = attr.evolve(edge)
                    self._process_user_edge(
                        direction, from_lineage_node, _lineage_node, user_lineage_edges, edge_into_user
                    )

            # 当前节点深度，等待遍历后的结果，默认0
            depth = 0

            lineage_nodes.append(_lineage_node)
            if _lineage_node.generate_type == 'user':
                user_lineage_nodes.append(_lineage_node)
            # 正向遍历
            if 'children' in _node:
                depth = self._traverse_nodes(
                    _lineage_node,
                    LineageDirection.OUTPUT,
                    _node['children'],
                    lineage_nodes,
                    user_lineage_nodes,
                    lineage_edges,
                    user_lineage_edges,
                    nodes_extra=nodes_extra,
                    only_user_entity=only_user_entity,
                )
            # 反向遍历
            if 'parents' in _node:
                depth = self._traverse_nodes(
                    _lineage_node,
                    LineageDirection.INPUT,
                    _node['parents'],
                    lineage_nodes,
                    user_lineage_nodes,
                    lineage_edges,
                    user_lineage_edges,
                    nodes_extra=nodes_extra,
                    only_user_entity=only_user_entity,
                )
            # 如果当前节点为数据节点，则深度 +1
            depth += 1 if isinstance(_lineage_node, LineageDataNode) else 0

            if depth > max_depth:
                max_depth = depth

        return max_depth

    def _parse_per_retrieve_expression(self, retrieve_expression, parsed_retrieve_expression, md_type):
        """
        解析额外的ERP协议表达式(临时)

        :param retrieve_expression: 协议表达式
        :param parsed_retrieve_expression: 解析后的适配GraphQL模板的表达式
        :param md_type: md类型
        :return:
        """

        attr_defs = attr.fields_dict(md_type)
        if retrieve_expression is True:
            retrieve_expression = {
                attr_def.name: True
                for attr_def in attr.fields(md_type)
                if not issubclass(parse_list_type(attr_def.type)[1], MetaData)
            }
        wildcard = retrieve_expression.pop('*', False)
        if wildcard:
            retrieve_expression.update(
                {
                    attr_def.name: True
                    for attr_def in attr.fields(md_type)
                    if not issubclass(parse_list_type(attr_def.type)[1], MetaData)
                }
            )
        for k, v in retrieve_expression.items():
            if k in attr_defs:
                is_list, ib_primitive_type = parse_list_type(attr_defs[k].type)
                if not issubclass(ib_primitive_type, MetaData):
                    parsed_retrieve_expression[
                        ''.join(
                            [
                                k,
                                ':',
                                md_type.__name__,
                                '.',
                                k,
                            ]
                        )
                        if k not in md_type.__metadata__.get('dgraph').get('common_predicates')
                        else k
                    ] = v
                else:
                    k_parsed_retrieve_expression = {}
                    self._parse_per_retrieve_expression(v, k_parsed_retrieve_expression, ib_primitive_type.agent)
                    parsed_retrieve_expression[
                        ''.join(
                            [
                                k,
                                ':',
                                md_type.__name__,
                                '.',
                                k,
                            ]
                        )
                    ] = k_parsed_retrieve_expression
            elif k.startswith('~') and k.split('~')[1] in md_type.metadata['dgraph']['reverse_edges']:
                k_parsed_retrieve_expression = {}
                self._parse_per_retrieve_expression(
                    v, k_parsed_retrieve_expression, md_type.metadata['dgraph']['reverse_edges'][k.split('~')[1]]
                )
                parsed_retrieve_expression[k] = k_parsed_retrieve_expression
            else:
                raise ERPError(_('Not existed attr name {}'.format(k)))
        parsed_retrieve_expression[
            ''.join(
                [
                    'identifier_value',
                    ':',
                    md_type.__name__,
                    '.',
                    md_type.metadata['identifier'].name,
                ]
            )
        ] = True

    @staticmethod
    def _process_user_edge(direction, from_lineage_node, _lineage_node, user_lineage_edges, edge_into_user):
        """
        记录血缘中，仅用户创建的实体节点的关联。并生成跨虚拟节点的关联。
        :param direction: 方向
        :param from_lineage_node: 起始节点
        :param _lineage_node: 指向节点
        :param user_lineage_edges: 用户血缘关联
        :param edge_into_user: 可能被用户血缘记录的当前关联
        :return:
        """
        if from_lineage_node.generate_type == 'user':
            rt_local.last_user_status[_lineage_node.qualified_name] = edge_into_user
            if _lineage_node.generate_type == 'system':
                return
            user_lineage_edges.append(edge_into_user)
        elif from_lineage_node.generate_type == 'system' and _lineage_node.generate_type == 'system':
            previous_user_edge = rt_local.last_user_status[from_lineage_node.qualified_name]
            rt_local.last_user_status[_lineage_node.qualified_name] = previous_user_edge
        elif from_lineage_node.generate_type == 'system' and _lineage_node.generate_type == 'user':
            previous_user_edge = rt_local.last_user_status[from_lineage_node.qualified_name]
            edge = attr.evolve(
                previous_user_edge, **{'end' if direction == LineageDirection.OUTPUT else 'start': _lineage_node}
            )
            user_lineage_edges.append(edge)

    @staticmethod
    def _remove_dup_nodes(lineage_nodes):
        """
        移除重复节点

        :todo 当 lineage_nodes 过大时，会引起过多的内存开销
        """
        verified_lineage_nodes = []

        # 使用集合判断是否存在更为高效
        indexs = set()

        for _node in lineage_nodes:

            if _node.uid not in indexs:
                verified_lineage_nodes.append(_node)
                indexs.add(_node.uid)

        return verified_lineage_nodes

    @staticmethod
    def _remove_dup_edges(lineage_edges):
        """
        移除重复的关系
        """
        verified_lineage_edges = []

        # 使用集合判断是否存在更为高效
        indexs = set()

        for _edge in lineage_edges:

            _index = '{}::{}'.format(_edge.start.uid, _edge.end.uid)
            if _index not in indexs:
                verified_lineage_edges.append(_edge)
                indexs.add(_index)

        return verified_lineage_edges

    @staticmethod
    def _purify_relation(relation):
        """
        仅保留 relation 中的目标字段
        """
        targer_node_fields = ['type', 'qualified_name']

        return {
            'from': {k: v for k, v in relation['start'].items() if k in targer_node_fields},
            'to': {k: v for k, v in relation['end'].items() if k in targer_node_fields},
        }


def generate_lineage_node(node, nodes_extra):
    """
    识别 dgraph 数据，转换为对应的血缘节点对象
    """
    if 'AccessRawData.id' in node:
        extra = get_node_extra(nodes_extra, node['AccessRawData.id'], 'AccessRawData')
        return LineageDataNode(node['uid'], 'raw_data', node['AccessRawData.id'], extra=extra)

    if 'ResultTable.result_table_id' in node:
        extra = get_node_extra(nodes_extra, node['ResultTable.result_table_id'], 'ResultTable')
        return LineageDataNode(
            node['uid'],
            'result_table',
            node['ResultTable.result_table_id'],
            extra=extra,
            generate_type=node['ResultTable.generate_type'],
        )

    if 'DataProcessing.processing_id' in node:
        extra = get_node_extra(nodes_extra, node['DataProcessing.processing_id'], 'DataProcessing')
        return LineageProcessNode(
            node['uid'],
            node['DataProcessing.processing_id'],
            extra=extra,
            generate_type=node['DataProcessing.generate_type'],
        )

    return None


def get_node_extra(nodes_extra, identifier_value, md_type_name):
    extra = {}
    if nodes_extra and identifier_value in nodes_extra[md_type_name]:
        extra = nodes_extra[md_type_name][identifier_value]
        # 血缘原生遍历时，会有重复节点
        if 'identifier_value' in extra:
            extra.pop('identifier_value')
    return extra
