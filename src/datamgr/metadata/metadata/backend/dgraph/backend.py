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
from collections import OrderedDict, defaultdict
from copy import deepcopy
from datetime import datetime
from typing import List, Type, Union

import arrow
import attr
from cached_property import cached_property
from jinja2 import Environment, FileSystemLoader, Template
from singledispatch import singledispatch

from metadata.backend.dgraph.client import DgraphClient
from metadata.backend.dgraph.lineage import DgraphLineageMixin
from metadata.backend.erp.utils import combine_filters, pretty_statement
from metadata.backend.interface import Backend, BackendOperator, BackendSessionHub
from metadata.exc import (
    DgraphBackendError,
    DgraphClientError,
    DgraphDefConvertError,
    DgraphInstanceConvertError,
    MetaDataError,
)
from metadata.runtime import rt_context
from metadata.state import StateMode
from metadata.type_system.basic_type import Internal
from metadata.type_system.core import (
    MetaData,
    ReferredMetaData,
    gen_instance_type,
    parse_list_type,
)
from metadata.util.common import Empty, StrictABCMeta, snake_to_camel, version_compare
from metadata.util.i18n import lazy_selfish as _


@attr.s
class DgraphDef(object):
    """
    Dgraph predicate定义数据结构
    """

    name = attr.ib(type=str)
    type = attr.ib(type=str)
    is_list = attr.ib(type=bool, default=False)
    directives = attr.ib(type=list, factory=list)
    metadata = attr.ib(type=dict, factory=dict)

    def serialize(self, dgraph_ver=None):
        full_str_lst = []
        for directive in self.directives:
            if isinstance(directive, (tuple, list)):
                full_str_lst.append('@{}({})'.format(directive[0], ','.join(directive[1])))
            else:
                full_str_lst.append('@{}'.format(directive))
        full_str_lst.append('.')
        if version_compare(dgraph_ver, '1.1.0') >= 0:
            full_str_lst.insert(0, self.type if not self.is_list else '[{}]'.format(self.type))
        else:
            full_str_lst.insert(0, self.type)
        full_str_lst.insert(0, '<{}>:'.format(self.name))
        return ' '.join(full_str_lst)


@attr.s
class DgraphNode(object, metaclass=StrictABCMeta):
    __abstract__ = True
    """
    Dgraph Node数据结构
    """


@attr.s
class Identifier(DgraphNode):
    """
    Dgraph Identifier Node数据结构
    """

    md_type = attr.ib(type=Type[MetaData])
    identifier_value = attr.ib()


@attr.s
class Uid(DgraphNode):
    """
    Dgraph Uid Node数据结构
    """

    uid = attr.ib(type=str)


@attr.s
class Asterisk(DgraphNode):
    """
    Dgraph * Node数据结构，用于指示批量操作
    """

    pass


@attr.s
class UidVariable(Identifier):
    """
    Dgraph uid(alias) Node数据结构，继承于Identifier，用于批量查询变更操作的中间变量存储
    """

    alias = attr.ib(type=str)


@attr.s
class DgraphMetaData(object):
    """
    Dgraph元数据类型。起始父类。
    """

    predicate = None
    subject = attr.ib(type=Union[str, DgraphNode])
    object_ = attr.ib(type=Union[str, DgraphNode])

    def serialize(self):
        items_lst = [self.subject, '<{}>'.format(self.predicate.name), self.object_, '.']
        str_lst = []
        for n, item in enumerate(items_lst):
            if isinstance(item, UidVariable):
                str_lst.append('uid(' + item.alias + ')')
            elif isinstance(item, Identifier):
                str_lst.append('_:' + item.md_type.__name__ + '.' + str(item.identifier_value))
            elif isinstance(item, Uid):
                str_lst.append('<{}>'.format(item.uid))
            elif isinstance(item, Asterisk):
                str_lst.append('*')
            elif n == 2:
                str_lst.append(json.dumps(str(item)))
            else:
                str_lst.append(str(item))
        return ' '.join(str_lst)


class DgraphSessionHub(BackendSessionHub):
    """
    Dgraph后端连接池，支持事务。
    """

    def __init__(self, servers, pool_size=20, pool_maxsize=20 * 2, **kwargs):
        super(DgraphSessionHub, self).__init__(servers, pool_size, pool_maxsize)
        self.client = DgraphClient(servers, False, pool_size, pool_maxsize)

    def session(self, read_only=False, best_effort=False):
        """
        开启一个事务
        :param read_only: 只读
        :param best_effort: 只读属性下最佳检索方式
        :return:
        """
        return self.client.tnx(read_only=read_only, best_effort=best_effort)


class DgraphBackend(DgraphLineageMixin, Backend):
    """
    Dgraph后端
    """

    default_ver = '1.2.3'
    drop_predicate_template = Template('{"drop_attr": "{{predicate}}"}')
    none_node_query_statement = '{none_node(func: eq(Internal.role,"as_none")) {uid}}'

    def __init__(self, session_hub, cache_manager=None, id_=None, ver_=None, **kwargs):
        self.jinja_env = Environment(loader=FileSystemLoader(searchpath='metadata/backend/dgraph/jinja/'))
        self.query_template = self.jinja_env.get_template('erp.jinja2')
        self.mappings = None
        self.md_types = None
        self.dgraph_types = None
        self.id = id_ if id_ else 'separated'
        self.dgraph_ver = ver_ if ver_ else self.default_ver
        super(DgraphBackend, self).__init__(
            session_hub,
            cache_manager,
        )

    def operate_session(self, in_local=None):
        """
        获取封装了统一方法的事务对象
        :return:
        """
        session = self.session_hub.session()
        if (
            rt_context.system_state.mode == StateMode.ONLINE
            and rt_context.system_state.dgraph_best_effort(self.id) == 'on'
        ):
            raise DgraphBackendError("It's BE mode now. Modify is not allowed here.")
        return DgraphOperator(session, self.mappings, backend=self)

    def query(
        self,
        statement,
        read_only=True,
        best_effort=False,
    ):
        """
        单次只读查询

        :param best_effort: 是否跳过事务机制
        :param read_only: 是否为只读查询
        :param statement: 查询语句
        :return: 结果
        """
        client = self.session_hub.client
        if (
            rt_context.system_state.mode == StateMode.ONLINE
            and rt_context.system_state.dgraph_best_effort(self.id) == 'on'
        ):
            best_effort = True
        response = client.query(statement, read_only=read_only, best_effort=best_effort)
        return self.pre_deal_query_response(response)

    def query_via_erp(self, erp_tree):
        """
        基于erp协议的模板查询

        :param erp_tree: erp语法解析树
        :return: dict 执行语句列表 & 执行结果
        """
        statement = pretty_statement(
            self.query_template.render(erp_tree=erp_tree, combine_filters=combine_filters, get_list_length=len),
        )
        return {'statement': [statement], 'ret': self.query(statement)}

    def pre_deal_query_response(self, response):
        """
        预处理Dgraph查询返回结果, 保持输出兼容旧版本输出
        :param response:
        :return:
        """
        data_ret_collection = response.get('data', {})
        response['data'] = self._format_res_dict_recursively(data_ret_collection)
        return response

    def _format_res_dict_recursively(self, data_input, depth=0):
        """
        递归对dgraph返回数据进行格式化
        :param data_input: dict 需要格式化的数据字典
        :param depth: int 递归深度
        :return: dict 经过格式化之后的数据字典
        """
        if depth >= 20:
            return {}
        depth += 1
        if isinstance(data_input, dict):
            for key, item in list(data_input.items()):
                if isinstance(item, dict):
                    data_input[key] = [item]
                data_input[key] = (
                    [self._format_res_dict_recursively(sub_item, depth) for sub_item in data_input[key]]
                    if isinstance(data_input[key], list)
                    else data_input[key]
                )
        return data_input

    def convert_types(self, type_storage):
        """
        将标准类型转换为Dgraph类型

        :param type_storage: 标准类型存储
        :return: Dgraph类型，映射关系
        """
        types = []
        mappings = {}
        for md_type in type_storage:
            cov = DgraphTypeConverter()
            cov.input = md_type
            types.extend(cov.output[0])
            mappings.update(cov.output[1])
            if 'dgraph' not in md_type.metadata:
                md_type.metadata['dgraph'] = {}
            md_type.metadata['dgraph']['reverse_edges'] = self.gen_dgraph_reverse_edges(md_type)
        return types, mappings

    @staticmethod
    def gen_dgraph_reverse_edges(md_type):
        """
        生成某元数据类型的反向引用边

        :param md_type:
        :return:
        """
        full_reverse_referenced = set()
        for s in [getattr(item, 'metadata', {}).get('reverse_referenced', set()) for item in md_type.__mro__]:
            full_reverse_referenced = full_reverse_referenced.union(s)
        edges = {
            '.'.join([r[0].__name__, r[1].name])
            if not r[1].metadata.get('dgraph', {}).get('predicate_name')
            else r[1].metadata['dgraph']['predicate_name']: r[0]
            for r in full_reverse_referenced
        }
        return edges

    @cached_property
    def none_node(self):
        """
        空白节点

        :return:
        """
        ret = self.query(self.none_node_query_statement)
        return Uid(ret['data']['none_node'][0]['uid'])

    def gen_types(self, md_type_storage):
        """
        在后端中，载入当前存储类型及关系

        :param md_type_storage: 标准类型存储
        :return:
        """
        (self.dgraph_types, self.mappings), self.md_types = self.convert_types(md_type_storage), md_type_storage

    def declare_types(self, dry=False):
        """
        声明定义的类型。
        :return:
        """
        all_def_contents = set()
        if not dry:
            self._cleanup_dgraph_schema()
        all_predicates = {}
        for dgraph_type in self.dgraph_types:
            if dgraph_type.predicate.name not in all_predicates:
                all_predicates[dgraph_type.predicate.name] = dgraph_type.predicate.type
            else:
                if dgraph_type.predicate.type != all_predicates[dgraph_type.predicate.name]:
                    raise DgraphBackendError(_('Predicate is declaring with different type.'))
            predicate_def_content = dgraph_type.predicate.serialize(self.dgraph_ver)
            all_def_contents.add(predicate_def_content)
        all_def_contents = list(all_def_contents)
        all_def_contents.sort()
        all_def_content = str('\n').join([str(item) for item in all_def_contents])
        if dry:
            return all_def_content

        self.session_hub.client.alter(all_def_content)
        self._set_node_as_none()

    def gen_types_diff(self):
        """
        生成本地和dgraph模型差异
        :return: False/dict
        Todo: 增加字段索引关联变更的展示
        """
        if self.dgraph_types is None:
            return False
        ret = self.query('schema {}')
        p_names_server = {p['predicate'] for p in ret['data']['schema']}
        p_names_local = {t.predicate.name for t in self.dgraph_types}
        def_add_contents = p_names_local - p_names_server
        def_delete_contents = p_names_server - p_names_local
        def_delete_contents = {
            p_name
            for p_name in def_delete_contents
            if not p_name.startswith('_') and not p_name.startswith('internal') and not p_name.startswith('dgraph')
        }
        return dict(add=def_add_contents, delete=def_delete_contents)

    def _cleanup_dgraph_schema(self):
        ret = self.query('schema {}')
        p_names_server = {p['predicate'] for p in ret['data']['schema']}
        p_names_local = {t.predicate.name for t in self.dgraph_types}
        p_names_to_del = p_names_server - p_names_local
        for p_name in p_names_to_del:
            if not p_name.startswith('_') and not p_name.startswith('internal') and not p_name.startswith('dgraph'):
                self.session_hub.client.alter(self.drop_predicate_template.render(predicate=p_name))

    def _set_node_as_none(self):
        with self.operate_session() as op_se:
            op_se.upsert(Internal(role='as_none'), 'set', commit_now=True)


class DgraphOperator(BackendOperator):
    def __init__(self, session, mappings, **kwargs):
        """
        Dgraph Backend 操作者

        :param object session: backend会话连接
        :param dict mappings: 元数据建模类型和Dgraph元数据类型的映射关系 (md_type, attr_def): DgraphMetaData
        """

        super(DgraphOperator, self).__init__(session, **kwargs)
        self.mappings = mappings
        self.backend = kwargs[str('backend')] if str('backend') in kwargs else None
        self.uid_access_template = self.backend.jinja_env.get_template('uid_access.jinja2')
        self.event_targets_list = getattr(rt_context, 'event_targets', [])

    def query(self, statement):
        """
        查询，使用GraphQL语句。

        :param statement: GraphQL语句
        :return: dict 结果
        """
        response = self.session.query(statement)
        return self.backend.pre_deal_query_response(response)

    def create(self, mds, total_new=False, commit_now=False):
        """
        保存新元数据

        :param total_new: 是否提交的数据(包括引用的数据)都是新建的
        :param mds: 标准元数据实例，单个或列表
        :param commit_now: 立即提交
        :return: 结果
        """
        if not isinstance(mds, (tuple, list)):
            mds = [mds]
        instances = self.convert_instances(mds, node_form='blank', action='set')
        if not total_new:
            self.replace_with_existed_uids(instances)
        modify_content_lst = [instance.serialize() for instance in instances]
        return self.common_operate(modify_content_lst, 'set', 'rdf', commit_now)

    def update(self, mds, commit_now=False):
        """
        更新既有元数据

        :param mds: 标准元数据实例，单个或列表
        :param commit_now:立即提交
        :return: 结果
        """
        if not isinstance(mds, (tuple, list)):
            mds = [mds]
        instances = self.convert_instances(mds, node_form='blank', action='set')
        self.replace_with_existed_uids(instances)
        # 如果未找到既有节点，放弃此条记录的操作，避免更新操作生成脏节点
        modify_content_lst = [instance.serialize() for instance in instances if isinstance(instance.subject, Uid)]
        return self.common_operate(modify_content_lst, 'set', 'rdf', commit_now)

    def delete(self, mds, partial=False, commit_now=False):
        """
        删除既有元数据

        :param mds: 标准元数据实例，单个或列表
        :param partial: 仅删除被标记的属性。(主要用于一对多关联中删除其中一条)
        :param commit_now:立即提交
        :return: 结果
        """
        if not isinstance(mds, (tuple, list)):
            mds = [mds]
        instances = self.convert_instances(mds, node_form='blank', action='delete')
        self.replace_with_existed_uids(instances)
        modify_content_lst = []
        # 删除节点的部分关联、属性，不删除节点本身
        if partial:
            for instance in instances:
                # 既有节点不存在，避免删除blank节点报错
                if not isinstance(instance.subject, Uid):
                    continue
                # 不能删除主键标识
                if instance.predicate.metadata.get('as_identifier', False):
                    continue
                rdf_group_lst = instance.serialize().split(' ')
                # 非边非列表的属性值用*删除，否则删除指定值
                if (not instance.predicate.is_list) and instance.predicate.type != 'uid':
                    rdf_group_lst[2] = '*'
                modify_content_lst.append(' '.join(rdf_group_lst))
        # 删除整个节点
        else:
            for instance in instances:
                # 既有节点不存在，避免删除blank节点报错
                if not isinstance(instance.subject, Uid):
                    continue
                rdf_group_lst = instance.serialize().split(' ')
                rdf_group_lst[2] = '*'
                modify_content_lst.append(' '.join(rdf_group_lst))
        return self.common_operate(modify_content_lst, 'delete', 'rdf', commit_now)

    def upsert(self, mds, action, is_exists=False, partial=False, commit_now=False):
        """
        dgraph upsert block 实现
        对单个节点进行单次upsert变更

        :param mds: 标准元数据类型实例, 单个节点或节点列表
        :param action: set-设置;delete-删除
        :param is_exists: 限制部分修改(修改目标必须存在)
        :param partial: 是否只操作主键以外的属性
        :param commit_now: 是否立即提交
        :return: 执行结果
        """
        if not isinstance(mds, (tuple, list)):
            mds = [mds]
        identifier_cond = dict(name=None, value=set())
        link_target_map = defaultdict(set)
        instances = self.convert_instances(mds, node_form='var', action=action)
        for instance in instances:
            # 主键信息
            if instance.predicate.metadata.get('as_identifier', False):
                identifier_cond['name'] = instance.predicate.name
                identifier_cond['value'].add(instance.object_)
            # 外键信息
            elif instance.predicate.type == 'uid' and isinstance(instance.object_, Identifier):
                link_target_id = getattr(instance.object_, 'identifier_value', None)
                link_target_md_type = getattr(instance.object_, 'md_type', None)
                if link_target_id and link_target_md_type:
                    link_target_attr_name = '{}.{}'.format(
                        link_target_md_type.__name__, link_target_md_type.metadata['identifier'].name
                    )
                    link_target_map[(instance.predicate.name, link_target_attr_name)].add(str(link_target_id))
        # 判断upsert时是否需要获取操作前信息
        attr_name_set = self._get_attr_name_set(mds, instances)
        if_cond, statement = self._gen_upsert_statement(
            identifier_cond, link_target_map, attr_name_set, is_exists=is_exists
        )
        modify_content_lst = [
            item.serialize()
            for item in instances
            if not (item.predicate.metadata.get('as_identifier', False) and partial)
        ]
        if not statement:
            raise MetaDataError(_("The identifier of metadata can not be found"))
        return self.common_operate(modify_content_lst, action, 'rdf', commit_now, statement, if_cond)

    def _get_attr_name_set(self, mds, instances):
        """
        对注册了事件监听的模型，提取模型字段

        :param mds: 元数据类型md实例
        :param instances: 元数据RDF实例
        :return: set 模型字段集合
        """
        attr_name_set = set()
        for md_i in mds:
            instance_type = gen_instance_type(md_i)
            i_name = instance_type.__name__
            if i_name not in self.event_targets_list:
                continue
            common_attr_lst = list(instance_type.__metadata__['dgraph']['common_predicates'].keys())
            for attr_def in attr.fields(instance_type):
                attr_name = (
                    '{}.{}'.format(i_name, attr_def.name) if attr_def.name not in common_attr_lst else attr_def.name
                )
                attr_name_set.add(attr_name)
        if attr_name_set:
            # Todo: 配置化
            # 如果变更目标所关联的rt，只包含不展示给用户的临时rt，则不需要提取模型字段
            tmp_rt_tokens = set('FlinkSqlStaticJoin')
            check_list = []
            for instance in instances:
                if instance.predicate.name == 'result_table_id':
                    check_list.append(any([token in str(instance.object_) for token in tmp_rt_tokens]))
                if check_list and all(check_list):
                    attr_name_set = set()
                    break
        return attr_name_set

    @staticmethod
    def _gen_upsert_statement(identifier_cond, link_target_map, attr_name_set, is_exists=False):
        """
        根据主键和外键的uid生成dgraph查询statement
        :param identifier_cond: 主键条件
        :param link_target_map: 外键映射
        :param attr_name_set: 属性名称集合
        :param is_exists: 限制部分修改(目标必须存在)
        :return: tuple
        """

        id_key = identifier_cond['name']
        id_val = identifier_cond['value']
        if not id_key or not id_val:
            return False, False
        identifier_filter_tpl = (
            'get_subject_info(func: eq( {subject_key}, {subject_val} ))' '{{ {subject_tag} as uid {attr_list} }}'
        )
        identifier_filter = identifier_filter_tpl.format(
            subject_key=id_key,
            subject_val='"{}"'.format(id_val.pop()) if len(id_val) == 1 else '["{}"]'.format('", "'.join(id_val)),
            subject_tag='subject',
            attr_list='' if not is_exists else ' '.join(list(attr_name_set)),
        )
        link_filter_tpl = 'get_{link_key}_uid(func: eq( {link_target_key}, {link_val} )) {{ {link_tag} as uid }}'
        filter_list = [
            link_filter_tpl.format(
                link_key=key,
                link_target_key=target_key,
                link_val='"{}"'.format(value.pop()) if len(value) == 1 else '["{}"]'.format('", "'.join(list(value))),
                link_tag='o_{}'.format(key),
            )
            for (key, target_key), value in list(link_target_map.items())
        ]
        filter_list.insert(0, identifier_filter)
        statement = '{{ {} }}'.format("\n".join(filter_list))
        if_cond = '' if not is_exists else '@if(gt(len({}), 0))'.format('subject')
        return if_cond, statement

    def common_operate(self, modify_content_lst, action, data_format, commit_now, statement='', if_cond=''):
        """
        变更操作提交统一公共入口
        * MUTATE_CHUNKED_SIZE 参数控制允许的单次提交数量
        * 控制提交是否按事务处理

        :param list modify_content_lst: 变更内容rdf列表
        :param string action: 变更操作类型
        :param string data_format: 变更数据的format(rdf/json)
        :param boolean commit_now: 是否跳过事务直接提交(True/False)
        :param string statement: 可选，upsert操作附带的查询语句
        :param string if_cond: 可选，upsert操作附带的条件表达式
        :return: mixed 变更操作提交结果
        """
        if len(modify_content_lst) > rt_context.config_collection.dgraph_backend_config.MUTATE_CHUNKED_SIZE:
            if not commit_now:
                chunked_lst = self._optimize_mutate_operation(
                    modify_content_lst, rt_context.config_collection.dgraph_backend_config.MUTATE_CHUNKED_SIZE
                )
                for chunk in chunked_lst:
                    self.session.mutate(chunk, action, data_format, commit_now, statement, if_cond)
            else:
                DgraphClientError(_('The content to commit now is larger than the chunked size limitation.'))
        else:
            return self.session.mutate(modify_content_lst, action, data_format, commit_now, statement, if_cond)

    @staticmethod
    def _optimize_mutate_operation(modify_content_lst, chunk_cnt):
        """
        分割提交的RDF

        :param list modify_content_lst: rdf列表
        :param int chunk_cnt: 分割数量
        :return: list 分割后的列表
        """
        chunked_lst = []
        subject_dct = OrderedDict()

        # 通过subject_dct确保相同实体的属性在同一次操作中录入。防止创建多个相同含义节点等问题发生。
        for n, item in enumerate(modify_content_lst):
            subject_value = item.split(' ')[0]
            if subject_value not in subject_dct:
                subject_dct[subject_value] = []
            subject_dct[subject_value].append(item)
        chunk_lst = []
        for k, v in list(subject_dct.items()):
            if len(chunk_lst) + len(v) > chunk_cnt:
                if len(chunk_lst) == 0:
                    raise DgraphBackendError(_("The count of one node's edge is more than MUTATE_CHUNKED_SIZE."))
                chunked_lst.append(chunk_lst)
                chunk_lst = []
            chunk_lst.extend(v)
        if chunk_lst:
            chunked_lst.append(chunk_lst)
        return chunked_lst

    def operate(self, method, *args, **kwargs):
        """
        通用操作方法，直接透传操作dgraph tnx.

        :param method: 方法名
        :param args: 参数
        :param kwargs: 关键字参数
        :return: 结果
        """
        return getattr(self.session, method)(*args, **kwargs)

    def commit(self):
        """
        提交事务

        :return:
        """
        # 兼容
        dgraph_ver = getattr(self.backend, str('dgraph_ver'), None)
        if version_compare(dgraph_ver, '1.1.0') >= 0:
            return self.session.commit()
        return self.session.commit(start_ts_on_url=True)

    def convert_instances(self, md_instances, identifier_only=False, node_form='blank', action='set'):
        """
        将元数据实例转换为dgraph实例。

        :param identifier_only: 是否只生成指示属性实例
        :param md_instances: 元数据实例列表
        :param node_form: 元数据实例节点描述形式
        :param action: rdf实例产出后要进行的操作
        :return:
        """
        dgraph_instances = []
        for md_i in md_instances:
            if identifier_only:
                md_i = deepcopy(md_i)
                self._cleanup_attrs_without_identifier(md_i)
            conv = DgraphInstanceConverter(self.mappings, node_form=node_form, action=action)
            conv.input = md_i
            dgraph_instances.extend(conv.output)
        return dgraph_instances

    @staticmethod
    def _cleanup_attrs_without_identifier(md):
        for attr_def in attr.fields(md.__class__):
            if attr_def is not md.metadata['identifier']:
                setattr(md, attr_def.name, Empty)

    def replace_with_existed_uids(self, dgraph_instances):
        """
        替换已存在的实例的uid

        :param dgraph_instances: dgraph实例列表
        :return:
        """
        if not dgraph_instances:
            return
        full_identifiers = dict()
        # 初始化需要检查的节点映射关系列表
        for dg_i in dgraph_instances:
            if isinstance(dg_i.subject, Identifier):
                self._set_identifier_value_uid_mapping(full_identifiers, dg_i.subject, None)
            if isinstance(dg_i.object_, Identifier):
                self._set_identifier_value_uid_mapping(full_identifiers, dg_i.object_, None)
        existed_uids = self._get_existed_uids(
            {
                predicate_name: list(value_mapping.keys())
                for predicate_name, value_mapping in list(full_identifiers.items())
            }
        )
        # 将检查的结果回填到映射列表中
        for predicate_name, value_mapping in list(full_identifiers.items()):
            ret_uid_list = existed_uids[predicate_name]
            for item in ret_uid_list:
                if item['id_v'] in value_mapping:
                    value_mapping[item['id_v']] = item['uid']
        # 根据检查结果将已存在的Identifier替换为Uid类型的节点
        for dg_i in dgraph_instances:
            if isinstance(dg_i.subject, Identifier):
                existed_uid = self._get_identifier_value_uid_mapping(full_identifiers, dg_i.subject)
                if existed_uid:
                    dg_i.subject = Uid(existed_uid)
            if isinstance(dg_i.object_, Identifier):
                existed_uid = self._get_identifier_value_uid_mapping(full_identifiers, dg_i.object_)
                if existed_uid:
                    dg_i.object_ = Uid(existed_uid)

    def _get_identifier_value_uid_mapping(self, mapping_container, identifier_node):
        """
        获取节点主键key-value对

        :param dict mapping_container: mapping所在的字典空间
        :param Identifier identifier_node: dgraph节点实例
        :return: mixed 已存在节点对应的uid值或None
        """

        # Internal(as_none)是空节点，通过cached_property方法获取uid
        if identifier_node.md_type is Internal and identifier_node.identifier_value == 'as_none' and self.backend:
            none_node = self.backend.none_node
            return none_node.uid
        identifier_dgraph_type = self.mappings[
            (identifier_node.md_type, identifier_node.md_type.metadata['identifier'])
        ]
        predicate_name = identifier_dgraph_type.predicate.name
        identifier_value = identifier_node.identifier_value
        return mapping_container.get(predicate_name, {}).get(identifier_value, None)

    def _set_identifier_value_uid_mapping(self, mapping_container, identifier_node, uid_val):
        """
        设置主键value-uid mapping字典的值

        :param dict mapping_container: mapping所在的字典空间
        :param Identifier identifier_node: dgraph节点实例
        :param mixed uid_val: 要设置的uid值(string or None)
        :return: boolean
        """

        # Internal(as_none)是空节点，跳过
        if identifier_node.md_type is Internal and identifier_node.identifier_value == 'as_none':
            return False
        identifier_dgraph_type = self.mappings[
            (identifier_node.md_type, identifier_node.md_type.metadata['identifier'])
        ]
        predicate_name = identifier_dgraph_type.predicate.name
        identifier_value = identifier_node.identifier_value
        if predicate_name not in mapping_container:
            mapping_container[predicate_name] = dict()
        mapping_container[predicate_name][identifier_value] = uid_val
        return True

    def _get_existed_uids(self, query_contents):
        """
        查询已存在节点的uid值

        :param: dict query_contents 待查询的节点信息
        :return: list uid列表
        """
        query_contents = {
            predicate_name: json.dumps(identifier_values, ensure_ascii=False).strip('[').strip(']')
            for predicate_name, identifier_values in list(query_contents.items())
        }
        ret = self.query(self.uid_access_template.render(query_contents=query_contents))
        return ret['data'] if ret['data'] else []

    def __enter__(self):
        self.session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 兼容
        dgraph_ver = getattr(self.backend, str('dgraph_ver'), None)
        start_ts_on_url = True
        if version_compare(dgraph_ver, '1.1.0') >= 0:
            start_ts_on_url = False
        self.session.__exit__(exc_type, exc_val, exc_tb, start_ts_on_url=start_ts_on_url)


class DgraphTypeConverter(object):
    """
    Dgraph类型转换器。 input填充标准类型。output生成dgraph类型和映射关系。
    """

    type_mapping = OrderedDict(
        (
            (str, 'string'),
            (str, 'string'),
            (bool, 'bool'),
            (int, 'int'),
            (int, 'int'),
            (float, 'float'),
            (datetime, 'dateTime'),
            (MetaData, 'uid'),
        )
    )
    string_index = ['hash', 'exact', 'term', 'fulltext', 'trigram']
    default_indexes = {'string': ['exact'], 'int': ['int'], 'float': ['float'], 'dateTime': ['hour'], 'bool': ['bool']}

    def __init__(self):
        self.input = None

    @cached_property
    def output(self):
        """
        生成dgraph类型和映射关系。

        :return: 生成dgraph类型，映射关系。
        """
        types, mappings = [], {}
        if not (issubclass(self.input, MetaData) and hasattr(self.input, '__attrs_attrs__')):
            raise DgraphDefConvertError(_('Not standard metadata type.'))
        for attr_def in attr.fields(self.input):
            per_def = self.gen_dgraph_def(*self.parse_attr_def(attr_def, self.input))
            per_type = type(
                str('Dgraph{}.{}'.format(self.input.__name__, snake_to_camel(per_def.name, True))),
                (DgraphMetaData,),
                {str('predicate'): per_def},
            )
            mappings[(self.input, attr_def)] = per_type
            types.append(per_type)
        return types, mappings

    def parse_attr_def(self, attr_def, md_type):
        """
        解析标准类型的每个attr定义。

        :param md_type: 标准类型
        :param attr_def: attr定义。
        :return: 生成dgraph定义的参数
        """
        class_name = self.input.__name__
        attr_name = attr_def.name
        attr_type = attr_def.type
        if issubclass(attr_type, List):
            is_list = True
            primitive_attr_type = attr_type.__args__[0]
        else:
            is_list = False
            primitive_attr_type = attr_type
        for k, v in self.type_mapping.items():
            if issubclass(primitive_attr_type, k):
                primitive_dgraph_type = v
                break
        else:
            raise DgraphDefConvertError(_("Can't find dgraph type in mapping."))
        dgraph_type = primitive_dgraph_type
        dgraph_metadata = attr_def.metadata.get('dgraph', {})
        # count索引-仅根据元模型是否设置count:True决定,默认False
        if_count = dgraph_metadata.get('count', False)
        # upsert索引-仅根据元模型是否设置upsert:True决定,默认False
        if_upsert = dgraph_metadata.get('upsert', False)
        # reverse索引-属性默认为False,边自动添加
        if_reverse = dgraph_metadata.get('reverse', False if not issubclass(primitive_attr_type, MetaData) else True)
        predicate_name = dgraph_metadata.get('predicate_name', None)
        predicate_name = predicate_name if predicate_name else '.'.join([class_name, attr_name])
        as_identifier = True if attr_def is md_type.metadata['identifier'] else False
        default_index = (
            self.default_indexes.get(primitive_dgraph_type, None)
            if (md_type.metadata.get('dgraph', {}).get('auto_index', False) or as_identifier)
            else None
        )
        index = dgraph_metadata.get('index', default_index)
        return predicate_name, dgraph_type, index, if_count, if_upsert, if_reverse, as_identifier, is_list

    @staticmethod
    def gen_dgraph_def(predicate_name, dgraph_type, index, if_count, if_upsert, if_reverse, as_identifier, is_list):
        """
        使用参数生成dgraph定义

        :param predicate_name: 谓词名
        :param dgraph_type: 数据库类型
        :param index: 是否索引及索引方式
        :param if_count: 是否创建count索引(用于将边/属性的数量作为索引值，进行初始查询的比较)
        :param if_upsert: 边类型是否要求唯一
        :param if_reverse： 是否可反向查询
        :param as_identifier: 是否是主键
        :param is_list: 是否是list(属性列表/一对多关联的uid列表)
        :return: dgraph定义
        """
        dgraph_def = DgraphDef(name=predicate_name, type=dgraph_type)
        if index:
            dgraph_def.directives.append(('index', index))
        if if_count:
            dgraph_def.directives.append('count')
        if if_upsert:
            dgraph_def.directives.append('upsert')
        if if_reverse:
            dgraph_def.directives.append('reverse')
        if as_identifier:
            dgraph_def.metadata['as_identifier'] = True
        if is_list:
            dgraph_def.is_list = True
        return dgraph_def


@singledispatch
def convert_dgraph_primitive_value(value):
    return value


@convert_dgraph_primitive_value.register(datetime)
def _convert_datetime(value):
    return arrow.get(value).isoformat()


@convert_dgraph_primitive_value.register(bool)
def _convert_bool(value):
    return json.dumps(value)


class DgraphInstanceConverter(object):
    """
    Dgraph实例转换器。 input填充标准实例。output生成dgraph实例。
    """

    default_blank_value = {
        'string': '',
        'int': 0,
        'float': 0.0,
        'dateTime': convert_dgraph_primitive_value(arrow.get(0).datetime),
        'bool': convert_dgraph_primitive_value(False),
        'uid': Internal(role='as_none'),
    }

    def __init__(self, mappings, node_form='blank', action='set'):
        """
        :param mappings: Dgraph类型和标准类型映射关系
        :param node_form: 实例描述的node形式[blank: _:tag; uid: <0x123>; var: uid(tag)]
        :param action: 转换之后的实例用于进行的操作,根据操作不同，产出实例的属性会发生变化[set: 设置; delete: 删除]
        """
        self.input = None
        self.mappings = mappings
        self.node_form = node_form
        self.action = action

    @cached_property
    def output(self):
        """
        :return: dgraph rdf 实例。
        """
        if not isinstance(self.input, (MetaData, ReferredMetaData)):
            raise DgraphInstanceConvertError(_('Not standard metadata instance.'))
        dgraph_instances = []
        instance_type = gen_instance_type(self.input)

        for attr_def in attr.fields(instance_type):
            attr_value = getattr(self.input, attr_def.name, Empty)
            attr_value = '' if attr_def.name == 'typed' and self.action == 'delete' else attr_value
            if attr_value is not Empty:
                attr_type = attr_def.type
                is_list, attr_primitive_type = parse_list_type(attr_type)
                if is_list:
                    attr_value_lst = attr_value if attr_value is not None else [None]
                    if Empty in attr_value_lst:
                        continue
                else:
                    attr_value_lst = [attr_value]
                for per_attr_value in attr_value_lst:
                    dgraph_instance = self.gen_dgraph_instances(instance_type, attr_def, per_attr_value)
                    dgraph_instances.append(dgraph_instance)

        return dgraph_instances

    def gen_dgraph_instances(self, instance_type, attr_def, attr_value):
        """
        创建dgraph rdf 实例
        :param instance_type: 类型系统实例
        :param attr_def: 元模型属性定义
        :param attr_value: 单个元数据属性值
        :return:
        """
        dgraph_type = self.mappings[(instance_type, attr_def)]
        subject = self.gen_node_info(self.input, self.node_form, 'subject')
        # 对模型实例中缺少的属性设置默认值
        if attr_value is None:
            attr_value = self.default_blank_value[dgraph_type.predicate.type]
        # 不管是set还是delete, 对uid关联只能单个操作
        if dgraph_type.predicate.type == 'uid':
            object_ = (
                self.gen_node_info(attr_value, self.node_form, 'o_' + dgraph_type.predicate.name)
                if not isinstance(attr_value, DgraphNode)
                else attr_value
            )
        else:
            object_ = (
                convert_dgraph_primitive_value(attr_value)
                if (self.action == 'set' or dgraph_type.predicate.metadata.get('as_identifier', False))
                else Asterisk()
            )
        dgraph_instance = dgraph_type(subject=subject, object_=object_)
        return dgraph_instance

    @staticmethod
    def gen_node_info(instance, form='blank', tag=''):
        """
        获取节点定位信息

        :param instance: 标准实例。
        :param form: 节点形式
        :param tag: 节点标签(别名用)
        :return: 节点定位信息。
        """
        instance_type = gen_instance_type(instance)
        identifier_attr = instance_type.metadata['identifier']
        identifier_value = getattr(instance, identifier_attr.name)
        if form == 'uid':
            return Uid(uid=identifier_value)
        if form == 'var':
            alias = tag if tag else identifier_attr.name
            return UidVariable(md_type=instance_type, identifier_value=identifier_value, alias=alias)
        return Identifier(md_type=instance_type, identifier_value=identifier_value)
