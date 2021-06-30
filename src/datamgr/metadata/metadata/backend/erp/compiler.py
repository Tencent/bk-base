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

import attr
import cattr

from metadata.backend.erp.functions import (
    common_function_registry,
    complex_function_registry,
)
from metadata.backend.interface import BackendType
from metadata.exc import ERPParamsError
from metadata.runtime import rt_local
from metadata.type_system.core import MetaData, parse_list_type
from metadata.util.common import camel_to_snake
from metadata.util.i18n import lazy_selfish as _

erp_compiler_registry = {}
"""
erp编译器容器
"""


def erp_compiler_register():
    """
    erp编译器注册容器

    :return: function
    """

    def _inner(cls):
        if cls.backend_type:
            erp_compiler_registry[cls.backend_type] = cls
        return cls

    return _inner


@attr.s
class StatementParams(object):
    """
    表达式参数容器
    """

    attributes = attr.ib(factory=list)
    start_filter_expression = attr.ib(default=None)
    filter_expressions = attr.ib(factory=list)
    paging = attr.ib(default=None)
    count = attr.ib(default=None)
    order_by = attr.ib(default=None)


@attr.s
class DgraphStatementParams(StatementParams):
    """
    dgraph表达式参数容器
    """

    cascade = attr.ib(default=None)


@attr.s
class MysqlStatementParams(StatementParams):
    """
    mysql表达式参数容器
    """

    pass


class Compiler(object):
    """
    erp编译基类
    """

    ee_tree = None
    """
    ee_tree means erp_expression_tree
    """

    def __init__(self, ee_tree=None):
        self.ee_tree = ee_tree

    def reload_statements(self):
        pass

    def pre_compile(self):
        function_registry = complex_function_registry.copy()
        function_registry.update(common_function_registry)
        for pre_func_name, item in list(rt_local.erp_tree_root.variables.parsed_info.items()):
            if pre_func_name in function_registry:
                batch_func = function_registry[pre_func_name]
                batch_obj = batch_func()
                batch_obj.pre_compile(item, self)

    def compile(self):
        """
        解析erp语法树，得到实际执行的backend语句

        :return: None
        """
        self.compile_common_attributes()
        self.compile_linked_attributes()
        self.compile_common_functions()
        self.compile_complex_functions()
        self.append_primary_key_value()

    def compile_common_attributes(self):
        """
        构建普通属性表达式
        """
        raise NotImplementedError

    def compile_linked_attributes(self):
        """
        构建关联属性表达式
        """
        raise NotImplementedError

    def compile_common_functions(self):
        """
        构建通用函数表达式
        """
        raise NotImplementedError

    def compile_complex_functions(self):
        """
        构建自定义函数表达式
        """
        raise NotImplementedError

    def append_primary_key_value(self):
        """
        附带主键表达式
        """
        raise NotImplementedError


@erp_compiler_register()
class DgraphCompiler(Compiler):

    backend_type = BackendType.DGRAPH

    symbol_mapping = {'=': 'eq', '>': 'gt', '<': 'lt', '>=': 'ge', '<=': 'le', '*=': 'regexp'}
    """
    erp命令符和实际执行表达式指令的关系
    """

    def __init__(self, *args, **kwargs):
        super(DgraphCompiler, self).__init__(*args, **kwargs)
        if self.ee_tree:
            self.ee_tree.statements = self.reload_statements()
            self.ee_tree.backend_partition['dgraph.type'] = self.ee_tree.typed.__name__

    def reload_statements(self):
        statements_params = cattr.unstructure(self.ee_tree.statements)
        return DgraphStatementParams(**statements_params)

    def compile_common_attributes(self):
        for key, parsed_dict in list(self.ee_tree.common_attributes.items()):
            if not parsed_dict['attr_val']:
                continue
            md_type = parsed_dict['md_type']
            alias_key = parsed_dict['alias_key']
            attr_key = self.compile_to_rdf_attr_name(key, md_type)
            compile_result = self.compile_predicate_expression(alias_key, attr_key)
            self.add_to_statements('attributes', compile_result, set_append=True)

    def compile_linked_attributes(self):
        for key, link_parsed_dict in list(self.ee_tree.linked_attributes.items()):
            md_type = link_parsed_dict['md_type']
            alias_key = link_parsed_dict['alias_key']
            reverse = link_parsed_dict['reverse']
            attr_key = alias_key if reverse else self.compile_to_rdf_attr_name(key, md_type)
            compiled_key = self.compile_predicate_expression(alias_key, attr_key)
            self.ee_tree.linked_attributes[compiled_key] = self.ee_tree.linked_attributes.pop(key)
            sub_erp_expression_tree = link_parsed_dict['sub_tree']
            compiler = DgraphCompiler(sub_erp_expression_tree)
            compiler.compile()

    def compile_common_functions(self):
        for func_name in attr.fields_dict(self.ee_tree.common_functions.__class__):
            v = getattr(self.ee_tree.common_functions, func_name)
            if func_name in common_function_registry:
                func_obj = common_function_registry[func_name]()
                func_obj.compile(v, self)

    def compile_complex_functions(self):
        for func_name in self.ee_tree.complex_functions:
            if func_name in complex_function_registry:
                func_obj = complex_function_registry[func_name]()
                func_obj.compile(self.ee_tree.complex_functions[func_name], self)

    def append_primary_key_value(self):
        """
        若当前字段是模型主键，自动附带主键表达式

        :return: None
        """
        md_typed = self.ee_tree.typed
        if 'identifier' in md_typed.metadata:
            compile_result = 'identifier_value: {md_type}.{primary_key}'.format(
                md_type=md_typed.__name__, primary_key=md_typed.metadata['identifier'].name
            )
            self.add_to_statements('attributes', compile_result, set_append=True)

    def add_to_statements(self, statement_key, statement_expr, set_append=False, target_tree=None):
        """
        将解析结果添加到解析树的表达式容器中

        :param statement_key: string 填入key值
        :param statement_expr: string 填入内容
        :param set_append: boolean 是否为可迭代追加填入方式
        :param target_tree: object 指定设置表达式的树
        :return: boolean True/False
        """

        target_tree = target_tree if target_tree else self.ee_tree
        if set_append:
            getattr(target_tree.statements, statement_key).append(statement_expr)
        else:
            setattr(target_tree.statements, statement_key, statement_expr)

    @staticmethod
    def compile_to_rdf_attr_name(k, md_type):
        return (
            '{md_name}.{attr_name}'.format(md_name=md_type.__name__, attr_name=k)
            if k not in md_type.__metadata__.get('dgraph', {}).get('common_predicates', {})
            else k
        )

    @staticmethod
    def compile_predicate_expression(alias_key, attr_key):
        return (
            alias_key
            if attr_key == alias_key
            else '{alias_key}: {attr_key}'.format(alias_key=alias_key, attr_key=attr_key)
        )

    def compile_condition_expression(self, member):
        key = member.key.strip()
        fields_dict = attr.fields_dict(self.ee_tree.typed)
        if key not in fields_dict or issubclass(parse_list_type(fields_dict[key].type)[1], MetaData):
            raise ERPParamsError(_('The attribute {} is not found on type.'.format(key)))
        if getattr(member, '_tx_fqn', None) == 'boolean.InConditionExpression':
            ret = 'eq({}, {})'.format(
                self.compile_to_rdf_attr_name(key, self.ee_tree.typed),
                json.dumps([str(item) for item in member.value.values]),
            )
        elif getattr(member, '_tx_fqn', None) == 'boolean.BoolConditionExpression':
            ret = '{}({}, {})'.format(
                self.symbol_mapping[member.symbol],
                self.compile_to_rdf_attr_name(key, self.ee_tree.typed),
                '"{}"'.format(member.value) if member.symbol != '*=' else '/{}/'.format(member.value),
            )
        else:
            raise ERPParamsError(_('Invalid condition expression.'))
        return ret

    def compile_bool_expression(self, member):
        fqn = getattr(member, '_tx_fqn', None)
        if fqn == 'boolean.BoolExpression':
            return self.compile_bool_expression(member.expression)
        elif fqn == 'boolean.Or':
            return ' or '.join(self.compile_bool_expression(son_member) for son_member in member.op)
        elif fqn == 'boolean.And':
            return ' and '.join(self.compile_bool_expression(son_member) for son_member in member.op)
        else:
            return self.compile_condition_expression(member)


@erp_compiler_register()
class MysqlCompiler(Compiler):

    JOIN_DEPTH_LIMIT = 1

    backend_type = BackendType.MYSQL

    symbol_mapping = {'=': '=', '>': '>', '<': '<', '>=': '>=', '<=': '<=', '*=': 'like'}

    def __init__(self, depth=0, *args, **kwargs):
        super(MysqlCompiler, self).__init__(*args, **kwargs)
        if self.ee_tree:
            self.ee_tree.statements = self.reload_statements()
            self.ee_tree.backend_partition['db_name'] = 'bkdata_meta'
            self.ee_tree.backend_partition['table_name'] = camel_to_snake(self.ee_tree.typed.__name__)
        self.depth = depth

    def reload_statements(self):
        statements_params = cattr.unstructure(self.ee_tree.statements)
        return MysqlStatementParams(**statements_params)

    def compile_common_attributes(self):
        for key, parsed_dict in list(self.ee_tree.common_attributes.items()):
            val = parsed_dict['attr_val']
            if not val:
                continue
            compile_result = '{attr_key} AS {ret_key}'.format(
                attr_key=self.compile_to_mysql_column_fqn(key),
                ret_key=self.compile_to_mysql_column_fqn(key, con_str='__'),
            )
            self.add_to_statements('attributes', compile_result, set_append=True)

    def compile_linked_attributes(self):
        """
        用join实现mysql的关联查询，限制join深度

        :return: None
        """
        if self.depth >= self.JOIN_DEPTH_LIMIT:
            return
        for key, link_parsed_dict in list(self.ee_tree.linked_attributes.items()):
            sub_erp_expression_tree = link_parsed_dict['sub_tree']
            compiler = MysqlCompiler(ee_tree=sub_erp_expression_tree, depth=self.depth + 1)
            compiler.compile()

    def compile_common_functions(self):
        for func_name in attr.fields_dict(self.ee_tree.common_functions.__class__):
            v = getattr(self.ee_tree.common_functions, func_name)
            if func_name in common_function_registry:
                func_obj = common_function_registry[func_name]()
                func_obj.compile(v, self)

    def compile_complex_functions(self):
        for func_name in self.ee_tree.complex_functions:
            if func_name in complex_function_registry:
                func_obj = complex_function_registry[func_name]()
                func_obj.compile(self.ee_tree.complex_functions[func_name], self)

    def append_primary_key_value(self):
        """
        若当前字段是模型主键，自动附带主键表达式

        :return: None
        """
        md_typed = self.ee_tree.typed
        if 'identifier' in md_typed.metadata:
            compile_result = '{attr_key} AS {ret_key}'.format(
                attr_key=self.compile_to_mysql_column_fqn(md_typed.metadata['identifier'].name),
                ret_key=self.compile_to_mysql_column_fqn('identifier_value', con_str='__'),
            )
            self.add_to_statements('attributes', compile_result, set_append=True)

    def add_to_statements(self, statement_key, statement_expr, set_append=False, target_tree=None):
        """
        将解析结果添加到解析树的表达式容器中

        :param statement_key: string 填入key值
        :param statement_expr: string 填入内容
        :param set_append: boolean 是否为可迭代追加填入方式
        :param target_tree: object 指定设置表达式的树
        :return: boolean True/False
        """

        target_tree = target_tree if target_tree else self.ee_tree
        if set_append:
            getattr(target_tree.statements, statement_key).append(statement_expr)
        else:
            setattr(target_tree.statements, statement_key, statement_expr)

    def compile_to_mysql_column_fqn(self, k, con_str='.'):
        return '{table_name}{con_str}{attr_name}'.format(
            table_name=self.ee_tree.backend_partition['table_name'], con_str=con_str, attr_name=k
        )

    def compile_condition_expression(self, member):
        key = member.key.strip()
        fields_dict = attr.fields_dict(self.ee_tree.typed)
        if key not in fields_dict or issubclass(parse_list_type(fields_dict[key].type)[1], MetaData):
            raise ERPParamsError(_('The attribute {} is not found on type.'.format(key)))
        if getattr(member, '_tx_fqn', None) == 'boolean.InConditionExpression':
            ret = '{} IN ({})'.format(
                self.compile_to_mysql_column_fqn(key),
                ','.join(
                    '"{}"'.format(str(item)) if isinstance(item, (str, bytes)) else str(item)
                    for item in member.value.values
                ),
            )
        elif getattr(member, '_tx_fqn', None) == 'boolean.BoolConditionExpression':
            ret = '{} {} {}'.format(
                self.compile_to_mysql_column_fqn(key),
                self.symbol_mapping[member.symbol],
                '"%{}%"'.format(str(member.value))
                if member.symbol == '*='
                else '{}'.format(
                    '"{}"'.format(str(member.value)) if isinstance(member.value, (str, bytes)) else str(member.value)
                ),
            )
        else:
            raise ERPParamsError(_('Invalid condition expression.'))
        return ret

    def compile_bool_expression(self, member):
        fqn = getattr(member, '_tx_fqn', None)
        if fqn == 'boolean.BoolExpression':
            return self.compile_bool_expression(member.expression)
        elif fqn == 'boolean.Or':
            return ' OR '.join(self.compile_bool_expression(son_member) for son_member in member.op)
        elif fqn == 'boolean.And':
            return ' AND '.join(self.compile_bool_expression(son_member) for son_member in member.op)
        else:
            return self.compile_condition_expression(member)
