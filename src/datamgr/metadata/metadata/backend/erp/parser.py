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

from collections import defaultdict

import attr

from metadata.backend.erp.compiler import StatementParams
from metadata.backend.erp.functions import (
    common_function_registry,
    complex_function_registry,
)
from metadata.backend.interface import BackendType
from metadata.exc import ERPError
from metadata.runtime import rt_context
from metadata.type_system.core import MetaData, parse_list_type
from metadata.util.i18n import lazy_selfish as _


def md_typed_converter(typed):
    """
    元模型类型转换

    :param typed: string 模型类名
    :return: object
    Todo: 实现两种类型
    """
    if isinstance(typed, str) or not typed:
        return rt_context.md_types_registry.get(typed, None)
    else:
        return typed


@attr.s
class CommonFunctions(object):
    """
    Erp 公共函数语法要素
    """

    wild_card = attr.ib(default=False)
    start_filter = attr.ib(default=None)
    filter = attr.ib(default=None)
    paging = attr.ib(default=None)
    count = attr.ib(default=None)
    order_by = attr.ib(default=None)
    cascade = attr.ib(default=None)


@attr.s
class ErpGlobalVariables(object):
    """
    erp 顶层全局变量设置
    """

    parsed_info = attr.ib(factory=lambda: defaultdict(dict))  # erp解析关键字字典
    statements = attr.ib(factory=list)  # 可执行表达式列表


@attr.s
class ErpExpressionParseTree(object):
    """
    erp查询表达式解析树(某一层)
    """

    members = attr.ib()  # erp语法成分
    backend_type_name = attr.ib(default=BackendType.DGRAPH.value)  # erp目标后端(默认dgraph)
    expression_name = attr.ib(default=None)  # erp表达式
    is_start = attr.ib(default=False)  # 驱动查询标志
    typed = attr.ib(default=None, converter=md_typed_converter)  # 元模型类型
    backend_partition = attr.ib(factory=dict)  # 对应后端的实际分区信息(数据库表/dgraph类型系统)
    common_attributes = attr.ib(factory=dict)  # 普通属性
    linked_attributes = attr.ib(factory=dict)  # 关联属性
    common_functions = attr.ib(factory=CommonFunctions)  # 通用函数
    complex_functions = attr.ib(factory=lambda: defaultdict(list))  # 自定义函数
    statements = attr.ib(factory=lambda: StatementParams())  # 每一层erp解析树的解析表达式结果

    def __attrs_post_init__(self):
        """
        __init__后自动执行，初始化类

        :return: None
        """
        self.parse_directives()
        self.parse_attributes()

    def parse_directives(self):
        """
        解析指令

        :return: None
        """
        for member in self.members:
            # 解析类型指定指令
            if self.is_start and getattr(member, '_tx_fqn', None) == 'erp.TypedMember':
                self.typed = md_typed_converter(member.value)
            # 设置查询别名
            elif self.is_start and getattr(member, '_tx_fqn', None) == 'erp.ExpressionName':
                self.expression_name = member.value
            # 设置目标解析后端
            elif self.is_start and getattr(member, '_tx_fqn', None) == 'erp.BackendMember':
                if member.value not in BackendType._value2member_map_:
                    raise ERPError(_('{} type is not a valid backend_type.'.format(member.value)))
                self.backend_type_name = member.value
            # 解析公共方法指令
            elif getattr(member, '_tx_fqn', None) == 'erp.CommonFunctionMember':
                func_name = member.actual_member.key.lstrip('"').rstrip('"').lstrip('?:')
                if func_name in common_function_registry:
                    obj = common_function_registry[func_name]()
                    obj.parse(member.actual_member, self)
            # 解析自定义方法指令
            elif getattr(member, '_tx_fqn', None) == 'erp.ComplexFunctionMember':
                func_name = member.function_name
                if func_name in complex_function_registry:
                    obj = complex_function_registry[func_name]()
                    obj.parse(member, self)
        if not self.typed:
            raise ERPError(_('Md type is not existed.'))
        if not self.expression_name:
            self.expression_name = self.typed.__name__

    def parse_attributes(self):
        """
        解析属性

        :return: None
        """
        md_type = self.typed
        attr_defs = attr.fields_dict(md_type)
        # *直接解析全部非关联属性
        if self.common_functions.wild_card:
            attributes = {
                attr_def.name: True
                for attr_def in list(attr_defs.values())
                if not issubclass(parse_list_type(attr_def.type)[1], MetaData)  # 过滤关联，只处理当期那节点的属性
                and attr_def.name not in ['typed']
            }  # typed字段不处理
            for k, v in list(attributes.items()):
                self.parse_common_attributes(k, v, md_type)
        # 依次解析属性，包括关联和反向关联的属性要求
        for member in self.members:
            if getattr(member, '_tx_fqn', None) == 'erp.AttributeMember':
                k, v = member.key, member.value
                # 非当前typed模型域下的其他字段解析(虚拟大类型下的属性解析)
                if k.startswith('_:'):
                    self.parse_multi_target_attributes(k, v, md_type)
                # 反向关联解析
                elif k.startswith('~'):
                    actual_reserve_md_name, attr_name = k.lstrip('~').split('.')
                    actual_reserve_md_type = md_typed_converter(actual_reserve_md_name)
                    self.parse_reverse_linked_attributes(
                        k, v, md_type=md_type, reverse_linked_by=actual_reserve_md_type
                    )
                # 当前typed模型域下的字段解析
                elif k in attr_defs:
                    is_list, ib_primitive_type = parse_list_type(attr_defs[k].type)
                    if not issubclass(ib_primitive_type, MetaData):
                        self.parse_common_attributes(k, v, md_type)
                    else:
                        self.parse_linked_attributes(k, v, md_type, ib_primitive_type)
                else:
                    raise ERPError(_('Not existed attr name {}'.format(k)))

    def parse_multi_target_attributes(self, k, v, md_type):
        direct_attr_name = k.split('_:')[1]
        if direct_attr_name.startswith('~'):
            actual_reserve_md_name, attr_name = direct_attr_name.lstrip('~').split('.')
            actual_reserve_md_type = md_typed_converter(actual_reserve_md_name)
            if actual_reserve_md_type:
                self.parse_reverse_linked_attributes(
                    direct_attr_name, v, md_type=md_type, reverse_linked_by=actual_reserve_md_type
                )
            else:
                raise ERPError('Not existed md type ({}).'.format(actual_reserve_md_name))
        else:
            actual_md_name, attr_name = direct_attr_name.split('.')
            actual_metadata_type = md_typed_converter(actual_md_name)
            attr_defs = attr.fields_dict(actual_metadata_type)
            if actual_metadata_type and issubclass(actual_metadata_type, md_type):
                if attr_name in attr_defs:
                    is_list, ib_primitive_type = parse_list_type(attr_defs[attr_name].type)
                    if not issubclass(ib_primitive_type, MetaData):
                        self.parse_common_attributes(attr_name, v, actual_metadata_type)
                    else:
                        self.parse_linked_attributes(attr_name, v, actual_metadata_type, ib_primitive_type)
                else:
                    raise ERPError('Not existed attr ({}) on class ({}).'.format(attr_name, actual_metadata_type))
            else:
                raise ERPError('Class ({}) is not sub class of ({}).'.format(actual_metadata_type, md_type))

    def parse_common_attributes(self, k, v, md_type):
        """
        解析属性

        :param k:
        :param v:
        :param md_type:
        :return:
        """
        if v not in (True, 'true', 'True'):
            raise ERPError('Invalid value for common attr {}.'.format(k))
        self.common_attributes[k] = dict(attr_val=v, md_type=md_type, alias_key=k)

    def parse_linked_attributes(self, k, v, md_type, ib_primitive_type):
        """
        解析关联属性

        :param k:
        :param v:
        :param md_type:
        :param ib_primitive_type:
        :return:
        """
        if v in (True, 'true', 'True'):
            sub_tree = ErpExpressionParseTree(
                backend_type_name=self.backend_type_name,
                typed=ib_primitive_type.agent,
                members=[],
                common_functions=CommonFunctions(wild_card=True),
            )
        elif getattr(v, '_tx_fqn', None) == 'erp.Object':
            sub_tree = ErpExpressionParseTree(
                backend_type_name=self.backend_type_name, typed=ib_primitive_type.agent, members=v.members
            )
        else:
            raise ERPError('Invalid value for linked attr {}.'.format(k))
        self.linked_attributes[k] = dict(sub_tree=sub_tree, md_type=md_type, alias_key=k, reverse=False)

    def parse_reverse_linked_attributes(self, k, v, md_type=None, reverse_linked_by=None):
        """
        解析反向关联

        :param k:
        :param v:
        :param md_type:
        :param reverse_linked_by:
        :return:
        """
        reverse_md_type = (
            md_type.metadata['dgraph']['reverse_edges'][k.split('~')[1]] if not reverse_linked_by else reverse_linked_by
        )
        if v is True:
            sub_tree = ErpExpressionParseTree(
                backend_type_name=self.backend_type_name,
                typed=reverse_md_type,
                members=[],
                common_functions=CommonFunctions(wild_card=True),
            )
        elif getattr(v, '_tx_fqn', None) == 'erp.Object':
            sub_tree = ErpExpressionParseTree(
                backend_type_name=self.backend_type_name, typed=reverse_md_type, members=v.members
            )
        else:
            raise ERPError('Invalid value for linked attr {}.'.format(k))
        self.linked_attributes[k] = dict(sub_tree=sub_tree, md_type=md_type, alias_key=k, reverse=True)
