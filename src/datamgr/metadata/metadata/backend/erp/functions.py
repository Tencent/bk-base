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

import logging
import uuid
from enum import Enum

import attr
import cattr
from attr.validators import in_
from retry import retry

from metadata.exc import ERPError, ERPParamsError
from metadata.runtime import rt_context, rt_local
from metadata.type_system.core import MetaData, parse_list_type
from metadata.util.common import StrictABCMeta, camel_to_snake, error_adapt_handler
from metadata.util.i18n import lazy_selfish as _

module_logger = logging.getLogger(__name__)

complex_function_registry = {}
common_function_registry = {}


def register_function(registry=None):
    """
    函数注册装饰器

    :param registry: 注册容器
    :return: function
    """

    def _inner(cls):
        if not getattr(cls, 'function_name', None):
            raise ERPError('Fail to get ERP function name.')
        if registry is None:
            raise ERPError('Fail to ser registry.')
        registry[cls.function_name] = cls
        return cls

    return _inner


class FunctionMode(Enum):
    NORMAL = 'normal'
    PRE_COMPILE = 'pre_compile'


class ErpFunction(object, metaclass=StrictABCMeta):
    compile_mode = FunctionMode.NORMAL
    """
    函数模式
    - FunctionMode.NORMAL: 常规编译
    - FunctionMode.PRE_COMPILE:  需要预编译
    """

    tree_function_store_attr = None
    """
    函数存储在树中的属性名
    """

    function_name = None
    """
    函数名
    """

    statement_name = None
    """
    表达式名称
    """

    def __init__(self, *args, **kwargs):
        self.normal_conf = rt_context.config_collection.normal_config

    def parse(self, member, ee_tree):
        return self.inner_parse(member, ee_tree)

    def inner_parse(self, member, ee_tree):
        return member.value

    def compile(self, compile_input, compiler):
        compile_func_name = getattr(self, 'compile_to_{}'.format(compiler.backend_type.value), None)
        if not compile_func_name:
            # Todo: 未找到可用的编译方法
            raise
        compile_func_name(compile_input, compiler)

    def pre_compile(self, compile_input, compiler):
        pre_compile_func_name = getattr(self, 'pre_compile_to_{}'.format(compiler.backend_type.value), None)
        if not pre_compile_func_name:
            # Todo: 未找到可用的预编译方法
            raise
        pre_compile_func_name(compile_input, compiler)


class CommonFunction(ErpFunction):
    """
    公共函数基类
    """

    tree_function_store_attr = 'common_functions'

    def parse(self, member, ee_tree):
        parsed_info = self.inner_parse(member, ee_tree)
        setattr(getattr(ee_tree, self.tree_function_store_attr), self.function_name, parsed_info)


class ComplexFunction(ErpFunction):
    """
    自定义函数基类
    """

    tree_function_store_attr = 'complex_functions'

    def parse(self, member, ee_tree):
        parsed_info = self.inner_parse(member, ee_tree)
        if member.function_name in getattr(ee_tree, self.tree_function_store_attr):
            raise ERPError('Duplicate function.')
        # 预编译, 解析关键字先存到顶层变量集合
        if self.compile_mode == FunctionMode.PRE_COMPILE:
            rt_local.erp_tree_root.variables.parsed_info[self.function_name][parsed_info] = ee_tree
            getattr(ee_tree, self.tree_function_store_attr)[member.function_name].append(parsed_info)
        else:
            getattr(ee_tree, self.tree_function_store_attr)[member.function_name] = parsed_info


@register_function(common_function_registry)
class Filter(CommonFunction):
    function_name = 'filter'
    statement_name = 'filter_expressions'

    def inner_parse(self, member, ee_tree):
        return rt_local.erp_query_now.boolean_exp_meta.model_from_str(member.value)

    def compile(self, compile_input, compiler):
        if compile_input:
            super(Filter, self).compile(compile_input, compiler)

    def compile_to_dgraph(self, compile_input, compiler):
        compile_result = compiler.compile_bool_expression(compile_input)
        if not compile_result:
            raise ERPError('Fail to compile filter.')
        compiler.add_to_statements(self.statement_name, compile_result, set_append=True)

    def compile_to_mysql(self, compile_input, compiler):
        compile_result = compiler.compile_bool_expression(compile_input)
        if not compile_result:
            raise ERPError('Fail to compile filter.')
        compiler.add_to_statements(self.statement_name, compile_result, set_append=True)


@register_function(common_function_registry)
class StartFilter(Filter):
    function_name = 'start_filter'
    statement_name = 'start_filter_expression'

    def compile(self, compile_input, compiler):
        if not compiler.ee_tree.is_start:
            return None
        super(Filter, self).compile(compile_input, compiler)

    def compile_to_dgraph(self, compile_input, compiler):
        if compile_input:
            compile_result = compiler.compile_bool_expression(compile_input)
        elif compiler.ee_tree.statements.start_filter_expression:
            compile_result = compiler.ee_tree.statements.start_filter_expression
        else:
            compile_result = "has({md_type}.{attr_name})".format(
                md_type=compiler.ee_tree.typed.__name__, attr_name=compiler.ee_tree.typed.metadata['identifier'].name
            )

        if not compile_result:
            raise ERPError('Fail to compile start_filter.')
        compiler.add_to_statements(self.statement_name, compile_result)

    def compile_to_mysql(self, compile_input, compiler):
        if compile_input:
            compile_result = compiler.compile_bool_expression(compile_input)
            compiler.add_to_statements(self.statement_name, compile_result)


@attr.s(frozen=True)
class PagingParams(object):
    offset = attr.ib(type=int, default=0)
    limit = attr.ib(type=int, default=100)


@register_function(common_function_registry)
class Paging(CommonFunction):
    function_name = 'paging'
    statement_name = 'paging'

    def inner_parse(self, member, ee_tree):
        value = {item.key: item.value for item in member.value.members}
        return error_adapt_handler(module_logger, Exception, ERPParamsError, 'Erp expression is invalid.')(
            lambda: cattr.structure(value, PagingParams)
        )()

    def compile(self, compile_input, compiler):
        if not compile_input or not isinstance(compile_input, PagingParams):
            compile_input = PagingParams()
        super(Paging, self).compile(compile_input, compiler)

    def compile_to_dgraph(self, compile_input, compiler):
        if compile_input.limit or compile_input.offset:
            compile_result = {'first': compile_input.limit, 'offset': compile_input.offset}
            compiler.add_to_statements(self.statement_name, compile_result)

    def compile_to_mysql(self, compile_input, compiler):
        if compile_input.limit or compile_input.offset:
            compile_result = '{offset}, {limit}'.format(offset=compile_input.offset, limit=compile_input.limit)
            compiler.add_to_statements(self.statement_name, compile_result)


@register_function(common_function_registry)
class WildCard(CommonFunction):
    function_name = '*'
    statement_name = None

    def parse(self, member, ee_tree):
        parsed_info = self.inner_parse(member, ee_tree)
        setattr(getattr(ee_tree, self.tree_function_store_attr), 'wild_card', parsed_info)

    def compile(self, compile_input, compiler):
        return None


@register_function(common_function_registry)
class Count(CommonFunction):
    function_name = 'count'
    statement_name = 'count'

    def compile_to_dgraph(self, compile_input, compiler):
        if compile_input:
            compiler.add_to_statements(self.statement_name, compile_input)

    def compile_to_mysql(self, compile_input, compiler):
        if compile_input:
            compiler.add_to_statements(self.statement_name, compile_input)


@register_function(common_function_registry)
class Cascade(CommonFunction):
    function_name = 'cascade'
    statement_name = 'cascade'

    def compile_to_dgraph(self, compile_input, compiler):
        if compile_input:
            compiler.add_to_statements(self.statement_name, True)

    def compile_to_mysql(self, compile_input, compiler):
        pass


@attr.s(frozen=True)
class OrderByParams(object):
    order = attr.ib(type=str, validator=in_(('asc', 'desc')))
    by = attr.ib(type=str)


@register_function(common_function_registry)
class OrderBy(CommonFunction):
    function_name = 'order_by'
    statement_name = 'order_by'

    def inner_parse(self, member, ee_tree):
        value = {item.key: item.value for item in member.value.members}
        return error_adapt_handler(module_logger, Exception, ERPParamsError, 'Erp expression is invalid.')(
            lambda: cattr.structure(value, OrderByParams)
        )()

    def compile(self, compile_input, compiler):
        if compile_input:
            super(OrderBy, self).compile(compile_input, compiler)

    def compile_to_dgraph(self, compile_input, compiler):
        fields_dict = attr.fields_dict(compiler.ee_tree.typed)
        key = compile_input.by
        # order by的目标属性字段必须存在且不为关联字段
        if key not in fields_dict or issubclass(parse_list_type(fields_dict[key].type)[1], MetaData):
            raise ERPParamsError(_('The attribute {} is not found on type.'.format(key)))
        compile_result = dict(
            order='order{}'.format(compile_input.order),
            predicate=compiler.compile_to_rdf_attr_name(key, compiler.ee_tree.typed),
        )
        compiler.add_to_statements(self.statement_name, compile_result)

    def compile_to_mysql(self, compile_input, compiler):
        fields_dict = attr.fields_dict(compiler.ee_tree.typed)
        key = compile_input.by
        # order by的目标属性字段必须存在且不为关联字段
        is_list, ib_primitive_type = parse_list_type(fields_dict[key].type)
        if key not in fields_dict or issubclass(ib_primitive_type, MetaData):
            raise ERPParamsError(_('The attribute {} is not found on type.'.format(key)))
        compile_result = dict(
            order=compile_input.order.upper(),
            predicate=compiler.compile_to_mysql_column_fqn(key),
        )
        compiler.add_to_statements(self.statement_name, compile_result)


@attr.s(frozen=True)
class AuthCheckParams(object):
    user_id = attr.ib(type=str)
    action_id = attr.ib(type=str)
    metadata_type = attr.ib(type=str)
    variable_name = attr.ib(type=str, factory=lambda: 'auth_scope_' + uuid.uuid4().hex)


@register_function(complex_function_registry)
class AuthCheck(ComplexFunction):
    compile_mode = FunctionMode.PRE_COMPILE
    function_name = 'auth_check'
    statement_name = 'filter_expressions'

    def __init__(self, *args, **kwargs):
        super(AuthCheck, self).__init__(*args, **kwargs)
        self.variables_statements = rt_local.erp_tree_root.variables.statements

    def inner_parse(self, member, ee_tree):
        value = {item.key: item.value for item in member.value.members}
        value['metadata_type'] = camel_to_snake(ee_tree.typed.__name__)
        value = error_adapt_handler(module_logger, Exception, ERPParamsError, 'Erp expression is invalid.')(
            lambda: AuthCheckParams(**value)
        )()
        value_dict = cattr.unstructure(value)
        return self._gen_check_key({k: value_dict[k] for k in sorted(value_dict.keys())})

    def pre_compile(self, compile_input, compiler):
        if not isinstance(compile_input, dict):
            return None
        var_statements_dict = self.check(list(compile_input.keys()))
        compile_input_dict = {
            key: (compile_input[key], val) for key, val in var_statements_dict.items() if key in compile_input
        }
        super(AuthCheck, self).pre_compile(compile_input_dict, compiler)

    def compile(self, compile_input, compiler):
        pass

    def pre_compile_to_dgraph(self, compile_input, compiler):
        if not isinstance(compile_input, dict):
            return None
        for check_params_tuple, var_statement_tuple in list(compile_input.items()):
            check_params_dict = self._parse_check_key(check_params_tuple)
            target_tree, var_statement = var_statement_tuple
            var_content, compile_result = None, None
            if var_statement != '*':
                var_content = (
                    var_statement
                    if var_statement
                    else '{} as var (func: eq(Internal.role, "as_none"))'.format(check_params_dict['variable_name'])
                )
                compile_result = 'uid({})'.format(check_params_dict['variable_name'])
            # 设置前置变量和表达式
            if var_content and compile_result:
                self.variables_statements.append(var_content)
                if target_tree.is_start and target_tree.common_functions.start_filter is None:
                    compiler.add_to_statements('start_filter_expression', compile_result, target_tree=target_tree)
                else:
                    compiler.add_to_statements(
                        self.statement_name, compile_result, set_append=True, target_tree=target_tree
                    )

    def pre_compile_to_mysql(self, compile_input, compiler):
        pass

    @staticmethod
    def _gen_check_key(check_param_dict):
        key_part_list = list()
        for param_name in sorted(check_param_dict.keys()):
            key_part_list.append((param_name, check_param_dict[param_name]))
        return tuple(key_part_list)

    @staticmethod
    def _parse_check_key(check_param_tuple):
        key_part_dict = dict()
        for param_name, param_value in check_param_tuple:
            key_part_dict[param_name] = param_value
        return key_part_dict

    @retry(tries=3, delay=0.1, backoff=2)
    def check(self, auth_check_params_lst):
        """
        权限校验api接口

        :param auth_check_params_lst: list 输入参数列表
        [
            (
                ('action_id', 'raw_data.update'),
                ('user_id', self.USER1),
                ('variable_name', 'AAA'),
                ('metadata_type', 'access_raw_data')
            )
        ]
        :return: dict {输入参数字典: 权限情况字典}
        {
            AuthCheckParams(**{
                'action_id': 'raw_data.update',
                'user_id': self.USER1,
                'variable_name': 'AAA',
                'metadata_type': 'access_raw_data'
            }):
            'AAA as var(func: has(AccessRawData.typed)) @filter(  eq(AccessRawData.bk_biz_id, [591]))',
            ......
        }
        """
        url = self.normal_conf.auth_api_url + 'users/dgraph_scopes/'
        auth_check_kwargs = [self._parse_check_key(auth_check_params) for auth_check_params in auth_check_params_lst]
        r = rt_context.m_resource.http_session.post(url, json={'permissions': auth_check_kwargs})
        r.raise_for_status()
        info = r.json()
        ret = {}
        if info['result'] and info['data']:
            for n, item in enumerate(info['data']):
                if not item['result']['result']:
                    raise ERPError('Fail to targeted access auth info. The return info is {}'.format(item['result']))
                ret[auth_check_params_lst[n]] = item['result']['statement']
            return ret
        else:
            raise ERPError('Fail to access auth info.')
