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

import attr

from metadata.exc import ERPError
from metadata.runtime import rt_context
from metadata.type_system.core import MetaData, parse_list_type
from metadata.util.i18n import lazy_selfish as _

module_logger = logging.getLogger(__name__)


class DgraphErpMixin(object):
    """
    Dgraph关联计算功能
    """

    def __init__(self, *args, **kwargs):
        super(DgraphErpMixin, self).__init__(*args, **kwargs)
        self.erp_template = self.jinja_env.get_template('erp.jinja2')

    def query_via_erp(self, retrieve_args):
        """
        ERP协议查询接口

        :param retrieve_args: 协议参数
        :return:
        """
        parsed_retrieve_args = {}
        for k, per_retrieve_info in retrieve_args.items():
            per_retrieve_expression = per_retrieve_info['expression']
            per_retrieve_starts = per_retrieve_info['starts']
            per_parsed_retrieve_expression = {}
            md_type = rt_context.md_types_registry.get(k, None)
            if not md_type:
                raise ERPError(_('Fail to get md type in ERP expression.'))
            self._parse_per_retrieve_expression(per_retrieve_expression, per_parsed_retrieve_expression, md_type)
            parsed_retrieve_args[
                (
                    k,
                    md_type.metadata['identifier'].name,
                    '[{}]'.format(','.join('"{}"'.format(i) for i in per_retrieve_starts)),
                )
            ] = per_parsed_retrieve_expression
        statement = self.erp_template.render(
            is_dict=lambda x: isinstance(x, dict), retrieve_args_lst=parsed_retrieve_args
        )
        module_logger.info('[Dgraph erp] statement={}'.format(statement))
        response = self.query(statement)
        return response

    def _parse_per_retrieve_expression(self, retrieve_expression, parsed_retrieve_expression, md_type):
        """
        解析ERP协议表达式

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
