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
from os import path
from typing import List

import attr
from textx import metamodel_from_file

from metadata.backend.erp.compiler import erp_compiler_registry as erp_compilers
from metadata.backend.erp.parser import (
    ErpExpressionParseTree,
    ErpGlobalVariables,
    md_typed_converter,
)
from metadata.backend.interface import BackendType
from metadata.exc import ERPError
from metadata.runtime import rt_context, rt_local
from metadata.util.i18n import lazy_selfish as _

module_logger = logging.getLogger(__name__)


@attr.s
class ErpParseTree(object):
    """
    erp 解析树(可包含多条查询表达式部分和可选的前置变量部分)
    """

    expressions = attr.ib(type=List[ErpExpressionParseTree], factory=list)  # erp解析树分支
    variables = attr.ib(factory=ErpGlobalVariables)  # 根部全局变量


@attr.s
class ErpExecuteBranch(object):
    """
    erp 执行分支
    """

    expressions = attr.ib(factory=list)  # 执行表达式分支
    variables = attr.ib(default=None)  # 执行表达式变量依赖
    backend_type = attr.ib(default=None)  # 执行后端类型


class ErpQuery(object):
    """
    ERP 查询类
    """

    def __init__(self, service):
        self.service = service
        self.erp_tree = None
        self.target_backends_set = set()
        self.statements = dict()
        self.erp_meta = metamodel_from_file(path.join(path.dirname(__file__), 'textx/erp.tx'))
        self.boolean_exp_meta = metamodel_from_file(path.join(path.dirname(__file__), 'textx/boolean.tx'))
        rt_local.erp_tree_root = None
        rt_local.erp_query_now = self

    @staticmethod
    def process_response(response_mapping):
        """
        :param response_mapping: 返回结果分布
        :return: dict 实际返回结果

        response_mapping: backend_type -> response_item: {statement, ret}
        """
        module_logger.info('[Dgraph erp_v2] response_mapping={}'.format(response_mapping))
        response = dict()
        for backend_type, response_item in response_mapping.items():
            response.update(response_item['ret'])
        return response

    def parse(self, search_params):
        """
        erp查询解析

        :param search_params: erp查询参数
        :return: None
        """
        raw_erp_tree = self.erp_meta.model_from_str(json.dumps(search_params, ensure_ascii=False))
        self.erp_tree = ErpParseTree()
        rt_local.erp_tree_root = self.erp_tree
        for exp in raw_erp_tree.values:
            # 提前typed指令
            for n, member in enumerate(exp.members):
                if getattr(member, '_tx_fqn', None) == 'erp.TypedMember':
                    item = exp.members.pop(n)
                    exp.members.insert(0, item)
                    break
            # 从初始查询开始解析erp表达式
            ee_tree = ErpExpressionParseTree(members=exp.members, is_start=True)
            self.erp_tree.expressions.append(ee_tree)

    def compile(self):
        """
        将erp解析树编译成实际执行的backend查询语句片段

        :return: None
        """
        # 注册后端查询类型, 提前进行BATCH编译
        if self.erp_tree.variables.parsed_info:
            # Todo: 提前批量编译适配mysql
            self.target_backends_set.add(BackendType('dgraph'))
            pre_compiler_cls = erp_compilers[BackendType('dgraph')]
            pre_compiler = pre_compiler_cls()
            pre_compiler.pre_compile()
        for ee_tree in self.erp_tree.expressions:
            target_backend = BackendType(ee_tree.backend_type_name)
            if target_backend not in erp_compilers:
                raise ERPError(_("Can not find valid target_backend."))
            if target_backend not in self.target_backends_set:
                self.target_backends_set.add(target_backend)
            erp_compiler_cls = erp_compilers[target_backend]
            compiler = erp_compiler_cls(ee_tree=ee_tree)
            compiler.compile()

    def execute(self):
        execute_tree = {
            backend_type: ErpExecuteBranch(
                variables=self.erp_tree.variables,
                backend_type=backend_type,
                expressions=[
                    ee_tree
                    for ee_tree in self.erp_tree.expressions
                    if BackendType(ee_tree.backend_type_name) == backend_type
                ],
            )
            for backend_type in self.target_backends_set
            if backend_type in self.service.supported_backends
        }
        response_mapping = {
            backend_type: self.service.supported_backends[backend_type].query_via_erp(execute_tree[backend_type])
            for backend_type in execute_tree
        }

        return self.process_response(response_mapping)


class ErpService(object):
    """
    Erp服务出口
    """

    def __init__(self, backends_group, *args, **kwargs):
        self.supported_backends = backends_group
        self.normal_conf = rt_context.config_collection.normal_config

    def query(self, search_params):
        """
        Dgraph ERP协议查询接口

        :param search_params: 协议参数
        :return:
        """
        search_params = self.compatible_with_version_one(search_params)
        q = ErpQuery(service=self)
        q.parse(search_params)
        q.compile()
        return q.execute()

    @staticmethod
    def compatible_with_version_one(search_params):
        """
        兼容老版本erp语法
        :param search_params: erp表达式
        :return: 可执行erp表达式
        """
        current_version_params_lst = []
        if isinstance(search_params, dict):
            for k, v in list(search_params.items()):
                typed = md_typed_converter(k)
                if not typed:
                    raise ERPError(_("Not existed Metadata type."))
                updated_search_params = {}
                updated_search_params.update(v['expression'])
                updated_search_params['?:start_filter'] = "{} in {}".format(
                    typed.metadata['identifier'].name, json.dumps(v['starts'])
                )
                updated_search_params['?:typed'] = '{}'.format(k)
                current_version_params_lst.append(updated_search_params)
            return current_version_params_lst
        else:
            return search_params
