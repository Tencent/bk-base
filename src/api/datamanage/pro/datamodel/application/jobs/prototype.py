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


import re
import sqlparse

from abc import ABCMeta, abstractmethod


class Orchestrator(object, metaclass=ABCMeta):
    """
    编排原型基类, 对任务构建的指令进行流程编排
    """

    operator_buffer = dict()

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def gen_plan(self, command):
        """
        创建执行计划
        :return: Operator 配置好任务的执行者实例
        """
        raise NotImplementedError


class DataModelNode(object, metaclass=ABCMeta):
    """
    数据模型原型基类
    """

    node_type = None
    """
    节点类型
    """

    inner_app_table = 'app_tab'
    """
    应用阶段子表别名
    """

    outer_built_table = 'built_tab'
    """
    最外层复合模型构建表别名
    """

    sql_template = """
SELECT
    {fields_expr}
FROM
    {main_table}
    {join_expr}
{conditions_expr}
{group_by_expr}
    """

    filed_separator = ',\n'
    """
    字段间间隔符
    """

    join_separator = '\n'
    """
    join语句之间间隔符
    """

    outer_field_expr_template = 'cast({inner_expr} AS {type}) AS `{alias}`'
    """
    节点最外层字段输出模板
    """

    cast_field_expr_template = 'cast({field_expr} AS {type}) AS `{alias}`'
    """
    节点强制类型转换模板
    """

    join_expr_template = '{related_method} {related_table} AS `{related_table}` ON {main_expr} = {related_expr}'
    """
    join语句模板
    """

    def __init__(self):
        self._init_workspace()

    def _init_workspace(self):
        self.type_conf = dict()
        """
        字段类型配置
        """

        self.schema_conf = dict()
        """
        模型输出配置
        """

        self.built_conf = dict()
        """
        构建加工配置
        """

        self.app_conf = dict()
        """
        应用加工配置
        """

        self.related_conf = dict()
        """
        关联配置
        """

        self.indicator_conf = dict()
        """
        指标配置
        """

    @staticmethod
    def _clean_field_expr(field_name, expr):
        """
        过滤出字段表达式并返回(去掉as等信息)

        :param field_name:
        :param expr:
        :return:
        """
        expr = sqlparse.format(expr, strip_comments=True, strip_whitespace=True).strip()
        return re.sub(r' as {}$'.format(field_name), '', expr, flags=re.I).strip()


class Operator(object, metaclass=ABCMeta):
    """
    执行者原型基类
    """

    def __init__(self):
        pass

    @staticmethod
    def _default(self):
        """
        默认操作
        """
        return None
